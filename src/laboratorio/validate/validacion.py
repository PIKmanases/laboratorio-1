"""Validación idempotente de bronze a silver con esquemas de pandera.

Para cada revisión de bronze se arma un esquema (`esquema()`) a partir de
`config/pipeline.yaml` y de la fecha de la partición, y se valida:

- **esquema:** columnas exactas (sin extra ni faltantes) y tipos (se convierten si es posible);
- **rangos:** mínimo y máximo por columna, y nulos solo donde se permiten;
- **duplicados:** claves únicas;
- **fechas esperadas:** `date` igual a la fecha de la partición y nunca posterior a
  `available_at`;
- **referencias:** tiendas conocidas (`stores.csv`);
- **control:** `revision` (>= 1) y `available_at` en cada fila.

Los motivos de rechazo quedan en la columna `_motivos` y en `rev=N.motivos.json`.

Si pasa, la revisión (con los tipos ya convertidos) se escribe en **silver**; si no,
va a **cuarentena**. Cada revisión se procesa una sola vez,
cada escritura es atómica y solo se procesa lo conocido hasta `fecha_proceso` (as-of).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import pandas as pd
import pandera.pandas as pa
from pandera.errors import SchemaError, SchemaErrors

from laboratorio.capas import (
    escribir_json,
    escribir_parquet,
    listar_particiones,
    listar_revisiones,
    revisiones,
    ruta_particion,
    ruta_revision,
)
from laboratorio.ingest.ingesta import DIR_REFERENCIA


@dataclass(frozen=True)
class Columna:
    tipo: str
    nulos: bool = False
    min: float | None = None
    max: float | None = None


@dataclass(frozen=True)
class Reglas:
    columnas: dict[str, Columna]
    claves: list[str]
    validar_tiendas: bool

    @classmethod
    def desde_dict(cls, d: dict) -> Reglas:
        return cls(
            columnas={n: Columna(**c) for n, c in d["columnas"].items()},
            claves=list(d["claves"]),
            validar_tiendas=bool(d["validar_tiendas"]),
        )


def ruta_motivos(ruta_cuarentena: Path) -> Path:
    """`rev=N.parquet` -> `rev=N.motivos.json`."""
    return Path(ruta_cuarentena).with_suffix(".motivos.json")


@dataclass
class Resumen:
    validas: dict[str, int] = field(default_factory=dict)
    rechazadas: dict[str, int] = field(default_factory=dict)


def esquema(reglas: Reglas, fecha: date, tiendas: set[int]) -> pa.DataFrameSchema:
    """Esquema pandera de una revisión de la partición `fecha`."""
    columnas = {}
    for nombre, c in reglas.columnas.items():
        checks = []
        if c.min is not None:
            checks.append(pa.Check.ge(c.min, error=f"menor que {c.min}"))
        if c.max is not None:
            checks.append(pa.Check.le(c.max, error=f"mayor que {c.max}"))
        if nombre == "date":
            checks.append(pa.Check.eq(fecha.isoformat(), error=f"fecha distinta a la esperada {fecha}"))
        if nombre == "store_nbr" and reglas.validar_tiendas:
            checks.append(pa.Check.isin(sorted(tiendas), error="tienda desconocida"))
        columnas[nombre] = pa.Column(c.tipo, checks=checks, nullable=c.nulos, coerce=True)

    columnas["revision"] = pa.Column("int64", pa.Check.ge(1, error="revisión inválida"))
    columnas["available_at"] = pa.Column("datetime64[ns]", coerce=True)
    columnas["origen"] = pa.Column("str")
    return pa.DataFrameSchema(
        columnas,
        strict=True,
        unique=reglas.claves,
        report_duplicates="all",
        checks=[
            pa.Check(lambda df: len(df) > 0, error="sin filas"),
            pa.Check(
                lambda df: pd.to_datetime(df["date"], errors="coerce") <= df["available_at"],
                error="date posterior a available_at",
            ),
            pa.Check(lambda df: df["revision"].nunique() <= 1, error="más de una revisión en el archivo"),
        ],
    )


_NOMBRES = {
    "multiple_fields_uniqueness": "claves duplicadas",
    "column_in_dataframe": "columna faltante",
    "column_in_schema": "columna no esperada",
    "not_nullable": "nulos no permitidos",
}


def _nombre_check(check: str) -> str:
    if check.startswith(("coerce_dtype", "dtype")):
        return f"tipo inválido, se esperaba {check.split('(', 1)[1].rstrip(')')}"
    return _NOMBRES.get(check, check)


def _motivos(err: SchemaErrors | SchemaError) -> list[str]:
    """Resume los casos fallidos de pandera: un motivo por regla, en español."""
    casos = err.failure_cases.assign(regla=lambda d: d["check"].astype(str).map(_nombre_check))
    motivos = []
    for regla, grupo in casos.groupby("regla", sort=False):
        columnas = sorted(set(grupo["column"].dropna().astype(str)))
        ejemplos = [str(x) for x in grupo["failure_case"].dropna().unique()[:3] if str(x)]
        detalle = f" ({grupo['index'].nunique()} filas)" if grupo["index"].notna().any() else ""
        if regla in ("columna faltante", "columna no esperada"):
            motivos.append(f"{regla}: {', '.join(ejemplos)}")
        elif len(columnas) == 1:
            ej = f"; p. ej. {', '.join(ejemplos)}" if ejemplos else ""
            motivos.append(f"{columnas[0]}: {regla}{detalle[:-1]}{ej})" if detalle else f"{columnas[0]}: {regla}")
        elif regla == "claves duplicadas":
            motivos.append(f"{regla} {columnas}{detalle}")
        else:
            motivos.append(f"{regla}{detalle}")
    return motivos


def validar_revision(
    df: pd.DataFrame, fecha: date, reglas: Reglas, tiendas: set[int]
) -> tuple[pd.DataFrame | None, list[str]]:
    """Valida una revisión. Devuelve (datos con tipos convertidos, []) o (None, motivos)."""
    try:
        return esquema(reglas, fecha, tiendas).validate(df, lazy=True), []
    except (SchemaErrors, SchemaError) as err:
        return None, _motivos(err)


def validar(
    dir_bronze: Path,
    dir_silver: Path,
    dir_cuarentena: Path,
    reglas: dict[str, Reglas],
    fecha_proceso: date,
) -> Resumen:
    """Corrida diaria de validación para el día `fecha_proceso`."""
    tiendas = set(pd.read_parquet(Path(dir_bronze) / DIR_REFERENCIA / "stores.parquet")["store_nbr"])
    limite = pd.Timestamp(fecha_proceso)
    resumen = Resumen()
    for fuente, r in reglas.items():
        for fecha, particion in listar_particiones(dir_bronze, fuente).items():
            procesadas = revisiones(ruta_particion(dir_silver, fuente, fecha)) | revisiones(
                ruta_particion(dir_cuarentena, fuente, fecha)
            )
            for rev, ruta in listar_revisiones(particion).items():
                if rev in procesadas:
                    continue
                df = pd.read_parquet(ruta)
                if not df.empty and (df["available_at"] > limite).any():
                    continue  # todavía no se conocía el día fecha_proceso
                validado, motivos = validar_revision(df, fecha, r, tiendas)
                if motivos:
                    destino = ruta_revision(dir_cuarentena, fuente, fecha, rev)
                    # Motivos también en JSON: una revisión vacía no tiene filas donde guardarlos.
                    escribir_json({"fecha_proceso": fecha_proceso.isoformat(), "motivos": motivos}, ruta_motivos(destino))
                    escribir_parquet(df.assign(_motivos="; ".join(motivos)), destino)
                    resumen.rechazadas[fuente] = resumen.rechazadas.get(fuente, 0) + 1
                else:
                    escribir_parquet(validado, ruta_revision(dir_silver, fuente, fecha, rev))
                    resumen.validas[fuente] = resumen.validas.get(fuente, 0) + 1
    return resumen


def desde_config(cfg: dict, raiz: Path) -> dict:
    """Argumentos de `validar` (sin `fecha_proceso`) a partir de config/pipeline.yaml."""
    c = cfg["capas"]
    return {
        "dir_bronze": raiz / c["bronze"],
        "dir_silver": raiz / c["silver"],
        "dir_cuarentena": raiz / c["cuarentena"],
        "reglas": {n: Reglas.desde_dict(r) for n, r in cfg["validacion"].items()},
    }
