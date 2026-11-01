"""Validación idempotente de bronze a silver.

Cada revisión de bronze se valida una sola vez:
- si pasa, se copia a **silver** (`silver/<fuente>/fecha=<D>/rev=<N>.parquet`);
- si no, va a **cuarentena** con la columna `_motivos`.

Cada escritura es atómica. Solo se procesan revisiones con
`_fecha_ingesta <= fecha_proceso` (as-of), y las columnas de control de bronze
se conservan, así que `leer_asof` funciona igual sobre silver.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import pandas as pd

from laboratorio.capas import (
    COLUMNAS_CONTROL,
    escribir_parquet,
    listar_particiones,
    listar_revisiones,
    revisiones,
    ruta_particion,
    ruta_revision,
)
from laboratorio.ingest.ingesta import DIR_REFERENCIA


@dataclass(frozen=True)
class Reglas:
    columnas: list[str]
    claves: list[str]
    no_nulos: list[str]
    no_negativos: list[str]
    validar_tiendas: bool


@dataclass
class Resumen:
    validas: dict[str, int] = field(default_factory=dict)
    rechazadas: dict[str, int] = field(default_factory=dict)


def validar_revision(df: pd.DataFrame, fecha: date, reglas: Reglas, tiendas: set[int]) -> list[str]:
    """Motivos de rechazo de una revisión (lista vacía = válida)."""
    if df.empty:
        return ["sin filas"]
    faltan = [c for c in reglas.columnas if c not in df.columns]
    sobran = [c for c in df.columns if c not in reglas.columnas]
    if faltan or sobran:
        return [m for m in (faltan and f"columnas faltantes {faltan}", sobran and f"columnas sobrantes {sobran}") if m]

    motivos = []
    otras_fechas = sorted(set(df["date"].astype(str)) - {fecha.isoformat()})
    if otras_fechas:
        motivos.append(f"fechas distintas a la partición: {otras_fechas[:3]}")
    for c in reglas.no_nulos:
        if (n := int(df[c].isna().sum())) > 0:
            motivos.append(f"{n} nulos en {c}")
    for c in reglas.no_negativos:
        if not pd.api.types.is_numeric_dtype(df[c]):
            motivos.append(f"{c} no es numérica")
        elif (n := int((df[c] < 0).sum())) > 0:
            motivos.append(f"{n} negativos en {c}")
    if (n := int(df.duplicated(reglas.claves).sum())) > 0:
        motivos.append(f"{n} claves duplicadas {reglas.claves}")
    if reglas.validar_tiendas:
        desconocidas = sorted(set(df["store_nbr"].dropna()) - tiendas)
        if desconocidas:
            motivos.append(f"tiendas desconocidas: {desconocidas[:5]}")
    return motivos


def validar(
    dir_bronze: Path,
    dir_silver: Path,
    dir_cuarentena: Path,
    reglas: dict[str, Reglas],
    fecha_proceso: date,
) -> Resumen:
    """Corrida diaria de validación para el día `fecha_proceso`."""
    tiendas = set(pd.read_parquet(Path(dir_bronze) / DIR_REFERENCIA / "stores.parquet")["store_nbr"])
    limite = fecha_proceso.isoformat()
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
                if not df.empty and (df["_fecha_ingesta"] > limite).any():
                    continue  # todavía no se conocía el día fecha_proceso
                datos = df.drop(columns=[c for c in COLUMNAS_CONTROL if c in df.columns])
                motivos = validar_revision(datos, fecha, r, tiendas)
                if motivos:
                    escribir_parquet(df.assign(_motivos="; ".join(motivos)), ruta_revision(dir_cuarentena, fuente, fecha, rev))
                    resumen.rechazadas[fuente] = resumen.rechazadas.get(fuente, 0) + 1
                else:
                    escribir_parquet(df, ruta_revision(dir_silver, fuente, fecha, rev))
                    resumen.validas[fuente] = resumen.validas.get(fuente, 0) + 1
    return resumen


def desde_config(cfg: dict, raiz: Path) -> dict:
    """Argumentos de `validar` (sin `fecha_proceso`) a partir de config/pipeline.yaml."""
    c = cfg["capas"]
    return {
        "dir_bronze": raiz / c["bronze"],
        "dir_silver": raiz / c["silver"],
        "dir_cuarentena": raiz / c["cuarentena"],
        "reglas": {n: Reglas(**r) for n, r in cfg["validacion"].items()},
    }
