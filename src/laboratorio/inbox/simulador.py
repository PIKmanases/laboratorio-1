"""Simulador de la bandeja de entrada.

Toma los datos de Kaggle posteriores a la fecha de corte y los entrega en la
bandeja como un archivo por fecha y fuente (`<fuente>/<fuente>_<fecha>.csv`).
Cada fuente llega con su propio retraso: el archivo del día D aparece el día
D + retraso. En algunas fechas el archivo original trae valores erróneos y
más tarde llega `<fuente>_<fecha>_rev2.csv` con los valores corregidos (los
reales de Kaggle).

`generar_bandeja(..., hasta=X)` deja en la bandeja solo lo que ya llegó el día
simulado X. Es determinista e idempotente: misma entrada, mismos archivos.
"""

from __future__ import annotations

import os
import zipfile
import zlib
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class Fuente:
    nombre: str
    archivo: str
    columna_valor: str
    retraso_dias: int


@dataclass(frozen=True)
class Revisiones:
    fraccion_fechas: float
    fraccion_filas: float
    retraso_extra_dias: int


@dataclass(frozen=True)
class Entrega:
    fuente: str
    fecha_dato: date
    revision: int
    fecha_llegada: date

    @property
    def ruta_relativa(self) -> Path:
        sufijo = "" if self.revision == 1 else f"_rev{self.revision}"
        return Path(self.fuente) / f"{self.fuente}_{self.fecha_dato.isoformat()}{sufijo}.csv"


def _rng(semilla: int, *claves: str) -> np.random.Generator:
    """Generador determinista e independiente por fuente/fecha."""
    return np.random.default_rng([semilla, *(zlib.crc32(c.encode()) for c in claves)])


def leer_fuente_zip(ruta_zip: Path, fuente: Fuente) -> pd.DataFrame:
    with zipfile.ZipFile(ruta_zip) as z, z.open(fuente.archivo) as f:
        return pd.read_csv(f)


def fechas_revisadas(fechas: list[date], fuente: Fuente, rev: Revisiones, semilla: int) -> set[date]:
    k = round(len(fechas) * rev.fraccion_fechas)
    if k == 0:
        return set()
    elegidas = _rng(semilla, fuente.nombre).choice(len(fechas), size=k, replace=False)
    return {sorted(fechas)[i] for i in elegidas}


def planificar_entregas(
    fechas_por_fuente: dict[str, list[date]],
    fuentes: dict[str, Fuente],
    rev: Revisiones,
    semilla: int,
) -> list[Entrega]:
    entregas = []
    for nombre, fechas in fechas_por_fuente.items():
        fuente = fuentes[nombre]
        revisadas = fechas_revisadas(fechas, fuente, rev, semilla)
        for d in sorted(fechas):
            llegada = d + timedelta(days=fuente.retraso_dias)
            entregas.append(Entrega(nombre, d, 1, llegada))
            if d in revisadas:
                entregas.append(Entrega(nombre, d, 2, llegada + timedelta(days=rev.retraso_extra_dias)))
    return sorted(entregas, key=lambda e: (e.fecha_llegada, e.fuente, e.fecha_dato, e.revision))


def alterar_valores(df: pd.DataFrame, columna: str, fraccion: float, rng: np.random.Generator) -> pd.DataFrame:
    """Versión 'errónea' del archivo: altera algunas filas con valor no nulo y distinto de cero."""
    df = df.copy()
    candidatas = df.index[df[columna].notna() & (df[columna] != 0)]
    if len(candidatas) == 0:
        return df
    k = max(1, round(len(candidatas) * fraccion))
    filas = rng.choice(candidatas, size=k, replace=False)
    # Factor lejos de 1 para que el error sea visible: [0.5, 0.8] o [1.2, 1.5].
    factor = rng.uniform(0.5, 0.8, size=k)
    factor = np.where(rng.random(k) < 0.5, factor, factor + 0.7)
    nuevos = df.loc[filas, columna].to_numpy() * factor
    if pd.api.types.is_integer_dtype(df[columna]):
        df.loc[filas, columna] = np.rint(nuevos).astype(df[columna].dtype)
    else:
        df.loc[filas, columna] = np.round(nuevos, 3)
    return df


def _escribir_atomico(df: pd.DataFrame, destino: Path) -> None:
    destino.parent.mkdir(parents=True, exist_ok=True)
    tmp = destino.with_name(f".{destino.name}.tmp")
    df.to_csv(tmp, index=False)
    os.replace(tmp, destino)


def generar_bandeja(
    ruta_zip: Path,
    dir_bandeja: Path,
    fuentes: dict[str, Fuente],
    revisiones: Revisiones,
    fecha_corte: date,
    fecha_fin: date,
    semilla: int,
    hasta: date | None = None,
) -> list[Entrega]:
    """Escribe en `dir_bandeja` las entregas que llegaron hasta el día `hasta`.

    Sin `hasta`, entrega todo. Devuelve las entregas escritas.
    """
    datos, fechas = {}, {}
    for nombre, fuente in fuentes.items():
        df = leer_fuente_zip(ruta_zip, fuente)
        df = df[(df["date"] > fecha_corte.isoformat()) & (df["date"] <= fecha_fin.isoformat())]
        datos[nombre] = df
        fechas[nombre] = [date.fromisoformat(d) for d in df["date"].unique()]

    entregas = planificar_entregas(fechas, fuentes, revisiones, semilla)
    if hasta is not None:
        entregas = [e for e in entregas if e.fecha_llegada <= hasta]

    revisadas = {n: fechas_revisadas(fechas[n], f, revisiones, semilla) for n, f in fuentes.items()}
    for e in entregas:
        df = datos[e.fuente]
        dia = df[df["date"] == e.fecha_dato.isoformat()]
        if e.revision == 1 and e.fecha_dato in revisadas[e.fuente]:
            # El original de una fecha revisada llega con errores; el _rev2 trae el valor real.
            rng = _rng(semilla, e.fuente, e.fecha_dato.isoformat())
            dia = alterar_valores(dia, fuentes[e.fuente].columna_valor, revisiones.fraccion_filas, rng)
        _escribir_atomico(dia, Path(dir_bandeja) / e.ruta_relativa)
    return entregas


def desde_config(cfg: dict) -> dict:
    """Argumentos de `generar_bandeja` a partir de config/pipeline.yaml."""
    b = cfg["bandeja"]
    return {
        "ruta_zip": Path(cfg["datos"]["zip_kaggle"]),
        "dir_bandeja": Path(b["dir"]),
        "fuentes": {n: Fuente(nombre=n, **f) for n, f in b["fuentes"].items()},
        "revisiones": Revisiones(**b["revisiones"]),
        "fecha_corte": date.fromisoformat(b["fecha_corte"]),
        "fecha_fin": date.fromisoformat(b["fecha_fin"]),
        "semilla": b["semilla"],
    }
