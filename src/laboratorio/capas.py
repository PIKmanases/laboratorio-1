"""Capas de datos particionadas por fuente, fecha y revisión: escritura atómica y lectura as-of.

Estructura de una capa:

    <capa>/<fuente>/fecha=<AAAA-MM-DD>/rev=<N>.parquet

Cada archivo es una revisión completa de esa fecha, tal cual llegó (mismo
esquema), más las columnas de control:

- `_revision`: 1 = original, 2 = `_rev2`, ...
- `_fecha_ingesta`: día de proceso en que el sistema conoció esa revisión.
- `_origen`: archivo de procedencia.

Cada escritura reemplaza el archivo de forma atómica (temporal en la misma
carpeta + `os.replace`): un lector ve la versión anterior completa o la nueva
completa, nunca una a medias.

`leer_asof(..., asof=X)` devuelve, por fecha, la última revisión conocida el
día X: lo que el sistema sabía ese día, sin información futura.
"""

from __future__ import annotations

import json
import os
import re
from datetime import date
from pathlib import Path

import pandas as pd

COLUMNAS_CONTROL = ["_revision", "_fecha_ingesta", "_origen"]
_PATRON_FECHA = re.compile(r"^fecha=(\d{4}-\d{2}-\d{2})$")
_PATRON_REV = re.compile(r"^rev=(\d+)\.parquet$")


def ruta_particion(dir_capa: Path, fuente: str, fecha: date) -> Path:
    return Path(dir_capa) / fuente / f"fecha={fecha.isoformat()}"


def ruta_revision(dir_capa: Path, fuente: str, fecha: date, revision: int) -> Path:
    return ruta_particion(dir_capa, fuente, fecha) / f"rev={revision}.parquet"


def listar_particiones(dir_capa: Path, fuente: str) -> dict[date, Path]:
    """Particiones (carpetas por fecha) de una fuente, ordenadas por fecha."""
    dir_fuente = Path(dir_capa) / fuente
    if not dir_fuente.is_dir():
        return {}
    particiones = {}
    for ruta in dir_fuente.iterdir():
        m = _PATRON_FECHA.match(ruta.name)
        if m and ruta.is_dir():
            particiones[date.fromisoformat(m[1])] = ruta
    return dict(sorted(particiones.items()))


def listar_revisiones(particion: Path) -> dict[int, Path]:
    """Revisiones de una partición (ignora temporales), ordenadas."""
    if not Path(particion).is_dir():
        return {}
    revs = {}
    for ruta in Path(particion).iterdir():
        m = _PATRON_REV.match(ruta.name)
        if m:
            revs[int(m[1])] = ruta
    return dict(sorted(revs.items()))


def revisiones(particion: Path) -> set[int]:
    return set(listar_revisiones(particion))


def _reemplazo_atomico(ruta: Path, escribir) -> None:
    ruta = Path(ruta)
    ruta.parent.mkdir(parents=True, exist_ok=True)
    tmp = ruta.with_name(f".{ruta.name}.{os.getpid()}.tmp")
    try:
        escribir(tmp)
        os.replace(tmp, ruta)
    finally:
        tmp.unlink(missing_ok=True)


def escribir_parquet(df: pd.DataFrame, ruta: Path) -> None:
    """Escribe (o reemplaza) un archivo de la capa de forma atómica."""
    _reemplazo_atomico(ruta, lambda tmp: df.to_parquet(tmp, index=False))


def escribir_json(datos: dict, ruta: Path) -> None:
    _reemplazo_atomico(ruta, lambda tmp: Path(tmp).write_text(json.dumps(datos, indent=2), encoding="utf-8"))


def leer_asof(
    dir_capa: Path, fuente: str, asof: date, desde: date | None = None, hasta: date | None = None
) -> pd.DataFrame:
    """Por fecha, la última revisión conocida el día `asof` (principio as-of)."""
    limite = asof.isoformat()
    partes = []
    for fecha, particion in listar_particiones(dir_capa, fuente).items():
        if fecha > asof or (desde and fecha < desde) or (hasta and fecha > hasta):
            continue
        for ruta in reversed(listar_revisiones(particion).values()):
            df = pd.read_parquet(ruta)
            if not df.empty and (df["_fecha_ingesta"] <= limite).all():
                partes.append(df)
                break
    if not partes:
        return pd.DataFrame()
    return pd.concat(partes, ignore_index=True)
