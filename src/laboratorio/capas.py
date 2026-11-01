"""Capas de datos particionadas por fuente, fecha y revisión: escritura atómica y lectura as-of.

Estructura de una capa:

    <capa>/<fuente>/fecha=<AAAA-MM-DD>/rev=<N>.parquet

Cada archivo es una revisión completa de esa fecha, tal cual llegó (mismo
esquema), y **cada fila** lleva las columnas de control:

- `revision`: 1 = original, 2 = `_rev2`, ...
- `available_at`: momento (día de proceso) en que el sistema conoció esa revisión.
- `origen`: archivo de procedencia.

Cada escritura reemplaza el archivo de forma atómica (temporal en la misma
carpeta + `os.replace`): un lector ve la versión anterior completa o la nueva
completa, nunca una a medias.

`load_as_of(source, cutoff)` devuelve, por fecha, la última revisión con
`available_at <= cutoff`: lo que el sistema sabía ese día, sin información futura.
"""

from __future__ import annotations

import json
import os
import re
from datetime import date
from pathlib import Path

import pandas as pd

COLUMNAS_CONTROL = ["revision", "available_at", "origen"]
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


class FugaDeInformacion(RuntimeError):
    """Una lectura as-of devolvió datos posteriores al corte."""


def _como_timestamp(cutoff: date | str) -> pd.Timestamp:
    return pd.Timestamp(cutoff).normalize()


def leer_asof(
    dir_capa: Path, fuente: str, asof: date | str, desde: date | None = None, hasta: date | None = None
) -> pd.DataFrame:
    """Por fecha, la última revisión con `available_at <= asof` (principio as-of)."""
    corte = _como_timestamp(asof)
    partes = []
    for fecha, particion in listar_particiones(dir_capa, fuente).items():
        if fecha > corte.date() or (desde and fecha < desde) or (hasta and fecha > hasta):
            continue
        for ruta in reversed(listar_revisiones(particion).values()):
            df = pd.read_parquet(ruta)
            if not df.empty and (df["available_at"] <= corte).all():
                partes.append(df)
                break
    if not partes:
        return pd.DataFrame()
    df = pd.concat(partes, ignore_index=True)
    verificar_sin_futuro(df, corte)
    return df


def verificar_sin_futuro(df: pd.DataFrame, cutoff: date | str) -> None:
    """Falla si alguna fila tiene fecha o `available_at` posterior al corte."""
    if df.empty:
        return
    corte = _como_timestamp(cutoff)
    futuras = (pd.to_datetime(df["date"]) > corte) | (df["available_at"] > corte)
    if futuras.any():
        raise FugaDeInformacion(f"{int(futuras.sum())} filas posteriores al corte {corte.date()}")


def load_as_of(
    source: str,
    cutoff: date | str,
    capa: str = "silver",
    desde: date | None = None,
    hasta: date | None = None,
    dir_capa: Path | None = None,
) -> pd.DataFrame:
    """Datos de `source` tal como se conocían al cierre del día `cutoff`.

    Por fecha devuelve la última revisión con `available_at <= cutoff`, y nunca
    filas con `date` o `available_at` posteriores al corte. La capa se toma de
    `config/pipeline.yaml` (`capas.<capa>`) salvo que se pase `dir_capa`.
    """
    if dir_capa is None:
        from laboratorio.config import RAIZ, cargar_config

        dir_capa = RAIZ / cargar_config()["capas"][capa]
    return leer_asof(dir_capa, source, cutoff, desde=desde, hasta=hasta)
