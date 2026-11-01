"""Lectura de la bandeja de entrada (solo lectura: nunca escribe en ella)."""

from __future__ import annotations

import re
from datetime import date
from pathlib import Path

import pandas as pd

PATRON = re.compile(r"^(?P<fuente>[a-z_]+)_(?P<fecha>\d{4}-\d{2}-\d{2})(?:_rev(?P<rev>\d+))?\.csv$")


def listar_archivos(dir_bandeja: Path, fuente: str) -> pd.DataFrame:
    """Archivos de una fuente: columnas fecha, revision, ruta (ordenado)."""
    filas = []
    for ruta in sorted((Path(dir_bandeja) / fuente).glob("*.csv")):
        m = PATRON.match(ruta.name)
        if m and m["fuente"] == fuente:
            filas.append({"fecha": date.fromisoformat(m["fecha"]), "revision": int(m["rev"] or 1), "ruta": ruta})
    return pd.DataFrame(filas, columns=["fecha", "revision", "ruta"]).sort_values(["fecha", "revision"], ignore_index=True)


def leer_fuente(dir_bandeja: Path, fuente: str) -> pd.DataFrame:
    """Datos de una fuente usando, por fecha, solo la revisión más reciente."""
    archivos = listar_archivos(dir_bandeja, fuente)
    if archivos.empty:
        return pd.DataFrame()
    ultimas = archivos.groupby("fecha").tail(1)
    return pd.concat((pd.read_csv(r) for r in ultimas["ruta"]), ignore_index=True)
