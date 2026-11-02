"""Modelos de pronóstico. Línea base hasta la fase de modelado.

Cada modelo recibe los predictores de un corte y devuelve la venta esperada por
fila (tienda, familia, fecha_objetivo). Se elige en `config/pipeline.yaml`
(`pronostico.modelo`).
"""

from __future__ import annotations

import pandas as pd

from laboratorio.features.predictores import CLAVES


def media_dia_semana(p: pd.DataFrame) -> pd.Series:
    """Media del mismo día de la semana en la ventana; si no hay, media de 28 días."""
    return p["media_dia_semana"].fillna(p["media_28"]).fillna(0.0)


def media_28(p: pd.DataFrame) -> pd.Series:
    return p["media_28"].fillna(0.0)


MODELOS = {"media_dia_semana": media_dia_semana, "media_28": media_28}


def pronosticar(predictores: pd.DataFrame, modelo: str) -> pd.DataFrame:
    if modelo not in MODELOS:
        raise ValueError(f"modelo desconocido {modelo!r}; opciones: {sorted(MODELOS)}")
    if predictores.empty:
        return pd.DataFrame()
    df = predictores[[*CLAVES, "fecha_objetivo", "h"]].copy()
    df["pronostico"] = MODELOS[modelo](predictores).clip(lower=0).round(3).to_numpy()
    df["modelo"] = modelo
    return df
