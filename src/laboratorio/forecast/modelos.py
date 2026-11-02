"""Modelos de pronóstico.

Cada modelo recibe los predictores de un corte y devuelve la venta esperada por
fila (tienda, familia, fecha_objetivo). Se elige en `config/pipeline.yaml`
(`pronostico.modelo`):

- `media_dia_semana`, `media_28`: líneas base, sin entrenamiento.
- `lightgbm_cuantiles`: el modelo guardado en `models/campeon` (ver `forecast.entrenamiento`);
  además del pronóstico trae cuantiles y `sigma_diario`.
- `campeon`: lo que diga `models/campeon/meta.json` (LightGBM, o la línea base si la regla de
  reemplazo no aprobó al candidato).
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from laboratorio.config import RAIZ, cargar_config
from laboratorio.features.predictores import CLAVES

LIGHTGBM = "lightgbm_cuantiles"
CAMPEON = "campeon"


def media_dia_semana(p: pd.DataFrame) -> pd.Series:
    """Media del mismo día de la semana en la ventana; si no hay, media de 28 días."""
    return p["media_dia_semana"].fillna(p["media_28"]).fillna(0.0)


def media_28(p: pd.DataFrame) -> pd.Series:
    return p["media_28"].fillna(0.0)


MODELOS = {"media_dia_semana": media_dia_semana, "media_28": media_28}


def dir_campeon(cfg: dict | None = None) -> Path:
    return RAIZ / (cfg or cargar_config())["capas"]["modelos"] / "campeon"


def hay_campeon(cfg: dict | None = None) -> bool:
    return (dir_campeon(cfg) / "meta.json").exists()


def modelo_vigente(cfg: dict) -> str:
    """Nombre concreto del modelo a usar: `pronostico.modelo`, o el del campeón si dice `campeon`."""
    nombre = cfg["pronostico"]["modelo"]
    if nombre != CAMPEON:
        return nombre
    if not hay_campeon(cfg):
        raise FileNotFoundError("no hay modelo campeón en models/campeon: corre dvc repro o la tarea pronosticar")
    receta = json.loads((dir_campeon(cfg) / "meta.json").read_text(encoding="utf-8"))["receta"]
    return receta["modelo"] if receta["tipo"] == "baseline" else LIGHTGBM


def pronosticar(predictores: pd.DataFrame, modelo: str, dir_modelo: Path | None = None) -> pd.DataFrame:
    if predictores.empty:
        return pd.DataFrame()
    if modelo == LIGHTGBM:
        from laboratorio.forecast.lgbm import ModeloCuantiles
        from laboratorio.forecast.lgbm import pronosticar as pronosticar_lgbm

        df = pronosticar_lgbm(ModeloCuantiles.cargar(dir_modelo or dir_campeon()), predictores)
        df["modelo"] = modelo
        return df
    if modelo not in MODELOS:
        raise ValueError(f"modelo desconocido {modelo!r}; opciones: {sorted([*MODELOS, LIGHTGBM])}")
    df = predictores[[*CLAVES, "fecha_objetivo", "h"]].copy()
    df["pronostico"] = MODELOS[modelo](predictores).clip(lower=0).round(3).to_numpy()
    df["modelo"] = modelo
    return df
