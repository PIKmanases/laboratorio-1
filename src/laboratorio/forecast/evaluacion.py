"""Backtest temporal (rolling origin) de una receta de modelo.

Para cada corte de prueba F: se entrena (si la receta lo requiere) solo con cortes
cuyas etiquetas se conocían el día F, se pronostica de F+1 a F+7, se calcula el
pedido con la misma regla del vendedor de periódicos y se compara con lo vendido.

Métricas técnicas: WAPE, sesgo, RMSLE y, si hay cuantiles, pinball y cobertura de
[p10, p90]. Métricas de negocio: costo (faltante × costo_faltante + sobrante ×
costo_sobrante), faltante, sobrante y fill rate. Se reutiliza `monitoring.desempeno.evaluar`,
la misma función del seguimiento en producción.

Recetas:
  {"tipo": "baseline", "modelo": "media_dia_semana"}
  {"tipo": "lightgbm_cuantiles", "cuantiles": [...], "lightgbm": {...}, "semanas_entrenamiento": N}
"""

from __future__ import annotations

import re
from datetime import date

import numpy as np
import pandas as pd

from laboratorio.forecast import modelos
from laboratorio.forecast.dataset import con_etiquetas, cortes_de_entrenamiento, cortes_de_prueba
from laboratorio.forecast.lgbm import ModeloCuantiles
from laboratorio.forecast.lgbm import pronosticar as pronosticar_lgbm
from laboratorio.monitoring.desempeno import evaluar
from laboratorio.orders.pedido import calcular_pedido

CLAVES_REAL = ["date", "store_nbr", "family", "sales"]


def receta_baseline(cfg: dict) -> dict:
    return {"tipo": "baseline", "modelo": cfg["modelado"]["baseline"]}


def receta_lightgbm(cfg: dict) -> dict:
    m = cfg["modelado"]
    return {
        "tipo": modelos.LIGHTGBM,
        "cuantiles": m["cuantiles"],
        "lightgbm": m["lightgbm"],
        "semanas_entrenamiento": m["semanas_entrenamiento"],
    }


def entrenar_receta(receta: dict, entrenamiento: pd.DataFrame) -> ModeloCuantiles:
    """Entrena la receta con filas que ya traen `sales`."""
    return ModeloCuantiles(receta["cuantiles"], receta["lightgbm"]).entrenar(entrenamiento, entrenamiento["sales"])


def pronosticar_fold(receta: dict, cfg: dict, corte: date, predictores: pd.DataFrame, etiquetas: pd.DataFrame) -> pd.DataFrame:
    prueba = predictores[predictores["corte"] == corte]
    if receta["tipo"] == "baseline":
        return modelos.pronosticar(prueba, receta["modelo"])
    cortes = cortes_de_entrenamiento(cfg, corte, receta["semanas_entrenamiento"])
    entrenamiento = con_etiquetas(predictores[predictores["corte"].isin(cortes)], etiquetas)
    return pronosticar_lgbm(entrenar_receta(receta, entrenamiento), prueba)


def _niveles_cuantil(pron: pd.DataFrame) -> dict[str, float]:
    return {c: int(c[1:]) / 100 for c in pron.columns if re.fullmatch(r"q\d\d", c)}


def componentes_fold(pron: pd.DataFrame, pred: pd.DataFrame, etiquetas: pd.DataFrame, costos: dict, umbral_wape: float) -> dict:
    """Sumas aditivas del fold (se agregan entre folds en `agregar`)."""
    from laboratorio.features.predictores import CLAVES

    objetivos = pron["fecha_objetivo"].unique()
    reales = etiquetas[etiquetas["date"].isin(objetivos)][CLAVES_REAL]
    pedido = calcular_pedido(pron, pred, costos)
    total = evaluar(pron, pedido, reales, umbral_wape).query("family == 'TOTAL'").iloc[0]
    if not total["horizonte_completo"]:
        raise ValueError("el fold de prueba no tiene el horizonte completo observado")
    y = total["venta_real"]
    comp = {
        "filas": int(total["filas"]),
        "venta_real": float(y),
        "error_abs": float(total["wape"] * y),
        "error_neto": float(total["sesgo"] * y),
        "error_log_cuad": float(total["rmsle"] ** 2 * total["filas"]),
        "costo": float(total["costo"]),
        "faltante": float(total["faltante"]),
        "sobrante": float(total["sobrante"]),
        "cantidad_pedida": float(total["cantidad_pedida"]),
    }
    niveles = _niveles_cuantil(pron)
    if niveles:
        m = pron.merge(reales.rename(columns={"date": "fecha_objetivo"}), on=[*CLAVES, "fecha_objetivo"])
        comp["pinball"] = {}
        for col, q in niveles.items():
            d = m["sales"] - m[col]
            comp["pinball"][col] = float(np.maximum(q * d, (q - 1) * d).sum())
        bajo, alto = min(niveles, key=niveles.get), max(niveles, key=niveles.get)
        comp["dentro_intervalo"] = int(((m["sales"] >= m[bajo]) & (m["sales"] <= m[alto])).sum())
    return comp


def agregar(componentes: list[dict]) -> dict:
    """Métricas técnicas y de negocio de uno o más folds (se suman antes de dividir)."""
    s = {k: sum(c[k] for c in componentes) for k in ("filas", "venta_real", "error_abs", "error_neto", "error_log_cuad", "costo", "faltante", "sobrante", "cantidad_pedida")}  # fmt: skip
    y = s["venta_real"]
    out = {
        "wape": s["error_abs"] / y,
        "sesgo": s["error_neto"] / y,
        "rmsle": float(np.sqrt(s["error_log_cuad"] / s["filas"])),
        "costo": s["costo"],
        "costo_por_unidad_vendida": s["costo"] / y,
        "faltante": s["faltante"],
        "sobrante": s["sobrante"],
        "fill_rate": 1 - s["faltante"] / y,
        "cantidad_pedida": s["cantidad_pedida"],
        "venta_real": y,
        "costo_por_fold": [c["costo"] for c in componentes],
    }
    if all("pinball" in c for c in componentes):
        for col in componentes[0]["pinball"]:
            out[f"pinball_{col}"] = sum(c["pinball"][col] for c in componentes) / y
        out["cobertura"] = sum(c["dentro_intervalo"] for c in componentes) / s["filas"]
    return out


def backtest(receta: dict, cfg: dict, predictores: pd.DataFrame, etiquetas: pd.DataFrame, costos: dict) -> dict:
    """Métricas agregadas y por fold de la receta en los cortes de prueba."""
    umbral = cfg["seguimiento"]["umbral_wape"]
    folds = cortes_de_prueba(cfg)
    comps = []
    for corte in folds:
        pron = pronosticar_fold(receta, cfg, corte, predictores, etiquetas)
        pred = predictores[predictores["corte"] == corte]
        comps.append(componentes_fold(pron, pred, etiquetas, costos, umbral))
    return {
        "receta": receta,
        "cortes_prueba": [c.isoformat() for c in folds],
        **agregar(comps),
        "por_fold": [{"corte": c.isoformat(), **agregar([k])} for c, k in zip(folds, comps, strict=True)],
    }
