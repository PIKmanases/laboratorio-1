"""Desempeño de un pronóstico y su pedido frente a las ventas reales conocidas.

Por familia y en total (`family = "TOTAL"`):
- pronóstico: WAPE, sesgo y RMSLE sobre los días ya observados;
- pedido (cuando el horizonte está completo): faltante, sobrante, costo y fill rate.

Las ventas reales se leen as-of el día del seguimiento, así que una partición de
desempeño se recalcula cuando llegan más días observados o una revisión de un día
ya evaluado.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from laboratorio.features.predictores import CLAVES


def _metricas(g: pd.DataFrame) -> dict:
    y, yhat = g["sales"], g["pronostico"]
    total = y.sum()
    return {
        "filas": len(g),
        "venta_real": total,
        "venta_pronosticada": yhat.sum(),
        "wape": (y - yhat).abs().sum() / total if total > 0 else np.nan,
        "sesgo": (yhat - y).sum() / total if total > 0 else np.nan,
        "rmsle": float(np.sqrt(np.mean((np.log1p(yhat) - np.log1p(y)) ** 2))) if len(g) else np.nan,
    }


def _metricas_pedido(g: pd.DataFrame) -> dict:
    faltante = (g["demanda_real"] - g["cantidad_pedido"]).clip(lower=0)
    sobrante = (g["cantidad_pedido"] - g["demanda_real"]).clip(lower=0)
    demanda = g["demanda_real"].sum()
    return {
        "cantidad_pedida": g["cantidad_pedido"].sum(),
        "faltante": faltante.sum(),
        "sobrante": sobrante.sum(),
        "costo": (faltante * g["costo_faltante"] + sobrante * g["costo_sobrante"]).sum(),
        "fill_rate": 1 - faltante.sum() / demanda if demanda > 0 else np.nan,
    }


def evaluar(pronostico: pd.DataFrame, pedido: pd.DataFrame, reales: pd.DataFrame, umbral_wape: float) -> pd.DataFrame:
    reales = reales.rename(columns={"date": "fecha_objetivo"})[[*CLAVES, "fecha_objetivo", "sales"]]
    m = pronostico.merge(reales, on=[*CLAVES, "fecha_objetivo"], how="inner")
    dias_horizonte = pronostico["fecha_objetivo"].nunique()
    dias_observados = m["fecha_objetivo"].nunique()
    completo = dias_observados == dias_horizonte

    filas = [{"family": fam, **_metricas(g)} for fam, g in m.groupby("family")]
    filas.append({"family": "TOTAL", **_metricas(m)})
    df = pd.DataFrame(filas)

    if completo and not pedido.empty:
        demanda = m.groupby(CLAVES, as_index=False)["sales"].sum().rename(columns={"sales": "demanda_real"})
        p = pedido.merge(demanda, on=CLAVES, how="inner")
        ped = [{"family": fam, **_metricas_pedido(g)} for fam, g in p.groupby("family")]
        ped.append({"family": "TOTAL", **_metricas_pedido(p)})
        df = df.merge(pd.DataFrame(ped), on="family", how="left")

    df["dias_horizonte"] = dias_horizonte
    df["dias_observados"] = dias_observados
    df["horizonte_completo"] = completo
    df["alerta"] = (df["family"] == "TOTAL") & (df["wape"] > umbral_wape)
    return df
