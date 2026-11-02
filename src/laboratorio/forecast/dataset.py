"""Conjunto de entrenamiento y prueba con esquema temporal (rolling origin), sin fuga.

- Un corte es un lunes (día de pronóstico). Sus predictores se calculan solo con lo
  conocido ese día (`features.construccion`); la variable objetivo son las ventas de
  los 7 días siguientes.
- Para probar en el corte F se entrena solo con cortes `c <= F - dias_espera_etiquetas`:
  así las ventas objetivo de `c` (y su revisión `_rev2`) ya se conocían el día F.
- Los folds de prueba son los últimos lunes cuyo horizonte completo ya se observó.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta

import pandas as pd

from laboratorio.capas import leer_asof
from laboratorio.config import RAIZ
from laboratorio.features.construccion import predictores_del_corte

log = logging.getLogger(__name__)


def cortes_semanales(hasta: date, cantidad: int, dia_semana: int) -> list[date]:
    """Los últimos `cantidad` días con ese día de la semana, hasta `hasta` inclusive (orden cronológico)."""
    ultimo = hasta - timedelta(days=(hasta.weekday() - dia_semana) % 7)
    return [ultimo - timedelta(weeks=i) for i in range(cantidad - 1, -1, -1)]


def fecha_entrenamiento(cfg: dict) -> date:
    """Día simulado de la corrida: el último con datos en las capas."""
    return date.fromisoformat(cfg["orquestacion"]["fin"])


def cortes_de_prueba(cfg: dict) -> list[date]:
    p, m, o = cfg["pronostico"], cfg["modelado"], cfg["orquestacion"]
    ultima_venta = date.fromisoformat(cfg["bandeja"]["fecha_fin"])
    ultimo_objetivo_a_dias = p["anticipacion_dias"] + p["horizonte_dias"] - 1
    hasta = ultima_venta - timedelta(days=ultimo_objetivo_a_dias)  # horizonte completo observado
    return cortes_semanales(hasta, m["folds"], o["dia_semana_pronostico"])


def cortes_de_entrenamiento(cfg: dict, corte_prueba: date | None = None, semanas: int | None = None) -> list[date]:
    """Cortes con etiquetas ya conocidas el día de la prueba (o de la corrida final si es None)."""
    m = cfg["modelado"]
    referencia = corte_prueba or fecha_entrenamiento(cfg)
    hasta = referencia - timedelta(days=m["dias_espera_etiquetas"])
    return cortes_semanales(hasta, semanas or m["semanas_entrenamiento"], cfg["orquestacion"]["dia_semana_pronostico"])


def todos_los_cortes(cfg: dict) -> list[date]:
    cortes = {*cortes_de_prueba(cfg), *cortes_de_entrenamiento(cfg)}
    for f in cortes_de_prueba(cfg):
        cortes.update(cortes_de_entrenamiento(cfg, f))
    return sorted(cortes)


def construir_predictores(cfg: dict, cortes: list[date]) -> pd.DataFrame:
    """Predictores as-of de cada corte, apilados con la columna `corte`."""
    partes = []
    for c in cortes:
        df, _ = predictores_del_corte(cfg, c)
        if not df.empty:
            partes.append(df.assign(corte=c))
        log.info("predictores %s: %d filas", c, len(df))
    return pd.concat(partes, ignore_index=True) if partes else pd.DataFrame()


def leer_etiquetas(cfg: dict, cortes: list[date]) -> pd.DataFrame:
    """Ventas observadas de los días objetivo de `cortes`, tal como se conocen en la fecha de la corrida."""
    p = cfg["pronostico"]
    desde = min(cortes) + timedelta(days=p["anticipacion_dias"])
    hasta = max(cortes) + timedelta(days=p["anticipacion_dias"] + p["horizonte_dias"] - 1)
    ventas = leer_asof(RAIZ / cfg["capas"]["silver"], "ventas", fecha_entrenamiento(cfg), desde=desde, hasta=hasta)
    return ventas[["date", "store_nbr", "family", "sales"]]


def con_etiquetas(predictores: pd.DataFrame, etiquetas: pd.DataFrame) -> pd.DataFrame:
    """Une la venta observada de cada fila; descarta filas sin venta conocida."""
    e = etiquetas.rename(columns={"date": "fecha_objetivo"})
    return predictores.merge(e, on=["store_nbr", "family", "fecha_objetivo"], how="inner")
