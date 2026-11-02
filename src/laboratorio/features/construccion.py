"""Predictores de un corte leyendo las capas as-of (sin persistir).

Lo usan la preparación diaria de features (`tareas.preparar_features`) y la
construcción del conjunto de entrenamiento: ambos calculan exactamente lo mismo.
"""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from laboratorio.capas import leer_asof
from laboratorio.config import RAIZ
from laboratorio.features.predictores import preparar_predictores
from laboratorio.ingest.ingesta import DIR_REFERENCIA

FUENTES = ("ventas", "transacciones", "petroleo")


def predictores_del_corte(cfg: dict, corte: date) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    """Predictores as-of de `corte` y las entradas as-of que se usaron (para el linaje)."""
    f, p = cfg["features"], cfg["pronostico"]
    silver, bronze = RAIZ / cfg["capas"]["silver"], RAIZ / cfg["capas"]["bronze"]
    desde = corte - timedelta(days=f["ventana_dias"] + 7)
    entradas = {fuente: leer_asof(silver, fuente, corte, desde=desde) for fuente in FUENTES}
    df = preparar_predictores(
        entradas["ventas"],
        entradas["transacciones"],
        entradas["petroleo"],
        pd.read_parquet(bronze / DIR_REFERENCIA / "stores.parquet"),
        pd.read_parquet(bronze / DIR_REFERENCIA / "holidays_events.parquet"),
        corte,
        f["ventana_dias"],
        p["horizonte_dias"],
        p["anticipacion_dias"],
    )
    return df, entradas
