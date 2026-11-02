"""LightGBM por cuantiles: un modelo por cuantil sobre log1p(ventas).

Los cuantiles se conservan bajo transformaciones monótonas, así que predecir el
cuantil de log1p(y) y aplicar expm1 da el cuantil de y. La mediana es el pronóstico
y la dispersión (p90 - p10) da el sigma que usa el pedido. Los predictores son los
de `features.predictores` (todos as-of del corte).
"""

from __future__ import annotations

import json
from pathlib import Path
from statistics import NormalDist

import lightgbm as lgb
import numpy as np
import pandas as pd

NUMERICAS = [
    "h", "dias_desde_ultimo_dato", "dia_semana", "dia_mes", "mes", "quincena",
    "media_7", "media_28", "std_28", "ceros_28", "promo_28", "ultima_venta", "media_dia_semana",
    "feriado_nacional", "feriado_regional", "feriado_local", "evento", "dia_laboral_compensado",
    "tienda_cluster", "transacciones_28", "petroleo_ultimo",
]  # fmt: skip
CATEGORICAS = ["store_nbr", "family", "tienda_tipo"]


def nombre_cuantil(q: float) -> str:
    return f"q{round(q * 100):02d}"


def matriz(predictores: pd.DataFrame) -> pd.DataFrame:
    """Matriz de entrada del modelo a partir de los predictores de uno o más cortes."""
    x = predictores[NUMERICAS].apply(pd.to_numeric, errors="coerce").astype("float32")
    x["razon_dia_semana"] = x["media_dia_semana"] / (x["media_28"] + 1.0)
    x["razon_7_28"] = x["media_7"] / (x["media_28"] + 1.0)
    for c in CATEGORICAS:
        x[c] = predictores[c].astype("category")
    return x


class ModeloCuantiles:
    def __init__(self, cuantiles: list[float], params: dict):
        if 0.5 not in cuantiles:
            raise ValueError("los cuantiles deben incluir 0.5 (el pronóstico es la mediana)")
        self.cuantiles = sorted(cuantiles)
        self.params = dict(params)
        self.boosters: dict[float, lgb.Booster] = {}

    def entrenar(self, predictores: pd.DataFrame, ventas: pd.Series) -> ModeloCuantiles:
        x, y = matriz(predictores), np.log1p(ventas.to_numpy(dtype="float64"))
        p = dict(self.params)
        n, hilos = p.pop("n_estimators"), p.pop("n_jobs", 4)
        base = {**p, "verbosity": -1, "deterministic": True, "force_row_wise": True, "num_threads": hilos}
        for q in self.cuantiles:
            datos = lgb.Dataset(x, y, categorical_feature=CATEGORICAS, free_raw_data=False)
            self.boosters[q] = lgb.train({**base, "objective": "quantile", "alpha": q}, datos, num_boost_round=n)
        return self

    def predecir(self, predictores: pd.DataFrame) -> pd.DataFrame:
        """Ventas esperadas por cuantil (columnas q10, q50, ...), sin cruces entre cuantiles."""
        x = matriz(predictores)
        crudo = np.column_stack([self.boosters[q].predict(x) for q in self.cuantiles])
        valores = np.sort(np.clip(np.expm1(crudo), 0.0, None), axis=1)
        return pd.DataFrame(valores, columns=[nombre_cuantil(q) for q in self.cuantiles], index=predictores.index)

    def sigma_diario(self, cuantiles: pd.DataFrame) -> pd.Series:
        """Desviación diaria implícita en el intervalo [cuantil menor, cuantil mayor] (normal)."""
        bajo, alto = self.cuantiles[0], self.cuantiles[-1]
        ancho = NormalDist().inv_cdf(alto) - NormalDist().inv_cdf(bajo)
        return (cuantiles[nombre_cuantil(alto)] - cuantiles[nombre_cuantil(bajo)]) / ancho

    def guardar(self, directorio: Path) -> None:
        directorio.mkdir(parents=True, exist_ok=True)
        for q, b in self.boosters.items():
            b.save_model(str(directorio / f"{nombre_cuantil(q)}.txt"))
        (directorio / "cuantiles.json").write_text(json.dumps({"cuantiles": self.cuantiles}), encoding="utf-8")

    @classmethod
    def cargar(cls, directorio: Path) -> ModeloCuantiles:
        cuantiles = json.loads((directorio / "cuantiles.json").read_text(encoding="utf-8"))["cuantiles"]
        m = cls(cuantiles, {})
        m.boosters = {q: lgb.Booster(model_file=str(directorio / f"{nombre_cuantil(q)}.txt")) for q in m.cuantiles}
        return m


def pronosticar(modelo: ModeloCuantiles, predictores: pd.DataFrame) -> pd.DataFrame:
    """Pronóstico (mediana), cuantiles y sigma diario por (tienda, familia, fecha_objetivo)."""
    c = modelo.predecir(predictores)
    out = predictores[["store_nbr", "family", "fecha_objetivo", "h"]].copy()
    out["pronostico"] = c[nombre_cuantil(0.5)].round(3)
    for col in c.columns:
        out[col] = c[col].round(3)
    out["sigma_diario"] = modelo.sigma_diario(c).round(3)
    return out
