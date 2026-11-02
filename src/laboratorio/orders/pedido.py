"""Pedido por (tienda, familia) con la regla del vendedor de periódicos (newsvendor).

La demanda del horizonte se aproxima con una normal: media = suma del pronóstico,
sigma = raíz de la suma de varianzas diarias (`sigma_diario` del modelo por cuantiles
o, si no existe, la desviación diaria de 28 días × raíz(horizonte)). Se pide el cuantil
costo_faltante / (costo_faltante + costo_sobrante). Supone inventario inicial cero
(no hay datos de inventario).
"""

from __future__ import annotations

from statistics import NormalDist

import numpy as np
import pandas as pd

from laboratorio.features.predictores import CLAVES


def costos_por_familia(costos: dict, familias: pd.Series) -> pd.DataFrame:
    defecto, especiales = costos["por_defecto"], costos.get("por_familia", {})
    filas = [{"family": f, **defecto, **especiales.get(f, {})} for f in sorted(familias.unique())]
    df = pd.DataFrame(filas)
    df["nivel_servicio"] = df["costo_faltante"] / (df["costo_faltante"] + df["costo_sobrante"])
    return df


def calcular_pedido(pronostico: pd.DataFrame, predictores: pd.DataFrame, costos: dict) -> pd.DataFrame:
    if pronostico.empty:
        return pd.DataFrame()
    horizonte = pronostico["fecha_objetivo"].nunique()
    df = pronostico.groupby(CLAVES, as_index=False)["pronostico"].sum().rename(columns={"pronostico": "demanda_esperada"})
    if "sigma_diario" in pronostico:  # modelo por cuantiles: sigma propio de cada día
        sigma = (pronostico["sigma_diario"] ** 2).groupby([pronostico[c] for c in CLAVES]).sum().pow(0.5)
        df = df.merge(sigma.rename("sigma").reset_index(), on=CLAVES, how="left")
        df["sigma"] = df["sigma"].fillna(0.0)
    else:
        sigma = predictores.groupby(CLAVES, as_index=False)["std_28"].first()
        df = df.merge(sigma, on=CLAVES, how="left")
        df["sigma"] = df.pop("std_28").fillna(0.0) * np.sqrt(horizonte)
    df = df.merge(costos_por_familia(costos, df["family"]), on="family", how="left")
    z = df["nivel_servicio"].map(NormalDist().inv_cdf)
    df["cantidad_pedido"] = (df["demanda_esperada"] + z * df["sigma"]).clip(lower=0).round(2)
    df["horizonte_dias"] = horizonte
    return df
