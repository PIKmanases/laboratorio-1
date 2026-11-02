"""Predictores as-of para pronosticar desde una fecha de corte.

Para el corte X se arma una fila por (tienda, familia, fecha_objetivo) con
fecha_objetivo en X+anticipación ... X+anticipación+horizonte-1. Todo se calcula
con datos leídos con `load_as_of(..., X)`: lo que se sabía el día X, nunca después.

- Historia de ventas (últimos `ventana_dias` conocidos): media de 7 y 28 días,
  desviación, % de días en cero, promoción media, última venta y media del mismo
  día de la semana que el objetivo.
- Calendario del objetivo: día de la semana, del mes, mes, quincena y feriados
  nacionales/regionales/locales de la tienda (el calendario se publica por adelantado).
- Contexto: transacciones medias de la tienda, último precio del petróleo, tipo y
  cluster de la tienda.

Los predictores no cambian si después llega una revisión: lo que se sabía el día X
no cambia. Las revisiones nuevas entran en los cortes posteriores.
"""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

CLAVES = ["store_nbr", "family"]
TIPOS_FERIADO = ["Holiday", "Transfer", "Additional", "Bridge"]


def _historia_ventas(ventas: pd.DataFrame, ventana: int) -> tuple[pd.DataFrame, pd.DataFrame, pd.Timestamp]:
    v = ventas.assign(fecha=pd.to_datetime(ventas["date"]))
    ultima = v["fecha"].max()
    v = v[v["fecha"] > ultima - pd.Timedelta(days=ventana)]
    g = v.groupby(CLAVES)
    base = pd.DataFrame(
        {
            "media_7": v[v["fecha"] > ultima - pd.Timedelta(days=7)].groupby(CLAVES)["sales"].mean(),
            "media_28": g["sales"].mean(),
            "std_28": g["sales"].std(ddof=0),
            "ceros_28": v["sales"].eq(0).groupby([v["store_nbr"], v["family"]]).mean(),
            "promo_28": g["onpromotion"].mean(),
            "ultima_venta": v[v["fecha"] == ultima].set_index(CLAVES)["sales"],
        }
    ).reset_index()
    por_dia = (
        v.assign(dia_semana=v["fecha"].dt.dayofweek)
        .groupby([*CLAVES, "dia_semana"])["sales"]
        .mean()
        .rename("media_dia_semana")
        .reset_index()
    )
    return base, por_dia, ultima


def _feriados(objetivos: pd.DataFrame, tiendas: pd.DataFrame, feriados: pd.DataFrame) -> pd.DataFrame:
    """Marcas de feriado por (tienda, fecha_objetivo). Un feriado trasladado no se celebra ese día."""
    f = feriados.assign(fecha=pd.to_datetime(feriados["date"]))
    f = f[~f["transferred"].astype(bool) & f["fecha"].isin(objetivos["fecha_objetivo"])]
    feriado = f[f["type"].isin(TIPOS_FERIADO)]

    df = tiendas[["store_nbr", "city", "state"]].merge(objetivos, how="cross")
    clave_estado = pd.MultiIndex.from_frame(df[["fecha_objetivo", "state"]])
    clave_ciudad = pd.MultiIndex.from_frame(df[["fecha_objetivo", "city"]])
    regional = feriado[feriado["locale"] == "Regional"]
    local = feriado[feriado["locale"] == "Local"]
    return pd.DataFrame(
        {
            "store_nbr": df["store_nbr"],
            "fecha_objetivo": df["fecha_objetivo"],
            "feriado_nacional": df["fecha_objetivo"].isin(feriado.loc[feriado["locale"] == "National", "fecha"]),
            "feriado_regional": clave_estado.isin(pd.MultiIndex.from_frame(regional[["fecha", "locale_name"]])),
            "feriado_local": clave_ciudad.isin(pd.MultiIndex.from_frame(local[["fecha", "locale_name"]])),
            "evento": df["fecha_objetivo"].isin(f.loc[f["type"] == "Event", "fecha"]),
            "dia_laboral_compensado": df["fecha_objetivo"].isin(f.loc[f["type"] == "Work Day", "fecha"]),
        }
    )


def preparar_predictores(
    ventas: pd.DataFrame,
    transacciones: pd.DataFrame,
    petroleo: pd.DataFrame,
    tiendas: pd.DataFrame,
    feriados: pd.DataFrame,
    cutoff: date,
    ventana_dias: int,
    horizonte_dias: int,
    anticipacion_dias: int,
) -> pd.DataFrame:
    """Predictores del corte `cutoff`. Las entradas deben venir de `load_as_of(..., cutoff)`."""
    if ventas.empty:
        return pd.DataFrame()
    base, por_dia, ultima = _historia_ventas(ventas, ventana_dias)

    inicio = pd.Timestamp(cutoff + timedelta(days=anticipacion_dias))
    objetivos = pd.DataFrame({"fecha_objetivo": pd.date_range(inicio, periods=horizonte_dias, freq="D")})
    df = base.merge(objetivos, how="cross")
    df["h"] = (df["fecha_objetivo"] - pd.Timestamp(cutoff)).dt.days
    df["dias_desde_ultimo_dato"] = (df["fecha_objetivo"] - ultima).dt.days
    df["dia_semana"] = df["fecha_objetivo"].dt.dayofweek
    df["dia_mes"] = df["fecha_objetivo"].dt.day
    df["mes"] = df["fecha_objetivo"].dt.month
    df["quincena"] = (df["dia_mes"] == 15) | df["fecha_objetivo"].dt.is_month_end
    df = df.merge(por_dia, on=[*CLAVES, "dia_semana"], how="left")
    df = df.merge(_feriados(objetivos, tiendas, feriados), on=["store_nbr", "fecha_objetivo"], how="left")

    tienda = tiendas[["store_nbr", "type", "cluster"]].rename(columns={"type": "tienda_tipo", "cluster": "tienda_cluster"})
    df = df.merge(tienda, on="store_nbr", how="left")
    if transacciones.empty:
        df["transacciones_28"] = float("nan")
    else:
        t = transacciones.assign(fecha=pd.to_datetime(transacciones["date"]))
        t = t[t["fecha"] > t["fecha"].max() - pd.Timedelta(days=ventana_dias)]
        df = df.merge(t.groupby("store_nbr")["transactions"].mean().rename("transacciones_28"), on="store_nbr", how="left")
    precios = petroleo.sort_values("date")["dcoilwtico"].dropna() if not petroleo.empty else pd.Series(dtype=float)
    df["petroleo_ultimo"] = precios.iloc[-1] if len(precios) else float("nan")

    df["fecha_objetivo"] = df["fecha_objetivo"].dt.date.astype(str)
    return df.sort_values([*CLAVES, "fecha_objetivo"], ignore_index=True)
