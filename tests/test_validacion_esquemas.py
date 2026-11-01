"""Esquemas pandera: tipos, rangos, nulos, duplicados y fechas esperadas."""

from datetime import date

import pandas as pd
import pytest

from laboratorio.config import cargar_config
from laboratorio.validate.validacion import Reglas, validar_revision

FECHA = date(2017, 1, 2)
TIENDAS = {1, 2}
REGLAS = {n: Reglas.desde_dict(r) for n, r in cargar_config()["validacion"].items()}


def control(df, available_at="2017-01-03", revision=1):
    return df.assign(revision=revision, available_at=pd.Timestamp(available_at), origen="prueba.csv")


VENTAS = control(
    pd.DataFrame(
        {
            "id": [10, 11, 12],
            "date": ["2017-01-02"] * 3,
            "store_nbr": [1, 1, 2],
            "family": ["AUTOMOTIVE", "BEVERAGES", "AUTOMOTIVE"],
            "sales": [5.0, 1520.0, 0.0],
            "onpromotion": [0, 3, 0],
        }
    )
)
PETROLEO = control(pd.DataFrame({"date": ["2017-01-02"], "dcoilwtico": [52.36]}))
TRANSACCIONES = control(pd.DataFrame({"date": ["2017-01-02"] * 2, "store_nbr": [1, 2], "transactions": [1820, 1450]}))


@pytest.mark.parametrize(
    ("fuente", "df"),
    [
        ("ventas", VENTAS),
        ("transacciones", TRANSACCIONES),
        ("petroleo", PETROLEO),
        ("petroleo", PETROLEO.assign(dcoilwtico=float("nan"))),  # el petróleo puede venir vacío
        ("ventas", VENTAS.assign(onpromotion=[0.0, 3.0, 0.0])),  # enteros leídos como float: se convierten
        ("ventas", VENTAS.assign(available_at=pd.Timestamp("2017-01-02"))),  # disponible el mismo día
    ],
)
def test_revisiones_validas(fuente, df):
    validado, motivos = validar_revision(df, FECHA, REGLAS[fuente], TIENDAS)
    assert motivos == []
    assert len(validado) == len(df)


INVALIDAS = {
    # esquema
    "columna faltante": ("ventas", VENTAS.drop(columns="onpromotion"), "columna faltante: onpromotion"),
    "columna extra": ("ventas", VENTAS.assign(extra=1), "columna no esperada: extra"),
    "tipo inválido": ("ventas", VENTAS.assign(sales=["abc", "1", "2"]), "sales: tipo inválido"),
    "sin revision": ("ventas", VENTAS.drop(columns="revision"), "columna faltante: revision"),
    "sin available_at": ("ventas", VENTAS.drop(columns="available_at"), "columna faltante: available_at"),
    "revisión 0": ("ventas", VENTAS.assign(revision=0), "revision: revisión inválida"),
    "sin filas": ("ventas", VENTAS.head(0), "sin filas"),
    # rangos y nulos
    "venta negativa": ("ventas", VENTAS.assign(sales=[5.0, -1.0, 0.0]), "sales: menor que 0"),
    "promoción negativa": ("ventas", VENTAS.assign(onpromotion=[0, -3, 0]), "onpromotion: menor que 0"),
    "petróleo fuera de rango": ("petroleo", PETROLEO.assign(dcoilwtico=500.0), "dcoilwtico: mayor que 200"),
    "transacciones negativas": ("transacciones", TRANSACCIONES.assign(transactions=[-5, 1]), "menor que 0"),
    "nulos no permitidos": ("ventas", VENTAS.assign(family=[None, "A", "B"]), "family: nulos no permitidos"),
    "tienda desconocida": ("ventas", VENTAS.assign(store_nbr=[1, 1, 99]), "store_nbr: tienda desconocida"),
    # duplicados
    "clave duplicada": ("ventas", pd.concat([VENTAS, VENTAS.head(1)], ignore_index=True), "claves duplicadas"),
    "petróleo duplicado": ("petroleo", pd.concat([PETROLEO, PETROLEO], ignore_index=True), "claves duplicadas"),
    # fechas esperadas
    "fecha de otra partición": ("ventas", VENTAS.assign(date="2016-12-31"), "fecha distinta a la esperada"),
    "fecha mal escrita": ("ventas", VENTAS.assign(date="02/01/2017"), "fecha distinta a la esperada"),
    "dato antes de existir": ("ventas", control(VENTAS, available_at="2017-01-01"), "date posterior a available_at"),
}


@pytest.mark.parametrize("caso", INVALIDAS)
def test_revisiones_invalidas(caso):
    fuente, df, esperado = INVALIDAS[caso]
    validado, motivos = validar_revision(df, FECHA, REGLAS[fuente], TIENDAS)
    assert validado is None
    assert any(esperado in m for m in motivos), motivos
