"""load_as_of(source, cutoff): lo que se sabía al cierre del corte, nunca datos posteriores."""

import zipfile
from datetime import date, timedelta

import pandas as pd
import pytest
from conftest import CORTE, FIN, ULTIMO_DIA, correr_hasta, entregas_planeadas, revisadas

from laboratorio.capas import (
    COLUMNAS_CONTROL,
    FugaDeInformacion,
    escribir_parquet,
    load_as_of,
    ruta_revision,
)

FUENTES = ["ventas", "transacciones", "petroleo"]
CORTES = [CORTE + timedelta(days=i) for i in range(-2, (ULTIMO_DIA - CORTE).days + 3)]


@pytest.fixture
def flujo(entorno):
    correr_hasta(entorno, ULTIMO_DIA)
    return entorno


def capas(e):
    return {"bronze": e["ing"]["dir_bronze"], "silver": e["val"]["dir_silver"]}


def esperado(e, fuente, cutoff):
    """(fecha, revision) que debería devolver load_as_of, calculado del plan del simulador."""
    esperado = {}
    if cutoff >= CORTE:  # la carga histórica está disponible desde la fecha de corte
        with zipfile.ZipFile(e["ing"]["ruta_zip"]) as z:
            fechas = pd.read_csv(z.open(e["ing"]["archivos"][fuente]))["date"]
        esperado |= {date.fromisoformat(f): 1 for f in fechas if f <= CORTE.isoformat()}
    for x in entregas_planeadas(e):
        if x.fuente == fuente and x.fecha_llegada <= cutoff:
            esperado[x.fecha_dato] = max(esperado.get(x.fecha_dato, 0), x.revision)
    return esperado


@pytest.mark.parametrize("cutoff", CORTES, ids=str)
def test_nunca_devuelve_datos_posteriores_al_corte(flujo, cutoff):
    for capa, dir_capa in capas(flujo).items():
        for fuente in FUENTES:
            df = load_as_of(fuente, cutoff, dir_capa=dir_capa)
            if df.empty:
                continue
            assert (pd.to_datetime(df["date"]) <= pd.Timestamp(cutoff)).all(), (capa, fuente)
            assert (df["available_at"] <= pd.Timestamp(cutoff)).all(), (capa, fuente)


@pytest.mark.parametrize("cutoff", CORTES, ids=str)
def test_devuelve_exactamente_la_ultima_revision_conocida(flujo, cutoff):
    for fuente in FUENTES:
        df = load_as_of(fuente, cutoff, dir_capa=flujo["val"]["dir_silver"])
        obtenido = {} if df.empty else {
            date.fromisoformat(f): int(r) for f, r in df[["date", "revision"]].drop_duplicates().itertuples(index=False)
        }
        assert obtenido == esperado(flujo, fuente, cutoff), (fuente, cutoff)


def test_antes_del_corte_historico_no_hay_nada(flujo):
    for fuente in FUENTES:
        assert load_as_of(fuente, CORTE - timedelta(days=1), dir_capa=flujo["val"]["dir_silver"]).empty


def test_rev2_solo_desde_el_dia_en_que_llega(flujo):
    for (fuente, fecha), llegada in revisadas(flujo).items():
        def rev(cutoff, fuente=fuente, fecha=fecha):
            df = load_as_of(fuente, cutoff, desde=fecha, hasta=fecha, dir_capa=flujo["val"]["dir_silver"])
            return set(df["revision"])

        assert rev(llegada - timedelta(days=1)) == {1}
        assert rev(llegada) == {2}


def test_acepta_cutoff_como_texto(flujo):
    dir_silver = flujo["val"]["dir_silver"]
    pd.testing.assert_frame_equal(
        load_as_of("ventas", "2017-01-04", dir_capa=dir_silver),
        load_as_of("ventas", date(2017, 1, 4), dir_capa=dir_silver),
    )


def test_ignora_revisiones_y_particiones_del_futuro(flujo):
    dir_silver = flujo["val"]["dir_silver"]
    cutoff = date(2017, 1, 5)
    antes = load_as_of("ventas", cutoff, dir_capa=dir_silver)

    base = antes[antes["date"] == "2017-01-02"].drop(columns=COLUMNAS_CONTROL)
    # Una revisión de una fecha pasada que se conocerá después del corte...
    futura = base.assign(sales=0.0, revision=9, available_at=pd.Timestamp("2017-01-06"), origen="futuro")
    escribir_parquet(futura, ruta_revision(dir_silver, "ventas", date(2017, 1, 2), 9))
    # ...y una partición de una fecha posterior al corte.
    manana = base.assign(date="2017-01-06", revision=1, available_at=pd.Timestamp("2017-01-06"), origen="futuro")
    escribir_parquet(manana, ruta_revision(dir_silver, "ventas", date(2017, 1, 6), 1))

    pd.testing.assert_frame_equal(load_as_of("ventas", cutoff, dir_capa=dir_silver), antes)


def test_falla_si_una_particion_esconde_filas_del_futuro(flujo):
    """Defensa en profundidad: si una fila dice ser posterior al corte, load_as_of falla en vez de devolverla."""
    dir_silver = flujo["val"]["dir_silver"]
    base = load_as_of("ventas", FIN, dir_capa=dir_silver)
    trampa = base.head(2).drop(columns=COLUMNAS_CONTROL)
    trampa = trampa.assign(date="2017-01-09", revision=9, available_at=pd.Timestamp("2017-01-04"), origen="trampa")
    escribir_parquet(trampa, ruta_revision(dir_silver, "ventas", date(2017, 1, 2), 9))

    with pytest.raises(FugaDeInformacion):
        load_as_of("ventas", date(2017, 1, 5), dir_capa=dir_silver)
