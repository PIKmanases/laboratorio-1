"""Pruebas del simulador de bandeja y de la ingesta (datos de ejemplo de tests/data)."""

import hashlib
import zipfile
from dataclasses import replace
from datetime import date

import pandas as pd
import pytest

from laboratorio.config import cargar_config
from laboratorio.inbox.simulador import desde_config, generar_bandeja
from laboratorio.ingest.bandeja import leer_fuente

CORTE, FIN = date(2017, 1, 1), date(2017, 1, 3)  # datos de ejemplo: 2017-01-01..03


@pytest.fixture
def kw(zip_kaggle_ejemplo, tmp_path):
    """Config real del proyecto, con fechas y zip de los datos de ejemplo."""
    kw = desde_config(cargar_config())
    kw.update(
        ruta_zip=zip_kaggle_ejemplo,
        dir_bandeja=tmp_path / "inbox",
        fecha_corte=CORTE,
        fecha_fin=FIN,
        revisiones=replace(kw["revisiones"], fraccion_fechas=0.5),  # 1 de las 2 fechas
    )
    return kw


def _nombres(dir_bandeja):
    return sorted(p.relative_to(dir_bandeja).as_posix() for p in dir_bandeja.rglob("*.csv"))


def _instantanea(dir_bandeja):
    return {
        p.relative_to(dir_bandeja).as_posix(): (hashlib.md5(p.read_bytes()).hexdigest(), p.stat().st_mtime_ns)
        for p in dir_bandeja.rglob("*")
        if p.is_file()
    }


def _original(zip_kaggle_ejemplo, archivo, fecha):
    with zipfile.ZipFile(zip_kaggle_ejemplo) as z:
        df = pd.read_csv(z.open(archivo))
    return df[df["date"] == fecha].reset_index(drop=True)


def test_un_archivo_por_fecha_y_fuente_solo_despues_del_corte(kw):
    entregas = generar_bandeja(**kw)
    originales = [e for e in entregas if e.revision == 1]
    assert {(e.fuente, e.fecha_dato) for e in originales} == {
        (f, d) for f in kw["fuentes"] for d in (date(2017, 1, 2), date(2017, 1, 3))
    }
    assert all(e.fecha_dato > CORTE for e in entregas)  # la carga histórica no pasa por la bandeja
    for e in entregas:
        df = pd.read_csv(kw["dir_bandeja"] / e.ruta_relativa)
        assert set(df["date"]) == {e.fecha_dato.isoformat()}


def test_retrasos_distintos_por_fuente(kw):
    retrasos = {n: f.retraso_dias for n, f in kw["fuentes"].items()}
    assert len(set(retrasos.values())) == len(retrasos)

    # Día simulado 2017-01-03: solo llegó lo que tiene fecha + retraso <= 03.
    entregas = generar_bandeja(**kw, hasta=date(2017, 1, 3))
    assert all(e.fecha_llegada <= date(2017, 1, 3) for e in entregas)
    assert _nombres(kw["dir_bandeja"]) == ["ventas/ventas_2017-01-02.csv"]
    for e in entregas:
        assert (e.fecha_llegada - e.fecha_dato).days == retrasos[e.fuente] + (e.revision - 1) * kw[
            "revisiones"
        ].retraso_extra_dias


def test_rev2_corrige_valores(kw, zip_kaggle_ejemplo):
    entregas = generar_bandeja(**kw)
    revs = [e for e in entregas if e.revision == 2]
    assert {e.fuente for e in revs} == set(kw["fuentes"])  # una fecha revisada por fuente

    for e in revs:
        fuente = kw["fuentes"][e.fuente]
        col = fuente.columna_valor
        orig = pd.read_csv(kw["dir_bandeja"] / replace(e, revision=1).ruta_relativa)
        rev2 = pd.read_csv(kw["dir_bandeja"] / e.ruta_relativa)
        real = _original(zip_kaggle_ejemplo, fuente.archivo, e.fecha_dato.isoformat())

        pd.testing.assert_frame_equal(rev2, real)  # el _rev2 trae el valor real
        assert not orig[col].equals(rev2[col])  # el original venía con errores
        pd.testing.assert_frame_equal(orig.drop(columns=col), rev2.drop(columns=col))  # solo cambia el valor


def test_generar_es_idempotente(kw):
    generar_bandeja(**kw)
    antes = {k: v[0] for k, v in _instantanea(kw["dir_bandeja"]).items()}
    generar_bandeja(**kw)
    assert {k: v[0] for k, v in _instantanea(kw["dir_bandeja"]).items()} == antes


def test_ingesta_usa_la_ultima_revision(kw, zip_kaggle_ejemplo):
    generar_bandeja(**kw)
    for nombre, fuente in kw["fuentes"].items():
        leido = leer_fuente(kw["dir_bandeja"], nombre)
        real = pd.concat(
            _original(zip_kaggle_ejemplo, fuente.archivo, d) for d in ("2017-01-02", "2017-01-03")
        ).reset_index(drop=True)
        pd.testing.assert_frame_equal(leido, real)


