"""Ingesta, validación y capas as-of (datos de ejemplo de tests/data, sin dvc pull)."""

import hashlib
import os
import stat
import zipfile
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest

from laboratorio import capas
from laboratorio.capas import (
    COLUMNAS_CONTROL,
    escribir_parquet,
    leer_asof,
    listar_particiones,
    listar_revisiones,
    ruta_particion,
    ruta_revision,
)
from laboratorio.config import cargar_config
from laboratorio.inbox.simulador import desde_config as config_simulador
from laboratorio.inbox.simulador import generar_bandeja
from laboratorio.ingest.ingesta import desde_config as config_ingesta
from laboratorio.ingest.ingesta import ingestar
from laboratorio.validate.validacion import desde_config as config_validacion
from laboratorio.validate.validacion import validar

CORTE = date(2017, 1, 1)  # datos de ejemplo: 2017-01-01..03
FIN = date(2017, 1, 3)
ULTIMO_DIA = date(2017, 1, 10)  # para entonces ya llegó todo, incluidos los _rev2


@pytest.fixture
def entorno(zip_kaggle_ejemplo, tmp_path):
    cfg = cargar_config()
    sim = config_simulador(cfg)
    sim.update(
        ruta_zip=zip_kaggle_ejemplo,
        dir_bandeja=tmp_path / "inbox",
        fecha_corte=CORTE,
        fecha_fin=FIN,
        revisiones=replace(sim["revisiones"], fraccion_fechas=0.5),  # 1 de las 2 fechas por fuente
    )
    ing = config_ingesta(cfg, tmp_path)
    ing.update(ruta_zip=zip_kaggle_ejemplo, dir_bandeja=sim["dir_bandeja"], fecha_corte=CORTE)
    val = config_validacion(cfg, tmp_path)
    return {"sim": sim, "ing": ing, "val": val, "tmp": tmp_path}


def correr_dia(e, dia):
    """Un día del flujo: llega lo que toca a la bandeja, se ingesta y se valida."""
    generar_bandeja(**e["sim"], hasta=dia)
    ingestar(**e["ing"], fecha_proceso=dia)
    validar(**e["val"], fecha_proceso=dia)


def correr_hasta(e, ultimo):
    dia = CORTE
    while dia <= ultimo:
        correr_dia(e, dia)
        dia += timedelta(days=1)


def huellas(directorio):
    return {
        p.relative_to(directorio).as_posix(): (hashlib.md5(p.read_bytes()).hexdigest(), p.stat().st_mtime_ns)
        for p in directorio.rglob("*")
        if p.is_file()
    }


def kaggle(zip_kaggle_ejemplo, archivo, fechas):
    with zipfile.ZipFile(zip_kaggle_ejemplo) as z:
        df = pd.read_csv(z.open(archivo))
    return df[df["date"].isin(fechas)].reset_index(drop=True)


def revisadas(e):
    """(fuente, fecha) que reciben _rev2 y el día en que llega."""
    entregas = generar_bandeja(**{**e["sim"], "dir_bandeja": e["tmp"] / "plan"})
    return {(x.fuente, x.fecha_dato): x.fecha_llegada for x in entregas if x.revision == 2}


# --- Escritura por partición, atómica -------------------------------------------------


def test_reemplazo_atomico_no_deja_particiones_a_medias(tmp_path, monkeypatch):
    ruta = ruta_revision(tmp_path, "ventas", date(2017, 1, 2), 1)
    escribir_parquet(pd.DataFrame({"x": [1, 2]}), ruta)
    antes = ruta.read_bytes()

    def falla_a_mitad(self, destino, **kw):
        Path(destino).write_bytes(b"escritura incompleta")
        raise OSError("disco lleno")

    monkeypatch.setattr(pd.DataFrame, "to_parquet", falla_a_mitad)
    with pytest.raises(OSError):
        escribir_parquet(pd.DataFrame({"x": [9]}), ruta)

    assert ruta.read_bytes() == antes  # la partición anterior sigue intacta
    assert [p.name for p in ruta.parent.iterdir()] == [ruta.name]  # sin temporales


# --- Ingesta ---------------------------------------------------------------------------


def test_carga_historica_hasta_la_fecha_de_corte(entorno):
    ingestar(**entorno["ing"], fecha_proceso=CORTE)
    bronze = entorno["ing"]["dir_bronze"]
    for fuente in entorno["ing"]["archivos"]:
        for fecha, particion in listar_particiones(bronze, fuente).items():
            assert fecha <= CORTE
            assert list(listar_revisiones(particion)) == [1]
            df = pd.read_parquet(listar_revisiones(particion)[1])
            assert set(df["_fecha_ingesta"]) == {CORTE.isoformat()}
    assert (bronze / "referencia" / "stores.parquet").exists()


def test_ingesta_es_idempotente(entorno):
    correr_hasta(entorno, ULTIMO_DIA)
    capas_dirs = [entorno["ing"]["dir_bronze"], entorno["val"]["dir_silver"]]
    antes = [huellas(d) for d in capas_dirs]

    # Repetir días ya procesados no reescribe nada (ni contenido ni fecha de modificación).
    for dia in (date(2017, 1, 4), ULTIMO_DIA, ULTIMO_DIA):
        ingestar(**entorno["ing"], fecha_proceso=dia)
        validar(**entorno["val"], fecha_proceso=dia)
    assert [huellas(d) for d in capas_dirs] == antes


def test_nueva_revision_reemplaza_la_particion_conservando_las_anteriores(entorno):
    (fuente, fecha), llegada = min(revisadas(entorno).items())
    correr_hasta(entorno, llegada)
    revs = listar_revisiones(ruta_particion(entorno["ing"]["dir_bronze"], fuente, fecha))
    assert list(revs) == [1, 2]  # el _rev2 se agrega sin borrar el original
    assert set(pd.read_parquet(revs[2])["_fecha_ingesta"]) == {llegada.isoformat()}


def test_ingesta_nunca_escribe_en_inbox(entorno):
    generar_bandeja(**entorno["sim"])
    bandeja = entorno["sim"]["dir_bandeja"]
    antes = huellas(bandeja)

    rutas = [bandeja, *bandeja.rglob("*")]
    for p in rutas:  # solo lectura: cualquier intento de escritura falla
        os.chmod(p, stat.S_IRUSR | (stat.S_IXUSR if p.is_dir() else 0))
    try:
        ingestar(**entorno["ing"], fecha_proceso=ULTIMO_DIA)
        ingestar(**entorno["ing"], fecha_proceso=ULTIMO_DIA)
    finally:
        for p in rutas:
            os.chmod(p, stat.S_IRWXU)
    assert huellas(bandeja) == antes


# --- Capas as-of -----------------------------------------------------------------------


def test_asof_devuelve_lo_que_se_sabia_ese_dia(entorno, zip_kaggle_ejemplo):
    revs = revisadas(entorno)
    correr_hasta(entorno, ULTIMO_DIA)
    for (fuente, fecha), llegada in revs.items():
        archivo = entorno["ing"]["archivos"][fuente]
        col = entorno["sim"]["fuentes"][fuente].columna_valor
        real = kaggle(zip_kaggle_ejemplo, archivo, [fecha.isoformat()])
        for capa in (entorno["ing"]["dir_bronze"], entorno["val"]["dir_silver"]):
            # Un día antes de que llegue el _rev2 se ve la versión original (errónea)...
            antes = leer_asof(capa, fuente, llegada - timedelta(days=1), desde=fecha, hasta=fecha)
            assert set(antes["_revision"]) == {1}
            assert not antes[col].reset_index(drop=True).equals(real[col])
            # ...y desde el día en que llega, la corregida, igual a Kaggle.
            despues = leer_asof(capa, fuente, llegada, desde=fecha, hasta=fecha).drop(columns=COLUMNAS_CONTROL)
            pd.testing.assert_frame_equal(despues, real)


@pytest.mark.parametrize("asof", [CORTE + timedelta(days=i) for i in range(10)])
def test_asof_sin_informacion_futura(entorno, asof):
    correr_hasta(entorno, ULTIMO_DIA)
    for fuente in entorno["ing"]["archivos"]:
        df = leer_asof(entorno["val"]["dir_silver"], fuente, asof)
        if df.empty:
            continue
        assert (df["date"] <= asof.isoformat()).all()
        assert (df["_fecha_ingesta"] <= asof.isoformat()).all()


def test_flujo_completo_reproduce_kaggle(entorno, zip_kaggle_ejemplo):
    correr_hasta(entorno, ULTIMO_DIA)
    fechas = [(CORTE + timedelta(days=i)).isoformat() for i in range(3)]
    for fuente, archivo in entorno["ing"]["archivos"].items():
        silver = leer_asof(entorno["val"]["dir_silver"], fuente, ULTIMO_DIA).drop(columns=COLUMNAS_CONTROL)
        pd.testing.assert_frame_equal(silver, kaggle(zip_kaggle_ejemplo, archivo, fechas), check_dtype=False)


# --- Validación ------------------------------------------------------------------------

MALAS = {
    "venta negativa": (lambda d: d.assign(sales=d["sales"] - 1e6), "negativos en sales"),
    "clave duplicada": (lambda d: pd.concat([d, d.head(1)]), "claves duplicadas"),
    "fecha ajena": (lambda d: d.assign(date="2016-12-31"), "fechas distintas"),
    "tienda desconocida": (lambda d: d.assign(store_nbr=999), "tiendas desconocidas"),
    "columna faltante": (lambda d: d.drop(columns="onpromotion"), "columnas faltantes"),
    "nulos": (lambda d: d.assign(family=None), "nulos en family"),
    "sin filas": (lambda d: d.head(0), "sin filas"),
}


@pytest.mark.parametrize("caso", MALAS)
def test_revision_invalida_va_a_cuarentena_y_silver_conserva_la_ultima_valida(entorno, caso):
    correr_hasta(entorno, date(2017, 1, 4))  # ventas 2017-01-02 ya validada (rev 1)
    fecha, dia = date(2017, 1, 2), date(2017, 1, 5)
    bronze = entorno["ing"]["dir_bronze"]
    rev1 = pd.read_parquet(ruta_revision(bronze, "ventas", fecha, 1)).drop(columns=COLUMNAS_CONTROL)
    alterar, motivo = MALAS[caso]
    mala = alterar(rev1).assign(_revision=7, _fecha_ingesta=dia.isoformat(), _origen="prueba")
    escribir_parquet(mala, ruta_revision(bronze, "ventas", fecha, 7))

    validar(**entorno["val"], fecha_proceso=dia)

    dir_cuarentena = entorno["val"]["dir_cuarentena"]
    assert list(listar_revisiones(ruta_particion(dir_cuarentena, "ventas", fecha))) == [7]
    assert 7 not in listar_revisiones(ruta_particion(entorno["val"]["dir_silver"], "ventas", fecha))
    silver = leer_asof(entorno["val"]["dir_silver"], "ventas", dia, desde=fecha, hasta=fecha)
    assert set(silver["_revision"]) == {1}  # sigue la última revisión válida
    cuarentena = pd.read_parquet(ruta_revision(dir_cuarentena, "ventas", fecha, 7))
    if not cuarentena.empty:
        assert motivo in cuarentena["_motivos"].iloc[0]


def test_validacion_no_procesa_revisiones_futuras(entorno):
    (fuente, fecha), llegada = min(revisadas(entorno).items())
    correr_hasta(entorno, llegada)
    silver = ruta_particion(entorno["val"]["dir_silver"], fuente, fecha)
    ruta_revision(entorno["val"]["dir_silver"], fuente, fecha, 2).unlink()
    # Validar con un día anterior a la llegada del _rev2 no lo publica en silver...
    validar(**entorno["val"], fecha_proceso=llegada - timedelta(days=1))
    assert capas.revisiones(silver) == {1}
    # ...y el día en que llega, sí.
    validar(**entorno["val"], fecha_proceso=llegada)
    assert capas.revisiones(silver) == {1, 2}
