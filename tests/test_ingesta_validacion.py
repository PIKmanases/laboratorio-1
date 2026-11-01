"""Ingesta, validación y capas as-of (datos de ejemplo de tests/data, sin dvc pull)."""

import hashlib
import json
import os
import stat
import zipfile
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest
from conftest import CORTE, ULTIMO_DIA, correr_hasta, revisadas

from laboratorio import capas
from laboratorio.capas import (
    COLUMNAS_CONTROL,
    escribir_parquet,
    listar_particiones,
    listar_revisiones,
    load_as_of,
    ruta_particion,
    ruta_revision,
)
from laboratorio.inbox.simulador import generar_bandeja
from laboratorio.ingest.ingesta import ingestar
from laboratorio.validate.validacion import ruta_motivos, validar


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
            assert set(df["revision"]) == {1}
            assert set(df["available_at"]) == {pd.Timestamp(CORTE)}
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
    for rev, ruta in revs.items():  # revision y available_at en cada fila
        df = pd.read_parquet(ruta)
        assert df[["revision", "available_at"]].notna().all().all()
        assert set(df["revision"]) == {rev}
    assert set(pd.read_parquet(revs[2])["available_at"]) == {pd.Timestamp(llegada)}
    assert (pd.read_parquet(revs[1])["available_at"] < pd.Timestamp(llegada)).all()


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


# --- Flujo completo y validación integrada -------------------------------------------


def test_flujo_completo_reproduce_kaggle(entorno, zip_kaggle_ejemplo):
    correr_hasta(entorno, ULTIMO_DIA)
    fechas = [(CORTE + timedelta(days=i)).isoformat() for i in range(3)]
    for fuente, archivo in entorno["ing"]["archivos"].items():
        silver = load_as_of(fuente, ULTIMO_DIA, dir_capa=entorno["val"]["dir_silver"])
        pd.testing.assert_frame_equal(
            silver.drop(columns=COLUMNAS_CONTROL), kaggle(zip_kaggle_ejemplo, archivo, fechas), check_dtype=False
        )


MALAS = {
    "venta negativa": (lambda d: d.assign(sales=d["sales"] - 1e6), "sales: menor que 0"),
    "clave duplicada": (lambda d: pd.concat([d, d.head(1)]), "claves duplicadas"),
    "fecha ajena": (lambda d: d.assign(date="2016-12-31"), "fecha distinta a la esperada"),
    "tienda desconocida": (lambda d: d.assign(store_nbr=range(900, 900 + len(d))), "tienda desconocida"),
    "columna faltante": (lambda d: d.drop(columns="onpromotion"), "columna faltante: onpromotion"),
    "nulos": (lambda d: d.assign(family=None), "family: nulos no permitidos"),
    "sin filas": (lambda d: d.head(0), "sin filas"),
}


@pytest.mark.parametrize("caso", MALAS)
def test_revision_invalida_va_a_cuarentena_y_silver_conserva_la_ultima_valida(entorno, caso):
    correr_hasta(entorno, date(2017, 1, 4))  # ventas 2017-01-02 ya validada (rev 1)
    fecha, dia = date(2017, 1, 2), date(2017, 1, 5)
    bronze = entorno["ing"]["dir_bronze"]
    rev1 = pd.read_parquet(ruta_revision(bronze, "ventas", fecha, 1)).drop(columns=COLUMNAS_CONTROL)
    alterar, motivo = MALAS[caso]
    mala = alterar(rev1).assign(revision=7, available_at=pd.Timestamp(dia), origen="prueba")
    escribir_parquet(mala, ruta_revision(bronze, "ventas", fecha, 7))

    validar(**entorno["val"], fecha_proceso=dia)

    dir_cuarentena, dir_silver = entorno["val"]["dir_cuarentena"], entorno["val"]["dir_silver"]
    assert list(listar_revisiones(ruta_particion(dir_cuarentena, "ventas", fecha))) == [7]
    assert 7 not in listar_revisiones(ruta_particion(dir_silver, "ventas", fecha))
    motivos = json.loads(ruta_motivos(ruta_revision(dir_cuarentena, "ventas", fecha, 7)).read_text())["motivos"]
    assert any(motivo in m for m in motivos), motivos
    silver = load_as_of("ventas", dia, desde=fecha, hasta=fecha, dir_capa=dir_silver)
    assert set(silver["revision"]) == {1}  # sigue la última revisión válida


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


def test_silver_tiene_los_tipos_del_esquema(entorno):
    correr_hasta(entorno, ULTIMO_DIA)
    tipos = {"int64": "int64", "float64": "float64", "str": "object"}
    for fuente, reglas in entorno["val"]["reglas"].items():
        df = load_as_of(fuente, ULTIMO_DIA, dir_capa=entorno["val"]["dir_silver"])
        for col, c in reglas.columnas.items():
            assert str(df[col].dtype) == tipos[c.tipo], (fuente, col)
        assert str(df["available_at"].dtype) == "datetime64[ns]"
        assert str(df["revision"].dtype) == "int64"
