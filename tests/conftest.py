"""Fixtures compartidas.

Las pruebas usan datos pequeños de ejemplo en `tests/data/kaggle/`
(mismo esquema que el zip de Kaggle). El CI nunca usa `dvc pull`.
"""

import zipfile
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path

import pytest

DIR_EJEMPLO = Path(__file__).parent / "data" / "kaggle"
ARCHIVOS_KAGGLE = ["train.csv", "stores.csv", "holidays_events.csv", "oil.csv", "transactions.csv"]


def crear_zip_ejemplo(directorio: Path) -> Path:
    ruta = directorio / "store-sales-time-series-forecasting.zip"
    with zipfile.ZipFile(ruta, "w") as z:
        for nombre in ARCHIVOS_KAGGLE:
            z.write(DIR_EJEMPLO / nombre, arcname=nombre)
    return ruta


@pytest.fixture
def zip_kaggle_ejemplo(tmp_path: Path) -> Path:
    """Arma un zip con la misma forma que el de Kaggle a partir de los CSV de ejemplo."""
    return crear_zip_ejemplo(tmp_path)


# --- Flujo diario con datos de ejemplo ------------------------------------------------

CORTE = date(2017, 1, 1)  # datos de ejemplo: 2017-01-01..03
FIN = date(2017, 1, 3)
ULTIMO_DIA = date(2017, 1, 10)  # para entonces ya llegó todo, incluidos los _rev2


@pytest.fixture
def entorno(zip_kaggle_ejemplo, tmp_path):
    """Config real del proyecto apuntando a los datos de ejemplo y a carpetas temporales."""
    from laboratorio.config import cargar_config
    from laboratorio.inbox.simulador import desde_config as config_simulador
    from laboratorio.ingest.ingesta import desde_config as config_ingesta
    from laboratorio.validate.validacion import desde_config as config_validacion

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
    from laboratorio.inbox.simulador import generar_bandeja
    from laboratorio.ingest.ingesta import ingestar
    from laboratorio.validate.validacion import validar

    generar_bandeja(**e["sim"], hasta=dia)
    ingestar(**e["ing"], fecha_proceso=dia)
    validar(**e["val"], fecha_proceso=dia)


def correr_hasta(e, ultimo):
    dia = CORTE
    while dia <= ultimo:
        correr_dia(e, dia)
        dia += timedelta(days=1)


def entregas_planeadas(e):
    """Todas las entregas que hará el simulador (sin tocar la bandeja del entorno)."""
    from laboratorio.inbox.simulador import generar_bandeja

    return generar_bandeja(**{**e["sim"], "dir_bandeja": e["tmp"] / "plan"})


def revisadas(e):
    """(fuente, fecha) que reciben _rev2 -> día en que llega."""
    return {(x.fuente, x.fecha_dato): x.fecha_llegada for x in entregas_planeadas(e) if x.revision == 2}


def crear_cfg_prueba(directorio: Path, zip_ejemplo: Path) -> dict:
    """config/pipeline.yaml real con rutas absolutas en `directorio` y fechas de los datos de ejemplo."""
    from laboratorio.config import cargar_config

    cfg = cargar_config()
    cfg["datos"]["zip_kaggle"] = str(zip_ejemplo)
    cfg["bandeja"].update(dir=str(directorio / "inbox"), fecha_corte=CORTE.isoformat(), fecha_fin=FIN.isoformat())
    cfg["bandeja"]["revisiones"]["fraccion_fechas"] = 1.0  # todas las fechas reciben _rev2
    cfg["capas"] = {nombre: str(directorio / nombre) for nombre in cfg["capas"]}
    cfg["features"]["historia_cortes_dias"] = 3
    cfg["orquestacion"]["inicio"] = CORTE.isoformat()
    return cfg


@pytest.fixture
def cfg_prueba(zip_kaggle_ejemplo, tmp_path):
    return crear_cfg_prueba(tmp_path, zip_kaggle_ejemplo)
