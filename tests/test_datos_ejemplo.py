import csv
import io
import zipfile

from conftest import ARCHIVOS_KAGGLE

# Encabezados del zip real de Kaggle.
ESQUEMAS = {
    "train.csv": ["id", "date", "store_nbr", "family", "sales", "onpromotion"],
    "stores.csv": ["store_nbr", "city", "state", "type", "cluster"],
    "holidays_events.csv": ["date", "type", "locale", "locale_name", "description", "transferred"],
    "oil.csv": ["date", "dcoilwtico"],
    "transactions.csv": ["date", "store_nbr", "transactions"],
}


def test_zip_ejemplo_tiene_los_archivos_esperados(zip_kaggle_ejemplo):
    with zipfile.ZipFile(zip_kaggle_ejemplo) as z:
        assert set(ARCHIVOS_KAGGLE) <= set(z.namelist())


def test_zip_ejemplo_respeta_esquema_de_kaggle(zip_kaggle_ejemplo):
    with zipfile.ZipFile(zip_kaggle_ejemplo) as z:
        for nombre, columnas in ESQUEMAS.items():
            with z.open(nombre) as f:
                filas = list(csv.reader(io.TextIOWrapper(f, encoding="utf-8")))
            assert filas[0] == columnas, nombre
            assert len(filas) > 1, f"{nombre} no tiene filas"
