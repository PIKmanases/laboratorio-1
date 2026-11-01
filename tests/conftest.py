"""Fixtures compartidas.

Las pruebas usan datos pequeños de ejemplo en `tests/data/kaggle/`
(mismo esquema que el zip de Kaggle). El CI nunca usa `dvc pull`.
"""

import zipfile
from pathlib import Path

import pytest

DIR_EJEMPLO = Path(__file__).parent / "data" / "kaggle"
ARCHIVOS_KAGGLE = ["train.csv", "stores.csv", "holidays_events.csv", "oil.csv", "transactions.csv"]


@pytest.fixture
def zip_kaggle_ejemplo(tmp_path: Path) -> Path:
    """Arma un zip con la misma forma que el de Kaggle a partir de los CSV de ejemplo."""
    ruta = tmp_path / "store-sales-time-series-forecasting.zip"
    with zipfile.ZipFile(ruta, "w") as z:
        for nombre in ARCHIVOS_KAGGLE:
            z.write(DIR_EJEMPLO / nombre, arcname=nombre)
    return ruta
