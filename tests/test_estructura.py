import importlib

import pytest

MODULOS = ["inbox", "ingest", "validate", "features", "models", "forecast", "orders", "monitoring"]


@pytest.mark.parametrize("modulo", MODULOS)
def test_modulo_importable(modulo):
    importlib.import_module(f"laboratorio.{modulo}")
