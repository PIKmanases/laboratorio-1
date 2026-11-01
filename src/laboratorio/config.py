"""Lectura de la configuración versionada en config/."""

from pathlib import Path

import yaml

RAIZ = Path(__file__).resolve().parents[2]


def cargar_config(nombre: str = "pipeline.yaml") -> dict:
    return yaml.safe_load((RAIZ / "config" / nombre).read_text(encoding="utf-8"))
