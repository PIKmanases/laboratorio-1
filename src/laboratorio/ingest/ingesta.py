"""Ingesta idempotente a la capa bronze.

- **Carga histórica:** los datos del zip con fecha <= fecha de corte entran como
  revisión 1, con `available_at = fecha_corte`.
- **Bandeja:** cada archivo `<fuente>_<fecha>[_revN].csv` entra tal cual a
  `bronze/<fuente>/fecha=<fecha>/rev=<N>.parquet` con
  `available_at = fecha_proceso` (el día en que se conoció). Cada fila
  lleva su `revision` y su `available_at`.

Idempotencia: una revisión que ya está en bronze no se vuelve a escribir
(conserva su `available_at` original); cada escritura es atómica. La bandeja
solo se lee.
"""

from __future__ import annotations

import zipfile
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import pandas as pd

from laboratorio.capas import (
    escribir_json,
    escribir_parquet,
    revisiones,
    ruta_particion,
    ruta_revision,
)
from laboratorio.ingest.bandeja import listar_archivos

DIR_REFERENCIA = "referencia"
MARCA_HISTORICO = "_carga_historica.json"


@dataclass
class Resumen:
    revisiones_nuevas: dict[str, int] = field(default_factory=dict)

    def sumar(self, fuente: str) -> None:
        self.revisiones_nuevas[fuente] = self.revisiones_nuevas.get(fuente, 0) + 1


def _escribir_revision(
    dir_bronze: Path, fuente: str, fecha: date, rev: int, df: pd.DataFrame, origen: str, fecha_ingesta: date
) -> None:
    df = df.reset_index(drop=True).assign(
        revision=rev, available_at=pd.Timestamp(fecha_ingesta), origen=origen
    )
    escribir_parquet(df, ruta_revision(dir_bronze, fuente, fecha, rev))


def cargar_historico(
    ruta_zip: Path, dir_bronze: Path, archivos: dict[str, str], referencias: list[str], fecha_corte: date
) -> Resumen:
    resumen = Resumen()
    with zipfile.ZipFile(ruta_zip) as z:
        for nombre in referencias:
            destino = Path(dir_bronze) / DIR_REFERENCIA / f"{Path(nombre).stem}.parquet"
            if not destino.exists():
                escribir_parquet(pd.read_csv(z.open(nombre)), destino)

        for fuente, archivo in archivos.items():
            marca = Path(dir_bronze) / fuente / MARCA_HISTORICO
            if marca.exists():
                continue
            df = pd.read_csv(z.open(archivo))
            df = df[df["date"] <= fecha_corte.isoformat()]
            for fecha_txt, dia in df.groupby("date", sort=True):
                fecha = date.fromisoformat(fecha_txt)
                if 1 not in revisiones(ruta_particion(dir_bronze, fuente, fecha)):
                    _escribir_revision(dir_bronze, fuente, fecha, 1, dia, f"zip:{archivo}", fecha_corte)
                    resumen.sumar(fuente)
            # La marca se escribe al final: si el proceso se corta, la próxima corrida retoma.
            escribir_json({"fecha_corte": fecha_corte.isoformat(), "archivo": archivo}, marca)
    return resumen


def ingestar_bandeja(dir_bandeja: Path, dir_bronze: Path, fuentes: list[str], fecha_proceso: date) -> Resumen:
    resumen = Resumen()
    for fuente in fuentes:
        for a in listar_archivos(dir_bandeja, fuente).itertuples():
            if a.revision not in revisiones(ruta_particion(dir_bronze, fuente, a.fecha)):
                df = pd.read_csv(a.ruta)
                _escribir_revision(dir_bronze, fuente, a.fecha, a.revision, df, a.ruta.name, fecha_proceso)
                resumen.sumar(fuente)
    return resumen


def ingestar(
    ruta_zip: Path,
    dir_bandeja: Path,
    dir_bronze: Path,
    archivos: dict[str, str],
    referencias: list[str],
    fecha_corte: date,
    fecha_proceso: date,
) -> tuple[Resumen, Resumen]:
    """Corrida diaria de ingesta para el día `fecha_proceso`."""
    if fecha_proceso < fecha_corte:
        raise ValueError(f"fecha_proceso {fecha_proceso} es anterior a la fecha de corte {fecha_corte}")
    historico = cargar_historico(ruta_zip, dir_bronze, archivos, referencias, fecha_corte)
    bandeja = ingestar_bandeja(dir_bandeja, dir_bronze, list(archivos), fecha_proceso)
    return historico, bandeja


def desde_config(cfg: dict, raiz: Path) -> dict:
    """Argumentos de `ingestar` (sin `fecha_proceso`) a partir de config/pipeline.yaml."""
    b = cfg["bandeja"]
    return {
        "ruta_zip": raiz / cfg["datos"]["zip_kaggle"],
        "dir_bandeja": raiz / b["dir"],
        "dir_bronze": raiz / cfg["capas"]["bronze"],
        "archivos": {n: f["archivo"] for n, f in b["fuentes"].items()},
        "referencias": cfg["datos"]["referencias"],
        "fecha_corte": date.fromisoformat(b["fecha_corte"]),
    }
