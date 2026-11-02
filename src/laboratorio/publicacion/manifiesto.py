"""Manifiesto de trazabilidad de un pronóstico.

Junto a cada revisión del pronóstico (`forecasts/pronosticos/fecha=<D>/rev=<N>.manifiesto.json`)
se guarda qué lo produjo, para poder regenerarlo y comprobar que sale idéntico:

- `git`: SHA del commit del código (y si había cambios sin commitear).
- `datos`: hash (md5, el de DVC) del dato crudo de Kaggle.
- `modelo`: nombre, versión, datos con los que se entrenó, hash DVC (`dvc.lock`) y hash del contenido de `models/campeon`.
- `fecha_corte`: el día as-of del pronóstico.
- `configuracion`: hash de `config/pipeline.yaml` y del archivo de costos (YAML ya leído,
  así los comentarios y el formato no cuentan).
- `pronostico` y `pedido`: revisión, filas y hash del contenido, además de la firma de
  los insumos que usó (qué revisión de cada día de ventas leyó).

No lleva rutas absolutas ni horas de reloj: mismo código, datos y configuración => mismo
manifiesto. Una vez escrito no cambia mientras el pronóstico y el pedido sean los mismos
(se conserva el SHA original); si el pedido se recalcula con otros insumos, se reescribe.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
from datetime import date
from pathlib import Path

import pandas as pd
import yaml

from laboratorio.capas import COLUMNAS_CONTROL, escribir_json, leer_linaje, ruta_revision
from laboratorio.config import RAIZ, cargar_config

ESQUEMA = 1
ENV_SHA = "LABORATORIO_GIT_SHA"  # para contenedores sin .git
ENV_SUCIO = "LABORATORIO_GIT_SUCIO"  # "1" si el árbol tenía cambios sin commitear, "0" si estaba limpio
LOCK_DVC = "dvc.lock"


def _sha256(datos: bytes) -> str:
    return hashlib.sha256(datos).hexdigest()


def _hash_json(obj) -> str:
    return _sha256(json.dumps(obj, sort_keys=True, default=str, ensure_ascii=False).encode())[:16]


def md5_archivo(ruta: Path) -> str:
    h = hashlib.md5()
    with open(ruta, "rb") as f:
        for bloque in iter(lambda: f.read(1 << 20), b""):
            h.update(bloque)
    return h.hexdigest()


def hash_directorio(directorio: Path) -> str | None:
    """Hash del contenido de una carpeta (nombres relativos + bytes), independiente de la ruta."""
    if not directorio.is_dir():
        return None
    partes = [f"{p.relative_to(directorio).as_posix()}:{_sha256(p.read_bytes())}" for p in sorted(directorio.rglob("*")) if p.is_file()]
    return _sha256("\n".join(partes).encode())[:16]


def hash_contenido(df: pd.DataFrame) -> str:
    """Hash del contenido de un resultado, sin columnas de control y sin depender del orden de filas."""
    datos = df[[c for c in df.columns if c not in COLUMNAS_CONTROL]]
    datos = datos.sort_values(list(datos.columns), ignore_index=True)
    return _sha256(pd.util.hash_pandas_object(datos, index=False).to_numpy().tobytes())[:16] + f":{len(datos)}x{datos.shape[1]}"


def estado_git(raiz: Path = RAIZ) -> dict:
    """SHA del commit y si hay cambios sin commitear (en un contenedor, de las variables de entorno; None si se desconoce)."""
    if os.environ.get(ENV_SHA):
        sucio = os.environ.get(ENV_SUCIO, "")
        return {"sha": os.environ[ENV_SHA], "sucio": sucio == "1" if sucio in ("0", "1") else None}
    try:
        sha = subprocess.run(["git", "-C", str(raiz), "rev-parse", "HEAD"], capture_output=True, text=True, check=True).stdout.strip()
        cambios = subprocess.run(["git", "-C", str(raiz), "status", "--porcelain"], capture_output=True, text=True, check=True).stdout
    except (OSError, subprocess.CalledProcessError):
        return {"sha": None, "sucio": None}
    return {"sha": sha, "sucio": bool(cambios.strip())}


def dvc_md5(raiz: Path, ruta_salida: str) -> str | None:
    """Hash DVC de una salida (`outs`) registrada en `dvc.lock`."""
    lock = raiz / LOCK_DVC
    if not lock.exists():
        return None
    for etapa in (yaml.safe_load(lock.read_text(encoding="utf-8")) or {}).get("stages", {}).values():
        for out in etapa.get("outs", []):
            if out.get("path") == ruta_salida:
                return out.get("md5")
    return None


def datos_crudos(cfg: dict, raiz: Path = RAIZ) -> dict:
    zip_ = raiz / cfg["datos"]["zip_kaggle"]
    puntero = zip_.with_name(zip_.name + ".dvc")
    declarado = None
    if puntero.exists():
        declarado = yaml.safe_load(puntero.read_text(encoding="utf-8"))["outs"][0]["md5"]
    return {"archivo": zip_.name, "md5": md5_archivo(zip_) if zip_.exists() else None, "md5_dvc": declarado}


def modelo(cfg: dict, raiz: Path = RAIZ) -> dict:
    from laboratorio.forecast.modelos import LIGHTGBM, dir_campeon, modelo_vigente

    nombre = modelo_vigente(cfg)
    if nombre != LIGHTGBM:
        return {"nombre": nombre, "version": f"baseline:{nombre}", "entrenado_con_datos_hasta": None, "md5_dvc": None, "contenido": None}
    carpeta = dir_campeon(cfg)
    meta = json.loads((carpeta / "meta.json").read_text(encoding="utf-8"))
    ruta_dvc = f"{cfg['capas']['modelos']}/campeon"
    return {
        "nombre": nombre,
        "version": meta["version"],
        "entrenado_con_datos_hasta": meta.get("entrenado_con_datos_hasta"),
        "md5_dvc": dvc_md5(raiz, ruta_dvc),
        "contenido": hash_directorio(carpeta),
    }


def configuracion(cfg: dict) -> dict:
    pipeline, costos = cargar_config("pipeline.yaml"), cargar_config(cfg["pedido"]["costos"])
    return {"pipeline": _hash_json(pipeline), "costos": _hash_json(costos), "conjunto": _hash_json([pipeline, costos])}


def _salida(df: pd.DataFrame) -> dict:
    return {"revision": int(df["revision"].iloc[0]), "filas": len(df), "hash_contenido": hash_contenido(df)}


def construir(cfg: dict, corte: date, pron: pd.DataFrame, ped: pd.DataFrame, dir_pron: Path, raiz: Path = RAIZ) -> dict:
    lin_pron = leer_linaje(ruta_revision(dir_pron, "pronosticos", corte, int(pron["revision"].iloc[0])))
    lin_ped = leer_linaje(ruta_revision(dir_pron, "pedidos", corte, int(ped["revision"].iloc[0])))
    return {
        "esquema": ESQUEMA,
        "fecha_corte": corte.isoformat(),
        "git": estado_git(raiz),
        "datos": datos_crudos(cfg, raiz),
        "modelo": modelo(cfg, raiz),
        "configuracion": configuracion(cfg),
        "pronostico": {**_salida(pron), "firma_insumos": lin_pron.get("firma")},
        "pedido": {**_salida(ped), "firma_insumos": lin_ped.get("firma")},
    }


def ruta_manifiesto(dir_pron: Path, corte: date, revision: int) -> Path:
    return ruta_revision(dir_pron, "pronosticos", corte, revision).with_name(f"rev={revision}.manifiesto.json")


def _produccion(m: dict) -> tuple:
    """Lo que el pronóstico y el pedido produjeron y con qué insumos (sin SHA de código)."""
    return (m["pronostico"], m["pedido"], m["configuracion"]["costos"], m["modelo"]["version"])


def publicar(cfg: dict, corte: date, pron: pd.DataFrame, ped: pd.DataFrame, dir_pron: Path) -> tuple[Path, bool]:
    """Escribe el manifiesto del pronóstico vigente. Devuelve (ruta, si se escribió).

    Idempotente: si ya existe uno que describe el mismo pronóstico y pedido, no se toca
    (así conserva el SHA con el que se generó, aunque después el repo avance).
    """
    nuevo = construir(cfg, corte, pron, ped, dir_pron)
    ruta = ruta_manifiesto(dir_pron, corte, nuevo["pronostico"]["revision"])
    if ruta.exists() and _produccion(json.loads(ruta.read_text(encoding="utf-8"))) == _produccion(nuevo):
        return ruta, False
    escribir_json(nuevo, ruta)
    return ruta, True


def leer(dir_pron: Path, corte: date, revision: int | None = None) -> dict:
    """Manifiesto de la última revisión (o de `revision`) del pronóstico de `corte`."""
    from laboratorio.capas import listar_revisiones, ruta_particion

    revs = listar_revisiones(ruta_particion(dir_pron, "pronosticos", corte))
    if not revs:
        raise FileNotFoundError(f"no hay pronóstico del corte {corte}")
    ruta = ruta_manifiesto(dir_pron, corte, revision or max(revs))
    return json.loads(ruta.read_text(encoding="utf-8"))


def verificar_entorno(esperado: dict, cfg: dict, raiz: Path = RAIZ) -> list[str]:
    """Diferencias entre lo que dice el manifiesto y el repositorio actual (vacío = mismo estado)."""
    dif = []
    git = estado_git(raiz)
    if esperado["git"]["sha"] and git["sha"] != esperado["git"]["sha"]:
        dif.append(f"git: {git['sha']} != {esperado['git']['sha']}")
    if git["sucio"]:
        dif.append("git: hay cambios sin commitear")
    if datos_crudos(cfg, raiz)["md5"] != esperado["datos"]["md5"]:
        dif.append("datos: el md5 del dato crudo cambió (corre dvc checkout)")
    actual = modelo(cfg, raiz)
    for k in ("version", "md5_dvc", "contenido"):
        if actual[k] != esperado["modelo"][k]:
            dif.append(f"modelo.{k}: {actual[k]} != {esperado['modelo'][k]}")
    conf = configuracion(cfg)
    for k, v in esperado["configuracion"].items():
        if conf[k] != v:
            dif.append(f"configuracion.{k}: {conf[k]} != {v}")
    return dif
