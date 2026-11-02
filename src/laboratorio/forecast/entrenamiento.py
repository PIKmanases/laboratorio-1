"""Entrenamiento, backtest y reemplazo del modelo campeón.

    PYTHONPATH=src python -m laboratorio.forecast.entrenamiento [--preparar-capas]

1. Arma los predictores as-of de cada corte semanal y las ventas observadas.
2. Backtest temporal del campeón actual (al inicio, la línea base) y del candidato
   (LightGBM por cuantiles) sobre los mismos folds.
3. `reemplazo.decidir_reemplazo` decide. Si el candidato gana se entrena con todos
   los cortes elegibles y queda en `models/campeon/`; si no, el campeón sigue.
4. `models/campeon/` (modelo + meta.json + metricas.json) lo versiona DVC (`dvc.yaml`).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import shutil
import time
from datetime import date, timedelta
from pathlib import Path

from laboratorio.config import cargar_config
from laboratorio.forecast import dataset, evaluacion, modelos
from laboratorio.forecast.reemplazo import Condicion, Decision, decidir_reemplazo

log = logging.getLogger(__name__)


def preparar_capas(cfg: dict) -> None:
    """Corre día a día (idempotente) bandeja, ingesta y validación hasta el fin del periodo."""
    from laboratorio import tareas

    dia, fin = date.fromisoformat(cfg["bandeja"]["fecha_corte"]), dataset.fecha_entrenamiento(cfg)
    while dia <= fin:
        tareas.recibir_bandeja(dia, cfg)
        tareas.ingestar(dia, cfg)
        tareas.validar(dia, cfg)
        dia += timedelta(days=1)


def leer_campeon(directorio: Path, cfg: dict) -> dict:
    """Receta del campeón guardado; si no hay ninguno, la línea base."""
    meta = directorio / "meta.json"
    if meta.exists():
        return json.loads(meta.read_text(encoding="utf-8"))["receta"]
    return evaluacion.receta_baseline(cfg)


def guardar_campeon(directorio: Path, receta: dict, meta: dict, modelo=None, metricas: dict | None = None) -> None:
    """Escribe el campeón completo en una carpeta temporal y la intercambia (nunca queda a medias)."""
    tmp, viejo = directorio.with_name(directorio.name + ".nuevo"), directorio.with_name(directorio.name + ".viejo")
    shutil.rmtree(tmp, ignore_errors=True)
    tmp.mkdir(parents=True)
    if modelo is not None:
        modelo.guardar(tmp)
    (tmp / "meta.json").write_text(json.dumps({**meta, "receta": receta}, indent=2, ensure_ascii=False), encoding="utf-8")
    (tmp / "metricas.json").write_text(json.dumps(metricas or {}, indent=2, ensure_ascii=False), encoding="utf-8")
    shutil.rmtree(viejo, ignore_errors=True)
    if directorio.exists():
        os.replace(directorio, viejo)
    os.replace(tmp, directorio)
    shutil.rmtree(viejo, ignore_errors=True)


def _version(receta: dict, datos_hasta: date) -> str:
    return hashlib.sha1(json.dumps([receta, datos_hasta.isoformat()], sort_keys=True).encode()).hexdigest()[:10]


def entrenar(cfg: dict | None = None, directorio: Path | None = None) -> dict:
    cfg = cfg or cargar_config()
    costos = cargar_config("costs.yaml")
    directorio = directorio or modelos.dir_campeon(cfg)
    inicio = time.time()

    cortes = dataset.todos_los_cortes(cfg)
    predictores = dataset.construir_predictores(cfg, cortes)
    etiquetas = dataset.leer_etiquetas(cfg, cortes)
    log.info("dataset: %d filas, %d cortes (%.0f s)", len(predictores), len(cortes), time.time() - inicio)

    candidata = evaluacion.receta_lightgbm(cfg)
    campeona = leer_campeon(directorio, cfg)
    resultado_c = evaluacion.backtest(campeona, cfg, predictores, etiquetas, costos)
    mismo_modelo = campeona == candidata and (directorio / "cuantiles.json").exists()
    resultado_n = resultado_c if campeona == candidata else evaluacion.backtest(candidata, cfg, predictores, etiquetas, costos)
    log.info("backtest listo (%.0f s)", time.time() - inicio)

    if mismo_modelo:  # misma receta: no hay nada que comparar, se reentrena con datos más recientes
        decision = Decision(True, [Condicion("reentrenamiento", True, "misma receta del campeón, datos más recientes")])
    else:
        decision = decidir_reemplazo(resultado_c, resultado_n, cfg["modelado"]["reemplazo"])
    informe = {"campeon_anterior": resultado_c, "candidato": resultado_n, "decision": decision.a_dict()}

    if decision.reemplazar:
        hasta = dataset.fecha_entrenamiento(cfg)
        cortes_final = dataset.cortes_de_entrenamiento(cfg, None, candidata["semanas_entrenamiento"])
        filas = dataset.con_etiquetas(predictores[predictores["corte"].isin(cortes_final)], etiquetas)
        modelo = evaluacion.entrenar_receta(candidata, filas)
        meta = {
            "version": _version(candidata, hasta),
            "entrenado_con_datos_hasta": hasta.isoformat(),
            "cortes_entrenamiento": [cortes_final[0].isoformat(), cortes_final[-1].isoformat()],
            "filas_entrenamiento": len(filas),
        }
        guardar_campeon(directorio, candidata, meta, modelo, informe)
        log.info("campeón reemplazado: %s", meta["version"])
    elif not (directorio / "meta.json").exists():  # primer campeón: la línea base
        meta = {"version": _version(campeona, dataset.fecha_entrenamiento(cfg)), "entrenado_con_datos_hasta": None}
        guardar_campeon(directorio, campeona, meta, None, informe)
    else:  # el campeón sigue: solo se actualiza el informe
        (directorio / "metricas.json").write_text(json.dumps(informe, indent=2, ensure_ascii=False), encoding="utf-8")
    informe["segundos"] = round(time.time() - inicio)
    return informe


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--preparar-capas", action="store_true", help="corre bandeja, ingesta y validación antes de entrenar")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    cfg = cargar_config()
    if args.preparar_capas:
        preparar_capas(cfg)
    informe = entrenar(cfg)
    for nombre in ("campeon_anterior", "candidato"):
        r = informe[nombre]
        print(f"{nombre:16} {r['receta']['tipo']:20} WAPE {r['wape']:.4f}  sesgo {r['sesgo']:+.4f}  fill {r['fill_rate']:.4f}  costo {r['costo']:,.0f}")
    print(informe["decision"]["motivo"], f"({informe['segundos']} s)")


if __name__ == "__main__":
    main()
