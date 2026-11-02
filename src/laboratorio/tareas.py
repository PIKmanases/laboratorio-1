"""Tareas del flujo: el único punto de entrada de los orquestadores (`dags/`).

Cada tarea recibe la fecha de proceso (`AAAA-MM-DD`, el `ds` de Airflow), lee
`config/` y llama a la lógica de `src/`. Todas son idempotentes: repetirlas no
cambia nada y, si cambió algún insumo (por ejemplo, llegó un `_rev2`), reprocesan
solo las particiones afectadas. Devuelven un resumen serializable (queda en XCom).

Uso sin Airflow: `python -m laboratorio.tareas <tarea> AAAA-MM-DD`.

Los imports pesados (pandas, pandera, LightGBM...) están dentro de cada tarea: Airflow importa este
módulo al leer cada DAG y en cada despertar de un sensor, y debe ser rápido.
"""

from __future__ import annotations

import logging
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

from laboratorio.config import RAIZ, cargar_config

if TYPE_CHECKING:
    import pandas as pd

log = logging.getLogger(__name__)


def _cfg(cfg: dict | None) -> dict:
    return cfg if cfg is not None else cargar_config()


def _fecha(fecha: str | date) -> date:
    return fecha if isinstance(fecha, date) else date.fromisoformat(str(fecha)[:10])


def _capa(cfg: dict, nombre: str) -> Path:
    return RAIZ / cfg["capas"][nombre]


def _sin_control(df: pd.DataFrame) -> pd.DataFrame:
    from laboratorio.capas import COLUMNAS_CONTROL
    return df.drop(columns=[c for c in [*COLUMNAS_CONTROL, "date"] if c in df.columns])


# --- Diario: bandeja, ingesta y validación ----------------------------------------------


def recibir_bandeja(fecha: str | date, cfg: dict | None = None) -> dict:
    """Simulación: deja en la bandeja lo que llegó hasta `fecha`. En producción no hace nada."""
    from laboratorio.inbox import simulador
    cfg = _cfg(cfg)
    if not cfg["simulacion"]["activa"]:
        return {"simulacion": False}
    kw = simulador.desde_config(cfg)
    kw.update(ruta_zip=RAIZ / kw["ruta_zip"], dir_bandeja=RAIZ / kw["dir_bandeja"])
    entregas = simulador.generar_bandeja(**kw, hasta=_fecha(fecha))
    return {"archivos_en_bandeja": len(entregas)}


def ingestar(fecha: str | date, cfg: dict | None = None) -> dict:
    from laboratorio.ingest import ingesta
    historico, bandeja = ingesta.ingestar(**ingesta.desde_config(_cfg(cfg), RAIZ), fecha_proceso=_fecha(fecha))
    return {"historico": historico.revisiones_nuevas, "bandeja": bandeja.revisiones_nuevas}


def validar(fecha: str | date, cfg: dict | None = None) -> dict:
    from laboratorio.validate import validacion
    r = validacion.validar(**validacion.desde_config(_cfg(cfg), RAIZ), fecha_proceso=_fecha(fecha))
    return {"validas": r.validas, "cuarentena": r.rechazadas}


# --- Features ---------------------------------------------------------------------------


def _preparar_corte(cfg: dict, corte: date) -> int | None:
    from laboratorio.capas import escribir_derivado, insumos_de
    from laboratorio.features.construccion import predictores_del_corte
    f, p = cfg["features"], cfg["pronostico"]
    df, entradas = predictores_del_corte(cfg, corte)
    if df.empty:
        return None
    insumos = {
        **insumos_de(entradas["ventas"], "ventas"),
        **insumos_de(entradas["transacciones"], "transacciones"),
        **insumos_de(entradas["petroleo"], "petroleo"),
        "parametros": {**f, "horizonte_dias": p["horizonte_dias"], "anticipacion_dias": p["anticipacion_dias"]},
    }
    return escribir_derivado(_capa(cfg, "features"), "predictores", corte, df, insumos, available_at=corte)


def preparar_features(fecha: str | date, cfg: dict | None = None, historia: bool = True) -> dict:
    """Predictores as-of del corte `fecha` y, si faltan, de los cortes históricos para entrenar.

    Con `historia=False` solo se prepara el corte del día (lo que necesita un pronóstico).
    """
    from laboratorio.capas import listar_particiones
    cfg, dia = _cfg(cfg), _fecha(fecha)
    existentes = listar_particiones(_capa(cfg, "features"), "predictores")
    escritas = {}
    for i in range(cfg["features"]["historia_cortes_dias"] if historia else 0, -1, -1):
        corte = dia - timedelta(days=i)
        if corte != dia and corte in existentes:
            continue  # un corte pasado no cambia (as-of); solo se completa si falta
        rev = _preparar_corte(cfg, corte)
        if rev is not None:
            escritas[corte.isoformat()] = rev
    return {"cortes_escritos": len(escritas), "corte_del_dia": escritas.get(dia.isoformat())}


# --- Semanal: pronóstico y pedido -------------------------------------------------------


def pronosticar(fecha: str | date, cfg: dict | None = None) -> dict:
    from laboratorio.capas import escribir_derivado, insumos_de, leer_asof
    from laboratorio.forecast import entrenamiento, modelos
    from laboratorio.forecast.modelos import pronosticar as aplicar_modelo
    cfg, dia = _cfg(cfg), _fecha(fecha)
    pred = leer_asof(_capa(cfg, "features"), "predictores", dia, desde=dia, hasta=dia)
    if pred.empty:
        raise RuntimeError(f"no hay predictores del corte {dia}: corre primero preparar_features")
    if cfg["pronostico"]["modelo"] == modelos.CAMPEON and not modelos.hay_campeon(cfg):
        # Primera corrida desde cero: se entrena el campeón inicial con la carga histórica (as-of de
        # `modelado.fecha_entrenamiento`, independiente del día de este pronóstico).
        log.warning("no hay modelo campeón: se entrena el inicial (tarda unos minutos)")
        entrenamiento.entrenar(cfg)
    modelo = modelos.modelo_vigente(cfg)
    df = aplicar_modelo(_sin_control(pred), modelo)
    insumos = {**insumos_de(pred, "predictores"), "modelo": modelo}
    rev = escribir_derivado(_capa(cfg, "pronosticos"), "pronosticos", dia, df, insumos, available_at=dia)
    return {"revision_escrita": rev, "filas": len(df), "modelo": modelo}


def generar_pedido(fecha: str | date, cfg: dict | None = None) -> dict:
    from laboratorio.capas import escribir_derivado, insumos_de, leer_asof
    from laboratorio.orders.pedido import calcular_pedido
    cfg, dia = _cfg(cfg), _fecha(fecha)
    pron = leer_asof(_capa(cfg, "pronosticos"), "pronosticos", dia, desde=dia, hasta=dia)
    pred = leer_asof(_capa(cfg, "features"), "predictores", dia, desde=dia, hasta=dia)
    if pron.empty:
        raise RuntimeError(f"no hay pronóstico del corte {dia}: corre primero pronosticar")
    costos = cargar_config(cfg["pedido"]["costos"])
    df = calcular_pedido(_sin_control(pron), _sin_control(pred), costos)
    insumos = {**insumos_de(pron, "pronosticos"), **insumos_de(pred, "predictores"), "costos": costos}
    rev = escribir_derivado(_capa(cfg, "pronosticos"), "pedidos", dia, df, insumos, available_at=dia)
    return {"revision_escrita": rev, "items": len(df), "cantidad_total": round(float(df["cantidad_pedido"].sum()), 2)}


def publicar(fecha: str | date, cfg: dict | None = None) -> dict:
    """Manifiesto de trazabilidad del pronóstico y el pedido del corte `fecha` (ver `publicacion.manifiesto`)."""
    from laboratorio.capas import leer_asof
    from laboratorio.publicacion import manifiesto
    cfg, dia = _cfg(cfg), _fecha(fecha)
    pron = leer_asof(_capa(cfg, "pronosticos"), "pronosticos", dia, desde=dia, hasta=dia)
    ped = leer_asof(_capa(cfg, "pronosticos"), "pedidos", dia, desde=dia, hasta=dia)
    if pron.empty or ped.empty:
        raise RuntimeError(f"no hay pronóstico y pedido del corte {dia}: corre primero pronosticar y generar_pedido")
    ruta, escrito = manifiesto.publicar(cfg, dia, pron, ped, _capa(cfg, "pronosticos"))
    return {"manifiesto": str(ruta.relative_to(_capa(cfg, "pronosticos"))), "escrito": escrito}


def procesar_capas_hasta(fecha: str | date, cfg: dict | None = None) -> None:
    """Corre día a día (idempotente) bandeja, ingesta y validación desde la fecha de corte hasta `fecha`."""
    cfg, fin = _cfg(cfg), _fecha(fecha)
    dia = date.fromisoformat(cfg["bandeja"]["fecha_corte"])
    while dia <= fin:
        recibir_bandeja(dia, cfg)
        ingestar(dia, cfg)
        validar(dia, cfg)
        dia += timedelta(days=1)


# --- Seguimiento del desempeño ----------------------------------------------------------


def dias_de_pronostico(desde: date, hasta: date, cfg: dict | None = None) -> list[date]:
    """Días de pronóstico semanal en [desde, hasta)."""
    dia_semana = _cfg(cfg)["orquestacion"]["dia_semana_pronostico"]
    return [desde + timedelta(days=i) for i in range((hasta - desde).days) if (desde + timedelta(days=i)).weekday() == dia_semana]


def pronosticos_previos_listos(fecha: str | date, cfg: dict | None = None) -> bool:
    """True si ya existen el pronóstico y el pedido de todos los días de pronóstico anteriores a `fecha`."""
    from laboratorio.capas import listar_revisiones, ruta_particion
    cfg, dia = _cfg(cfg), _fecha(fecha)
    inicio = date.fromisoformat(cfg["orquestacion"]["inicio"])
    dir_pron = _capa(cfg, "pronosticos")
    return all(
        listar_revisiones(ruta_particion(dir_pron, nombre, d))
        for d in dias_de_pronostico(inicio, dia, cfg)
        for nombre in ("pronosticos", "pedidos")
    )


def seguir_desempeno(fecha: str | date, cfg: dict | None = None) -> dict:
    """Evalúa cada pronóstico anterior a `fecha` con las ventas reales conocidas ese día.

    Una partición de desempeño se reprocesa (revisión nueva) cuando llegan más días
    observados o una revisión de un día ya evaluado.
    """
    import pandas as pd

    from laboratorio.capas import (
        escribir_derivado,
        insumos_de,
        leer_asof,
        leer_linaje,
        listar_particiones,
        listar_revisiones,
        ruta_particion,
    )
    from laboratorio.monitoring.desempeno import evaluar
    cfg, dia = _cfg(cfg), _fecha(fecha)
    dir_pron, dir_seg, silver = _capa(cfg, "pronosticos"), _capa(cfg, "seguimiento"), _capa(cfg, "silver")
    umbral = cfg["seguimiento"]["umbral_wape"]
    resumen = {"actualizadas": [], "reprocesadas_por_revision": [], "alertas": []}
    for corte in [d for d in listar_particiones(dir_pron, "pronosticos") if d < dia]:
        pron = leer_asof(dir_pron, "pronosticos", dia, desde=corte, hasta=corte)
        ped = leer_asof(dir_pron, "pedidos", dia, desde=corte, hasta=corte)
        if pron.empty:
            continue
        objetivos = pd.to_datetime(pron["fecha_objetivo"])
        reales = leer_asof(silver, "ventas", dia, desde=objetivos.min().date(), hasta=objetivos.max().date())
        if reales.empty:
            continue
        df = evaluar(_sin_control(pron), _sin_control(ped), reales[["date", "store_nbr", "family", "sales"]], umbral)
        insumos = {
            **insumos_de(reales, "ventas"),
            **insumos_de(pron, "pronosticos"),
            **insumos_de(ped, "pedidos"),
            "umbral_wape": umbral,
        }
        revs = listar_revisiones(ruta_particion(dir_seg, "desempeno", corte))
        previos = leer_linaje(revs[max(revs)]).get("insumos", {}) if revs else {}
        rev = escribir_derivado(dir_seg, "desempeno", corte, df, insumos, available_at=dia)
        if rev is None:
            continue
        resumen["actualizadas"].append(corte.isoformat())
        if any(k in insumos and insumos[k] != v for k, v in previos.items()):
            resumen["reprocesadas_por_revision"].append(corte.isoformat())
        total = df[df["family"] == "TOTAL"].iloc[0]
        if total["alerta"]:
            resumen["alertas"].append({"corte": corte.isoformat(), "wape": round(float(total["wape"]), 4)})
            log.warning("Alerta de desempeño: pronóstico %s con WAPE %.3f > %.3f", corte, total["wape"], umbral)
    return resumen


TAREAS = {
    f.__name__: f
    for f in (recibir_bandeja, ingestar, validar, preparar_features, pronosticar, generar_pedido, publicar, seguir_desempeno)
}


def main(argv: list[str] | None = None) -> None:
    args = argv if argv is not None else sys.argv[1:]
    if len(args) != 2 or args[0] not in TAREAS:
        sys.exit(f"uso: python -m laboratorio.tareas {{{'|'.join(TAREAS)}}} AAAA-MM-DD")
    print(TAREAS[args[0]](args[1]))


if __name__ == "__main__":
    main()
