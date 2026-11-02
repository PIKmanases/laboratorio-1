"""Tareas que llaman los DAGs: flujo diario/semanal completo, backfill y reproceso por revisión."""

import hashlib
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest
from conftest import CORTE, ULTIMO_DIA, crear_cfg_prueba, crear_zip_ejemplo

from laboratorio import tareas
from laboratorio.capas import (
    leer_asof,
    leer_linaje,
    listar_particiones,
    listar_revisiones,
    ruta_particion,
)
from laboratorio.config import cargar_config

LUNES = date(2017, 1, 2)  # único día de pronóstico en los datos de ejemplo
HORIZONTE = 7


def dias(desde=CORTE, hasta=ULTIMO_DIA):
    return [desde + timedelta(days=i) for i in range((hasta - desde).days + 1)]


def correr_backfill(cfg, desde=CORTE, hasta=ULTIMO_DIA):
    """Lo que hace Airflow en un backfill: cada día, los 4 DAGs en orden."""
    resumen = {}
    for d in dias(desde, hasta):
        r = {n: getattr(tareas, n)(d.isoformat(), cfg) for n in ("recibir_bandeja", "ingestar", "validar")}
        r["preparar_features"] = tareas.preparar_features(d.isoformat(), cfg)
        if d.weekday() == cfg["orquestacion"]["dia_semana_pronostico"]:
            r["pronosticar"] = tareas.pronosticar(d.isoformat(), cfg)
            r["generar_pedido"] = tareas.generar_pedido(d.isoformat(), cfg)
        assert tareas.pronosticos_previos_listos(d.isoformat(), cfg)
        r["seguir_desempeno"] = tareas.seguir_desempeno(d.isoformat(), cfg)
        resumen[d] = r
    return resumen


def capa(cfg, nombre):
    return Path(cfg["capas"][nombre])


def huellas(cfg):
    salida = {}
    for nombre in ("features", "pronosticos", "seguimiento", "silver"):
        base = capa(cfg, nombre)
        for p in base.rglob("*"):
            if p.is_file():
                salida[f"{nombre}/{p.relative_to(base)}"] = (hashlib.md5(p.read_bytes()).hexdigest(), p.stat().st_mtime_ns)
    return salida


@pytest.fixture(scope="module")
def backfill(tmp_path_factory):
    """Un backfill completo compartido por el módulo (las pruebas que lo repiten verifican que no cambia nada)."""
    base = tmp_path_factory.mktemp("backfill")
    cfg = crear_cfg_prueba(base, crear_zip_ejemplo(base))
    return cfg, correr_backfill(cfg)


def test_features_as_of_por_corte(backfill):
    cfg, _ = backfill
    dir_feat = capa(cfg, "features")
    cortes = listar_particiones(dir_feat, "predictores")
    # El primer día prepara los cortes históricos que tengan datos conocidos (aquí, ninguno antes de CORTE + 1).
    assert min(cortes) >= CORTE and max(cortes) == ULTIMO_DIA
    for corte in cortes:
        df = leer_asof(dir_feat, "predictores", corte, desde=corte, hasta=corte)
        assert (pd.to_datetime(df["fecha_objetivo"]) > pd.Timestamp(corte)).all()
        assert sorted(df["h"].unique()) == list(range(1, HORIZONTE + 1))
        assert len(df) == df[["store_nbr", "family"]].drop_duplicates().shape[0] * HORIZONTE
        # Linaje: solo usó datos conocidos ese día.
        linaje = leer_linaje(listar_revisiones(ruta_particion(dir_feat, "predictores", corte))[1])
        fechas = [date.fromisoformat(k.split(":")[1]) for k in linaje["insumos"] if ":" in k]
        assert fechas and max(fechas) < corte


def test_features_no_se_reprocesan_por_revisiones_posteriores(backfill):
    """Lo que se sabía el día X no cambia: un _rev2 posterior no toca los predictores de X."""
    cfg, _ = backfill
    for particion in listar_particiones(capa(cfg, "features"), "predictores").values():
        assert list(listar_revisiones(particion)) == [1]


def test_pronostico_y_pedido_semanal(backfill):
    cfg, resumen = backfill
    assert [d for d, r in resumen.items() if "pronosticar" in r] == [LUNES, LUNES + timedelta(days=7)]
    dir_pron = capa(cfg, "pronosticos")
    pron = leer_asof(dir_pron, "pronosticos", LUNES, desde=LUNES, hasta=LUNES)
    pedido = leer_asof(dir_pron, "pedidos", LUNES, desde=LUNES, hasta=LUNES)
    assert pron["fecha_objetivo"].min() == (LUNES + timedelta(days=1)).isoformat()
    assert (pron["pronostico"] >= 0).all()
    assert (pedido["cantidad_pedido"] >= 0).all()
    # Nivel de servicio = costo_faltante / (costo_faltante + costo_sobrante), de config/costs.yaml.
    costos = cargar_config("costs.yaml")
    for fila in pedido.itertuples():
        c = {**costos["por_defecto"], **costos["por_familia"].get(fila.family, {})}
        assert fila.nivel_servicio == pytest.approx(c["costo_faltante"] / (c["costo_faltante"] + c["costo_sobrante"]))
    assert costos["por_familia"]["PRODUCE"]["costo_sobrante"] > costos["por_defecto"]["costo_sobrante"]


def test_seguimiento_reprocesa_cuando_llega_una_revision(backfill):
    cfg, resumen = backfill
    # Ventas del 2017-01-03 (primer día del horizonte del lunes): rev1 llega el 01-04 y su _rev2 el 01-07.
    llegada_rev2 = date(2017, 1, 7)
    assert resumen[date(2017, 1, 4)]["seguir_desempeno"]["actualizadas"] == [LUNES.isoformat()]
    assert resumen[llegada_rev2]["seguir_desempeno"]["reprocesadas_por_revision"] == [LUNES.isoformat()]
    assert all(
        not r["seguir_desempeno"]["reprocesadas_por_revision"] for d, r in resumen.items() if d != llegada_rev2
    )

    dir_seg = capa(cfg, "seguimiento")
    revs = listar_revisiones(ruta_particion(dir_seg, "desempeno", LUNES))
    assert list(revs) == [1, 2]
    linajes = [leer_linaje(r)["insumos"] for r in revs.values()]
    assert linajes[0]["ventas:2017-01-03"] == 1 and linajes[1]["ventas:2017-01-03"] == 2

    # As-of: el desempeño visto antes de la revisión es el de rev1; después, el corregido.
    antes = leer_asof(dir_seg, "desempeno", llegada_rev2 - timedelta(days=1))
    despues = leer_asof(dir_seg, "desempeno", llegada_rev2)
    assert set(antes["revision"]) == {1} and set(despues["revision"]) == {2}
    total = despues.set_index("family").loc["TOTAL"]
    assert total["dias_observados"] == 1 and not total["horizonte_completo"]


def test_repetir_el_backfill_no_cambia_nada(backfill):
    cfg, _ = backfill
    antes = huellas(cfg)
    resumen = correr_backfill(cfg)
    assert huellas(cfg) == antes
    assert all(r["preparar_features"]["cortes_escritos"] == 0 for r in resumen.values())
    assert all(not r["seguir_desempeno"]["actualizadas"] for r in resumen.values())


def test_backfill_es_reproducible(backfill, tmp_path_factory):
    """Dos backfills desde cero producen exactamente los mismos datos."""
    cfg, _ = backfill
    otro = {**cfg, "capas": {}, "bandeja": {**cfg["bandeja"]}}
    base = tmp_path_factory.mktemp("otro")
    otro["capas"] = {n: str(base / n) for n in cfg["capas"]}
    otro["bandeja"]["dir"] = str(base / "inbox")
    correr_backfill(otro)
    contenido = lambda c: {k: v[0] for k, v in huellas(c).items()}
    assert contenido(otro) == contenido(cfg)


def test_reprocesar_un_dia_es_idempotente(backfill):
    """Equivale a `airflow tasks clear` de un día: reejecutarlo no cambia nada."""
    cfg, _ = backfill
    antes = huellas(cfg)
    for nombre in ("recibir_bandeja", "ingestar", "validar", "preparar_features", "pronosticar", "generar_pedido"):
        getattr(tareas, nombre)(LUNES.isoformat(), cfg)
    tareas.seguir_desempeno(ULTIMO_DIA.isoformat(), cfg)
    assert {k: v for k, v in huellas(cfg).items() if not k.startswith("silver")} == {
        k: v for k, v in antes.items() if not k.startswith("silver")
    }


def test_pronosticos_previos_listos(cfg_prueba):
    assert tareas.pronosticos_previos_listos(LUNES.isoformat(), cfg_prueba)  # no hay lunes anteriores
    assert not tareas.pronosticos_previos_listos((LUNES + timedelta(days=1)).isoformat(), cfg_prueba)


def test_cli(cfg_prueba, capsys):
    with pytest.raises(SystemExit):
        tareas.main(["no_existe", "2017-01-01"])
