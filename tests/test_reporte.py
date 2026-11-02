"""Reporte de evidencia: ejecuciones, comparación de modelos y seguimiento del desempeño."""

from datetime import date

import pytest
from conftest import crear_cfg_prueba, crear_zip_ejemplo
from test_tareas import correr_backfill

from laboratorio import tareas
from laboratorio.forecast import evaluacion
from laboratorio.forecast.entrenamiento import guardar_campeon
from laboratorio.publicacion import reporte

LUNES = date(2017, 1, 2)
ASOF = date(2017, 1, 10)


@pytest.fixture(scope="module")
def flujo(tmp_path_factory):
    """Backfill de los datos de ejemplo con horizonte de 1 día: el pronóstico del lunes queda observado por completo."""
    base = tmp_path_factory.mktemp("reporte")
    cfg = crear_cfg_prueba(base, crear_zip_ejemplo(base))
    cfg["pronostico"]["horizonte_dias"] = 1
    correr_backfill(cfg)
    for lunes in (LUNES, date(2017, 1, 9)):  # la tarea `publicar` del DAG semanal
        tareas.publicar(lunes, cfg)
    return cfg


def metricas(wape, costo, por_fold, **extra):
    return {
        "receta": {"tipo": extra.pop("tipo")}, "wape": wape, "sesgo": 0.01, "rmsle": 0.4, "fill_rate": 0.97, "costo": costo,
        "costo_por_unidad_vendida": 0.05, "costo_por_fold": por_fold, "cortes_prueba": ["2017-06-05", "2017-06-12"], **extra,
    }  # fmt: skip


def test_tabla_markdown():
    t = reporte.tabla([["a", 1], ["b", 2]], ["x", "y"])
    assert t.splitlines() == ["| x | y |", "|---|---|", "| a | 1 |", "| b | 2 |"]


def test_airflow_no_disponible():
    assert "no respondió" in reporte.seccion_airflow(None)
    assert reporte.corridas_airflow("http://127.0.0.1:9") is None  # nada escucha ahí


def test_airflow_resume_las_corridas():
    runs = [
        {"state": "success", "logical_date": "2017-06-30T06:00:00Z", "start_date": "2017-06-30T10:00:00Z", "end_date": "2017-06-30T10:01:00Z"},
        {"state": "failed", "logical_date": "2017-07-01T06:00:00Z", "start_date": "2017-07-01T10:00:00Z", "end_date": "2017-07-01T10:03:00Z"},
    ]  # fmt: skip
    t = reporte.seccion_airflow({"ingesta_validacion": runs})
    assert "| `ingesta_validacion` | 2 | 1 | 1 | 2017-06-30 a 2017-07-01 | 120 s |" in t


def test_modelos_sin_campeon(flujo):
    assert "dvc repro" in reporte.seccion_modelos(flujo)


def test_modelos_compara_y_muestra_la_decision(flujo):
    cfg = flujo
    info = {
        "campeon_anterior": metricas(0.20, 1000.0, [500.0, 500.0], tipo="baseline"),
        "candidato": metricas(0.15, 800.0, [350.0, 450.0], tipo="lightgbm_cuantiles", cobertura=0.81),
        "decision": {"reemplazar": True, "motivo": "el candidato cumple todas las condiciones",
                     "condiciones": [{"nombre": "costo", "cumple": True, "detalle": "mejora 20.00%"}]},
    }  # fmt: skip
    from laboratorio.forecast.modelos import dir_campeon

    guardar_campeon(dir_campeon(cfg), evaluacion.receta_baseline(cfg), {"version": "v9", "entrenado_con_datos_hasta": "2017-06-30"}, None, info)
    t = reporte.seccion_modelos(cfg)
    assert "baseline (campeón anterior)" in t and "lightgbm_cuantiles (candidato)" in t
    assert "| 2017-06-05 | 500 | 350 | candidato |" in t and "| 2017-06-12 | 500 | 450 | candidato |" in t
    assert "| costo | sí | mejora 20.00% |" in t and "**Decisión:** reemplazar" in t and "0.810" in t


def test_desempeno_por_corte_coincide_con_lo_calculado(flujo):
    d = reporte.desempeno_por_corte(flujo, ASOF)
    fila = d[d["corte"] == LUNES].iloc[0]
    assert bool(fila["horizonte_completo"]) and fila["dias_observados"] == 1
    assert fila["revisiones"] >= 1 and 0 <= fila["wape"]


def test_seccion_desempeno_y_alerta(flujo):
    t = reporte.seccion_desempeno(flujo, ASOF)
    assert "2017-01-02" in t and "1/1" in t and "Pronósticos con la semana completa" in t
    flujo_estricto = {**flujo, "seguimiento": {**flujo["seguimiento"], "umbral_wape": 0.0}}
    assert "ALERTA" in reporte.seccion_desempeno(flujo_estricto, ASOF) or "ok" in reporte.seccion_desempeno(flujo_estricto, ASOF)


def test_comparacion_en_vivo_campeon_y_linea_base(flujo):
    t = reporte.comparacion_en_vivo(flujo, ASOF)
    filas = [f for f in t.splitlines() if f.startswith("| 2017-01-02")]
    assert len(filas) == 2 and "(línea base)" in filas[1]
    # el campeón de los datos de ejemplo es la propia línea base: mismas métricas
    assert filas[0].split("|")[3:] == filas[1].split("|")[3:]
    assert "**Agregado**" in t


def test_comparacion_en_vivo_sin_semana_completa(tmp_path, zip_kaggle_ejemplo):
    cfg = crear_cfg_prueba(tmp_path, zip_kaggle_ejemplo)  # horizonte de 7 días: nunca se completa con los datos de ejemplo
    correr_backfill(cfg)
    assert "Aún no hay" in reporte.comparacion_en_vivo(cfg, ASOF)


def test_manifiestos_listados(flujo):
    assert "| 2017-01-02 |" in reporte.seccion_manifiestos(flujo)


def test_reporte_completo(flujo):
    texto = reporte.generar(flujo, asof=ASOF)
    for titulo in ("# Reporte", "## 1. Ejecuciones en Airflow", "## 2. Comparación de modelos", "## 3. Seguimiento del desempeño", "## 4. Trazabilidad"):
        assert titulo in texto
    assert reporte._fecha_asof(flujo) <= ASOF


def test_reporte_sin_ejecuciones(tmp_path, zip_kaggle_ejemplo):
    cfg = crear_cfg_prueba(tmp_path, zip_kaggle_ejemplo)
    texto = reporte.generar(cfg)
    assert "el flujo no ha corrido" in texto and "No hay pronósticos publicados" in texto
