"""Modelado: esquema temporal sin fuga, LightGBM por cuantiles, métricas y campeón guardado."""

import copy
import json
from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest

from laboratorio.config import cargar_config
from laboratorio.forecast import dataset, evaluacion, modelos
from laboratorio.forecast.entrenamiento import guardar_campeon, leer_campeon
from laboratorio.forecast.lgbm import CATEGORICAS, NUMERICAS, ModeloCuantiles, pronosticar
from laboratorio.orders.pedido import calcular_pedido

CFG = cargar_config()
PARAMS = {"n_estimators": 5, "learning_rate": 0.3, "num_leaves": 7, "min_child_samples": 5, "n_jobs": 1, "seed": 1}


def predictores_sinteticos(n_series=6, cortes=(date(2017, 1, 2),), horizonte=7, semilla=0):
    rng = np.random.default_rng(semilla)
    filas = []
    for corte in cortes:
        for s in range(n_series):
            nivel = 10 + 20 * s
            for h in range(1, horizonte + 1):
                objetivo = corte + timedelta(days=h)
                fila = {c: float(rng.integers(0, 3)) for c in NUMERICAS}
                fila.update(
                    store_nbr=1 + s % 3, family=f"F{s % 2}", tienda_tipo="A", fecha_objetivo=objetivo.isoformat(),
                    h=h, media_28=nivel, media_7=nivel, std_28=nivel * 0.2, media_dia_semana=nivel, corte=corte,
                    tienda_cluster=3.0,
                )  # fmt: skip
                filas.append(fila)
    return pd.DataFrame(filas)


# --- esquema temporal -----------------------------------------------------------------


def test_cortes_de_prueba_son_lunes_con_horizonte_observado():
    ultima_venta = dataset.fecha_entrenamiento(CFG) - timedelta(days=CFG["bandeja"]["fuentes"]["ventas"]["retraso_dias"])
    pruebas = dataset.cortes_de_prueba(CFG)
    assert len(pruebas) == CFG["modelado"]["folds"]
    assert all(c.weekday() == CFG["orquestacion"]["dia_semana_pronostico"] for c in pruebas)
    p = CFG["pronostico"]
    assert max(pruebas) + timedelta(days=p["anticipacion_dias"] + p["horizonte_dias"] - 1) <= ultima_venta
    # el siguiente lunes ya no tendría el horizonte completo
    assert max(pruebas) + timedelta(days=7 + 6) > ultima_venta


def test_entrenamiento_nunca_usa_etiquetas_del_futuro():
    """Para cada fold, toda etiqueta de entrenamiento (y su _rev2) se conocía el día de la prueba."""
    espera = CFG["modelado"]["dias_espera_etiquetas"]
    p, rev = CFG["pronostico"], CFG["bandeja"]["revisiones"]["retraso_extra_dias"]
    retraso_ventas = CFG["bandeja"]["fuentes"]["ventas"]["retraso_dias"]
    assert espera >= p["anticipacion_dias"] + p["horizonte_dias"] - 1 + retraso_ventas + rev
    for f in dataset.cortes_de_prueba(CFG):
        cortes = dataset.cortes_de_entrenamiento(CFG, f)
        assert len(cortes) == CFG["modelado"]["semanas_entrenamiento"]
        assert max(cortes) < f
        ultimo_objetivo = max(cortes) + timedelta(days=p["anticipacion_dias"] + p["horizonte_dias"] - 1)
        conocida_el = ultimo_objetivo + timedelta(days=retraso_ventas + rev)
        assert conocida_el <= f


def test_los_folds_entrenan_con_ventanas_que_avanzan():
    ventanas = [dataset.cortes_de_entrenamiento(CFG, f) for f in dataset.cortes_de_prueba(CFG)]
    assert [max(v) for v in ventanas] == sorted(max(v) for v in ventanas)
    assert len({max(v) for v in ventanas}) == len(ventanas)


def test_corrida_final_incluye_todos_los_cortes_con_etiquetas():
    final = dataset.cortes_de_entrenamiento(CFG)
    assert max(final) <= dataset.fecha_entrenamiento(CFG) - timedelta(days=CFG["modelado"]["dias_espera_etiquetas"])
    assert set(final) | set(dataset.cortes_de_prueba(CFG)) <= set(dataset.todos_los_cortes(CFG))


def test_cortes_semanales():
    cortes = dataset.cortes_semanales(date(2017, 8, 10), 3, 0)  # jueves -> lunes anterior
    assert cortes == [date(2017, 7, 24), date(2017, 7, 31), date(2017, 8, 7)]
    assert dataset.cortes_semanales(date(2017, 8, 7), 1, 0) == [date(2017, 8, 7)]


def test_con_etiquetas_descarta_filas_sin_venta():
    pred = predictores_sinteticos(n_series=1)
    etiquetas = pd.DataFrame(
        {"date": pred["fecha_objetivo"].iloc[:3], "store_nbr": pred["store_nbr"].iloc[0], "family": pred["family"].iloc[0], "sales": [1.0, 2.0, 3.0]}
    )
    unido = dataset.con_etiquetas(pred, etiquetas)
    assert len(unido) == 3 and list(unido["sales"]) == [1.0, 2.0, 3.0]


# --- LightGBM por cuantiles -----------------------------------------------------------


def entrenado(cuantiles=(0.1, 0.5, 0.9)):
    pred = predictores_sinteticos(cortes=[date(2017, 1, 2) + timedelta(weeks=i) for i in range(8)])
    rng = np.random.default_rng(1)
    ventas = pd.Series(pred["media_28"] * rng.lognormal(0, 0.3, len(pred)))
    return ModeloCuantiles(list(cuantiles), PARAMS).entrenar(pred, ventas), pred, ventas


def test_cuantiles_ordenados_y_no_negativos():
    modelo, pred, _ = entrenado()
    q = modelo.predecir(pred)
    assert list(q.columns) == ["q10", "q50", "q90"]
    assert (q["q10"] <= q["q50"]).all() and (q["q50"] <= q["q90"]).all() and (q >= 0).all().all()


def test_cuantil_alto_cubre_mas_que_el_bajo():
    modelo, pred, ventas = entrenado()
    q = modelo.predecir(pred)
    assert (ventas <= q["q90"]).mean() > (ventas <= q["q10"]).mean()


def test_pronostico_es_la_mediana_con_sigma():
    modelo, pred, _ = entrenado()
    out = pronosticar(modelo, pred)
    assert np.allclose(out["pronostico"], out["q50"]) and (out["sigma_diario"] >= 0).all()
    assert {"store_nbr", "family", "fecha_objetivo", "h"} <= set(out.columns)


def test_guardar_y_cargar_da_las_mismas_predicciones(tmp_path):
    modelo, pred, _ = entrenado()
    modelo.guardar(tmp_path / "m")
    otro = ModeloCuantiles.cargar(tmp_path / "m")
    pd.testing.assert_frame_equal(modelo.predecir(pred), otro.predecir(pred))
    nuevo = pred.iloc[:5]  # el mismo resultado con un lote distinto: las categorías se remapean bien
    pd.testing.assert_frame_equal(otro.predecir(nuevo), modelo.predecir(pred).iloc[:5])


def test_entrenamiento_reproducible():
    a, pred, _ = entrenado()
    b, _, _ = entrenado()
    pd.testing.assert_frame_equal(a.predecir(pred), b.predecir(pred))


def test_la_mediana_es_obligatoria():
    with pytest.raises(ValueError, match="0.5"):
        ModeloCuantiles([0.1, 0.9], PARAMS)


def test_modelos_registra_lightgbm_desde_carpeta(tmp_path):
    modelo, pred, _ = entrenado()
    modelo.guardar(tmp_path)
    out = modelos.pronosticar(pred.iloc[:7], modelos.LIGHTGBM, tmp_path)
    assert (out["modelo"] == modelos.LIGHTGBM).all() and "sigma_diario" in out
    with pytest.raises(ValueError, match="desconocido"):
        modelos.pronosticar(pred, "no_existe")


def test_el_modelo_no_usa_columnas_de_identidad_del_corte():
    # fechas y el propio corte no son predictores: solo lo conocido hasta el corte
    assert not {"corte", "fecha_objetivo", "sales"} & set(NUMERICAS + CATEGORICAS)


# --- pedido, métricas y backtest ------------------------------------------------------


def test_pedido_usa_sigma_del_modelo_si_existe():
    pred = predictores_sinteticos(n_series=1)
    pron = pred[["store_nbr", "family", "fecha_objetivo", "h"]].assign(pronostico=10.0, sigma_diario=2.0)
    costos = cargar_config("costs.yaml")
    con_modelo = calcular_pedido(pron, pred, costos)
    sin_modelo = calcular_pedido(pron.drop(columns="sigma_diario"), pred, costos)
    assert con_modelo["sigma"].iloc[0] == pytest.approx(2.0 * np.sqrt(7))
    assert sin_modelo["sigma"].iloc[0] == pytest.approx(pred["std_28"].iloc[0] * np.sqrt(7))


def reales_de(pron, factor):
    return pron.rename(columns={"fecha_objetivo": "date"}).assign(sales=pron["pronostico"] * factor)[["date", "store_nbr", "family", "sales"]]


def pron_perfecto():
    pred = predictores_sinteticos(n_series=2)
    pron = pred[["store_nbr", "family", "fecha_objetivo", "h"]].assign(pronostico=10.0, q10=8.0, q50=10.0, q90=12.0, sigma_diario=1.5)
    return pron, pred


def test_componentes_y_agregado_pronostico_exacto():
    pron, pred = pron_perfecto()
    comp = evaluacion.componentes_fold(pron, pred, reales_de(pron, 1.0), cargar_config("costs.yaml"), 0.35)
    m = evaluacion.agregar([comp])
    assert m["wape"] == pytest.approx(0) and m["sesgo"] == pytest.approx(0) and m["rmsle"] == pytest.approx(0)
    assert m["cobertura"] == 1.0 and m["fill_rate"] >= 0.5 and m["costo"] >= 0


def test_agregado_suma_folds_antes_de_dividir():
    pron, pred = pron_perfecto()
    costos = cargar_config("costs.yaml")
    a = evaluacion.componentes_fold(pron, pred, reales_de(pron, 2.0), costos, 0.35)  # vende el doble
    b = evaluacion.componentes_fold(pron, pred, reales_de(pron, 1.0), costos, 0.35)
    m = evaluacion.agregar([a, b])
    assert m["wape"] == pytest.approx((a["error_abs"] + b["error_abs"]) / (a["venta_real"] + b["venta_real"]))
    assert m["costo"] == pytest.approx(a["costo"] + b["costo"]) and m["costo_por_fold"] == [a["costo"], b["costo"]]
    assert m["sesgo"] < 0  # pronostica menos de lo vendido


def test_falta_de_inventario_baja_el_fill_rate_y_sube_el_costo():
    pron, pred = pron_perfecto()
    costos = cargar_config("costs.yaml")
    normal = evaluacion.agregar([evaluacion.componentes_fold(pron, pred, reales_de(pron, 1.0), costos, 0.35)])
    pico = evaluacion.agregar([evaluacion.componentes_fold(pron, pred, reales_de(pron, 3.0), costos, 0.35)])
    assert pico["fill_rate"] < normal["fill_rate"] and pico["costo"] > normal["costo"]


def test_fold_con_horizonte_incompleto_falla():
    pron, pred = pron_perfecto()
    parcial = reales_de(pron, 1.0)
    parcial = parcial[parcial["date"] != parcial["date"].max()]
    with pytest.raises(ValueError, match="horizonte completo"):
        evaluacion.componentes_fold(pron, pred, parcial, cargar_config("costs.yaml"), 0.35)


def test_backtest_de_baseline_y_lightgbm_con_datos_sinteticos(monkeypatch):
    cfg = cargar_config()
    cfg["modelado"] = {**cfg["modelado"], "folds": 2, "semanas_entrenamiento": 4, "lightgbm": PARAMS}
    pruebas = dataset.cortes_de_prueba(cfg)
    cortes = dataset.todos_los_cortes(cfg)
    pred = predictores_sinteticos(n_series=8, cortes=cortes)
    rng = np.random.default_rng(3)
    et = pred[["fecha_objetivo", "store_nbr", "family", "media_28"]].drop_duplicates(["fecha_objetivo", "store_nbr", "family"])
    et = et.assign(sales=et["media_28"] * rng.lognormal(0, 0.2, len(et))).rename(columns={"fecha_objetivo": "date"}).drop(columns="media_28")
    costos = cargar_config("costs.yaml")
    base = evaluacion.backtest(evaluacion.receta_baseline(cfg), cfg, pred, et, costos)
    lgbm = evaluacion.backtest(evaluacion.receta_lightgbm(cfg), cfg, pred, et, costos)
    assert base["cortes_prueba"] == [c.isoformat() for c in pruebas] == lgbm["cortes_prueba"]
    assert len(base["por_fold"]) == 2 and len(lgbm["costo_por_fold"]) == 2
    assert "cobertura" in lgbm and "cobertura" not in base
    assert np.isfinite(lgbm["wape"]) and np.isfinite(base["wape"])


# --- campeón guardado -----------------------------------------------------------------


def test_sin_campeon_se_parte_de_la_linea_base(tmp_path):
    assert leer_campeon(tmp_path / "campeon", CFG) == evaluacion.receta_baseline(CFG)


def test_guardar_campeon_reemplaza_sin_dejar_restos(tmp_path):
    destino = tmp_path / "campeon"
    modelo, pred, _ = entrenado()
    receta = evaluacion.receta_lightgbm(CFG)
    guardar_campeon(destino, evaluacion.receta_baseline(CFG), {"version": "v0"})
    assert leer_campeon(destino, CFG)["tipo"] == "baseline" and not (destino / "q50.txt").exists()
    guardar_campeon(destino, receta, {"version": "v1"}, modelo, {"decision": {"reemplazar": True}})
    assert leer_campeon(destino, CFG) == receta
    assert json.loads((destino / "meta.json").read_text())["version"] == "v1"
    assert sorted(p.name for p in destino.iterdir()) == ["cuantiles.json", "meta.json", "metricas.json", "q10.txt", "q50.txt", "q90.txt"]
    assert sorted(p.name for p in tmp_path.iterdir()) == ["campeon"]  # sin .nuevo ni .viejo
    pd.testing.assert_frame_equal(ModeloCuantiles.cargar(destino).predecir(pred), modelo.predecir(pred))


def test_config_de_modelado_coherente():
    m = CFG["modelado"]
    assert 0.5 in m["cuantiles"] and m["folds"] >= m["reemplazo"]["folds_ganados_minimos"]
    assert m["lightgbm"]["n_jobs"] <= 4  # laptop


# --- el modelo no conoce nada posterior al inicio del flujo ------------------------------


def test_el_modelo_se_entrena_solo_con_la_carga_historica():
    """Todo pronóstico del flujo (desde bandeja.fecha_corte) es posterior a los datos del entrenamiento."""
    assert dataset.fecha_entrenamiento(CFG) <= date.fromisoformat(CFG["bandeja"]["fecha_corte"])
    assert max(dataset.todos_los_cortes(CFG)) + timedelta(days=CFG["pronostico"]["horizonte_dias"]) <= dataset.fecha_entrenamiento(CFG)


def cfg_con_modelos(tmp_path, modelo="campeon"):
    cfg = copy.deepcopy(CFG)
    cfg["capas"]["modelos"] = str(tmp_path / "models")
    cfg["pronostico"]["modelo"] = modelo
    return cfg


def test_modelo_vigente(tmp_path):
    cfg = cfg_con_modelos(tmp_path)
    with pytest.raises(FileNotFoundError, match="campeón"):
        modelos.modelo_vigente(cfg)
    guardar_campeon(modos := tmp_path / "models" / "campeon", evaluacion.receta_baseline(cfg), {"version": "v0"})
    assert modelos.hay_campeon(cfg) and modelos.modelo_vigente(cfg) == "media_dia_semana"
    guardar_campeon(modos, evaluacion.receta_lightgbm(cfg), {"version": "v1"})
    assert modelos.modelo_vigente(cfg) == modelos.LIGHTGBM
    assert modelos.modelo_vigente(cfg_con_modelos(tmp_path, "media_28")) == "media_28"  # nombre explícito


def test_pronosticar_entrena_el_campeon_inicial_una_sola_vez(tmp_path, cfg_prueba, monkeypatch):
    """Desde cero no hay intervención manual: la primera tarea de pronóstico deja listo el campeón."""
    from laboratorio import tareas

    cfg = cfg_prueba
    cfg["pronostico"]["modelo"] = "campeon"
    cfg["capas"]["modelos"] = str(tmp_path / "models")
    llamadas = []

    def entrenar_falso(c):
        llamadas.append(c)
        guardar_campeon(tmp_path / "models" / "campeon", evaluacion.receta_baseline(c), {"version": "v0"})

    monkeypatch.setattr(tareas.entrenamiento, "entrenar", entrenar_falso)
    dia = date(2017, 1, 4)
    tareas.procesar_capas_hasta(dia, cfg)
    tareas.preparar_features(dia, cfg, historia=False)
    assert tareas.pronosticar(dia, cfg)["modelo"] == "media_dia_semana"  # el campeón es la línea base
    tareas.pronosticar(dia, cfg)
    assert len(llamadas) == 1
