"""Regla de reemplazo del campeón: cada condición por separado."""

import copy

import pytest

from laboratorio.config import cargar_config
from laboratorio.forecast.reemplazo import decidir_reemplazo

REGLAS = {
    "mejora_minima_costo": 0.02,
    "tolerancia_wape": 0.0,
    "folds_ganados_minimos": 3,
    "fill_rate_minimo": 0.85,
    "sesgo_maximo": 0.10,
    "cobertura_intervalo": [0.70, 0.90],
}
CAMPEON = {"wape": 0.30, "sesgo": 0.0, "fill_rate": 0.90, "costo": 1000.0, "costo_por_fold": [250.0] * 4}
CANDIDATO = {
    "wape": 0.25, "sesgo": -0.02, "fill_rate": 0.91, "costo": 900.0, "costo_por_fold": [225.0] * 4, "cobertura": 0.8,
}  # fmt: skip


def decidir(**cambios):
    cand = {**copy.deepcopy(CANDIDATO), **cambios}
    return decidir_reemplazo(CAMPEON, cand, REGLAS)


def incumplidas(d):
    return {c.nombre for c in d.condiciones if not c.cumple}


def test_candidato_que_cumple_todo_reemplaza():
    d = decidir()
    assert d.reemplazar and not incumplidas(d)


def test_mejora_de_costo_insuficiente():
    d = decidir(costo=985.0, costo_por_fold=[246.0] * 4)  # 1.5 % < 2 %
    assert not d.reemplazar and incumplidas(d) == {"costo"}


def test_mejora_justo_en_el_minimo_cumple():
    assert decidir(costo=980.0, costo_por_fold=[245.0] * 4).reemplazar  # exactamente 2 %


def test_wape_peor_bloquea_aunque_el_costo_mejore():
    d = decidir(wape=0.31)
    assert not d.reemplazar and incumplidas(d) == {"wape"}


def test_tolerancia_de_wape():
    reglas = {**REGLAS, "tolerancia_wape": 0.05}
    assert decidir_reemplazo(CAMPEON, {**CANDIDATO, "wape": 0.31}, reglas).reemplazar  # 0.31 <= 0.315


def test_estabilidad_exige_ganar_en_suficientes_folds():
    # gana 2 de 4 folds (aunque el total sea mejor): no alcanza
    d = decidir(costo_por_fold=[100.0, 100.0, 300.0, 400.0])
    assert not d.reemplazar and incumplidas(d) == {"estabilidad"}
    assert decidir(costo_por_fold=[100.0, 100.0, 100.0, 600.0]).reemplazar  # gana 3 de 4


def test_empate_en_un_fold_no_cuenta_como_victoria():
    d = decidir(costo_por_fold=[250.0, 250.0, 100.0, 100.0])
    assert "estabilidad" in incumplidas(d)


def test_fill_rate_minimo():
    d = decidir(fill_rate=0.80)
    assert not d.reemplazar and incumplidas(d) == {"fill_rate"}


@pytest.mark.parametrize("sesgo", [0.11, -0.11])
def test_sesgo_absoluto_maximo(sesgo):
    d = decidir(sesgo=sesgo)
    assert not d.reemplazar and incumplidas(d) == {"sesgo"}


@pytest.mark.parametrize("cobertura", [0.50, 0.95])
def test_cobertura_fuera_de_rango(cobertura):
    d = decidir(cobertura=cobertura)
    assert not d.reemplazar and incumplidas(d) == {"cobertura"}


def test_sin_cuantiles_no_se_evalua_cobertura():
    cand = {k: v for k, v in CANDIDATO.items() if k != "cobertura"}
    d = decidir_reemplazo(CAMPEON, cand, REGLAS)
    assert d.reemplazar and "cobertura" not in {c.nombre for c in d.condiciones}


def test_metricas_nan_conservan_el_campeon():
    d = decidir(wape=float("nan"))
    assert not d.reemplazar and "wape" in incumplidas(d)


def test_campeon_sin_costo_no_reemplaza():
    d = decidir_reemplazo({**CAMPEON, "costo": 0.0}, CANDIDATO, REGLAS)
    assert not d.reemplazar and "costo" in incumplidas(d)


def test_motivo_y_dict_legibles():
    d = decidir(fill_rate=0.5, sesgo=0.5)
    assert d.motivo == "no cumple: fill_rate; sesgo"
    assert d.a_dict()["reemplazar"] is False and len(d.a_dict()["condiciones"]) == 6


def test_las_reglas_de_config_tienen_todas_las_claves():
    reglas = cargar_config()["modelado"]["reemplazo"]
    assert decidir_reemplazo(CAMPEON, CANDIDATO, reglas).condiciones  # no lanza KeyError
