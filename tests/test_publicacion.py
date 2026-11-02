"""Manifiesto de trazabilidad, pedido desde costs.yaml y reproducibilidad del pronóstico."""

import copy
import hashlib
import inspect
import json
import subprocess
from datetime import date
from pathlib import Path

import pandas as pd
import pytest
import yaml
from conftest import crear_cfg_prueba, crear_zip_ejemplo

from laboratorio import tareas
from laboratorio.capas import leer_asof, listar_revisiones, ruta_particion
from laboratorio.config import cargar_config
from laboratorio.publicacion import manifiesto, reproducir

DIA = date(2017, 1, 4)  # ya se conocen las ventas del 01-02 y 01-03 (con 01-01 en cero no hay pedido que probar)


def generar(cfg, dia=DIA):
    """Lo mismo que regenera `reproducir`: capas, features del corte, pronóstico, pedido y manifiesto."""
    tareas.procesar_capas_hasta(dia, cfg)
    tareas.preparar_features(dia, cfg, historia=False)
    tareas.pronosticar(dia, cfg)
    tareas.generar_pedido(dia, cfg)
    return tareas.publicar(dia, cfg)


@pytest.fixture
def zip_ejemplo(tmp_path_factory):
    return crear_zip_ejemplo(tmp_path_factory.mktemp("zip"))


@pytest.fixture
def entorno_pub(tmp_path, zip_ejemplo):
    cfg = crear_cfg_prueba(tmp_path, zip_ejemplo)
    generar(cfg)
    return cfg


def nuevo_entorno(base, nombre, zip_):
    """Otra copia independiente del proyecto (capas propias) con los mismos datos y configuración."""
    directorio = base / nombre
    directorio.mkdir()
    cfg = crear_cfg_prueba(directorio, zip_)
    generar(cfg)
    return cfg


def dir_pron(cfg):
    return Path(cfg["capas"]["pronosticos"])


def leer_manifiesto(cfg, dia=DIA):
    return manifiesto.leer(dir_pron(cfg), dia)


def costos_con(tmp_path, nombre, **cambios):
    """Copia de costs.yaml con otros valores: es un cambio de configuración, no de código."""
    costos = cargar_config("costs.yaml")
    costos["por_defecto"].update(cambios)
    costos["por_familia"] = {}
    ruta = tmp_path / nombre
    ruta.write_text(yaml.safe_dump(costos), encoding="utf-8")
    return str(ruta)


# --- 1. manifiesto ---------------------------------------------------------------------


def test_manifiesto_tiene_todos_los_campos(entorno_pub, zip_ejemplo):
    m = leer_manifiesto(entorno_pub)
    assert m["fecha_corte"] == DIA.isoformat()
    git = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True).stdout.strip()
    assert m["git"]["sha"] == git and isinstance(m["git"]["sucio"], bool)
    assert m["datos"]["md5"] == hashlib.md5(zip_ejemplo.read_bytes()).hexdigest()
    assert m["modelo"]["nombre"] == "media_dia_semana" and m["modelo"]["version"] == "baseline:media_dia_semana"
    assert set(m["configuracion"]) == {"pipeline", "costos", "conjunto"}
    for parte in ("pronostico", "pedido"):
        assert m[parte]["revision"] == 1 and m[parte]["filas"] > 0 and m[parte]["firma_insumos"]
        assert ":" in m[parte]["hash_contenido"]


def test_manifiesto_no_lleva_rutas_ni_horas(entorno_pub, tmp_path):
    texto = json.dumps(leer_manifiesto(entorno_pub))
    assert str(tmp_path) not in texto and "/home/" not in texto


def test_manifiesto_se_guarda_junto_a_la_revision(entorno_pub):
    particion = ruta_particion(dir_pron(entorno_pub), "pronosticos", DIA)
    assert (particion / "rev=1.manifiesto.json").exists() and list(listar_revisiones(particion)) == [1]


def test_publicar_es_idempotente(entorno_pub):
    ruta = manifiesto.ruta_manifiesto(dir_pron(entorno_pub), DIA, 1)
    antes = (ruta.read_bytes(), ruta.stat().st_mtime_ns)
    assert tareas.publicar(DIA, entorno_pub)["escrito"] is False
    assert (ruta.read_bytes(), ruta.stat().st_mtime_ns) == antes


def test_publicar_conserva_el_sha_original(entorno_pub, monkeypatch):
    original = leer_manifiesto(entorno_pub)["git"]
    monkeypatch.setattr(manifiesto, "estado_git", lambda raiz=None: {"sha": "otro", "sucio": False})
    assert tareas.publicar(DIA, entorno_pub)["escrito"] is False  # mismo pronóstico y pedido: no se toca
    assert leer_manifiesto(entorno_pub)["git"] == original


def test_sha_desde_variable_de_entorno(monkeypatch):
    monkeypatch.setenv(manifiesto.ENV_SHA, "abc123")
    assert manifiesto.estado_git() == {"sha": "abc123", "sucio": None}  # el árbol no se conoce
    monkeypatch.setenv(manifiesto.ENV_SUCIO, "1")
    assert manifiesto.estado_git() == {"sha": "abc123", "sucio": True}
    monkeypatch.setenv(manifiesto.ENV_SUCIO, "0")
    assert manifiesto.estado_git() == {"sha": "abc123", "sucio": False}


def test_publicar_exige_pronostico_y_pedido(cfg_prueba):
    with pytest.raises(RuntimeError, match="pronosticar y generar_pedido"):
        tareas.publicar(DIA, cfg_prueba)


def test_hash_del_contenido():
    df = pd.DataFrame({"a": [1, 2, 3], "b": [0.1, 0.2, 0.3], "revision": 1, "available_at": pd.Timestamp("2017-01-02")})
    assert manifiesto.hash_contenido(df) == manifiesto.hash_contenido(df.iloc[::-1])  # el orden no importa
    assert manifiesto.hash_contenido(df) == manifiesto.hash_contenido(df.assign(revision=7))  # control no cuenta
    assert manifiesto.hash_contenido(df) != manifiesto.hash_contenido(df.assign(b=[0.1, 0.2, 0.30000001]))


def test_hash_de_config_ignora_formato(tmp_path):
    a, b = tmp_path / "a.yaml", tmp_path / "b.yaml"
    a.write_text("x: 1\ny: [1, 2]\n", encoding="utf-8")
    b.write_text("# comentario\ny:   [1,2]\nx: 1\n", encoding="utf-8")
    cfg = {"pedido": {"costos": str(a)}}
    h1 = manifiesto.configuracion(cfg)["costos"]
    cfg["pedido"]["costos"] = str(b)
    assert manifiesto.configuracion(cfg)["costos"] == h1


# --- 2. pedido desde costs.yaml --------------------------------------------------------


def pedido(cfg):
    return leer_asof(dir_pron(cfg), "pedidos", DIA, desde=DIA, hasta=DIA)


def test_cambiar_costos_cambia_el_pedido_sin_tocar_codigo(entorno_pub, tmp_path):
    cfg = entorno_pub
    base = pedido(cfg).sort_values(["store_nbr", "family"], ignore_index=True)
    m0 = leer_manifiesto(cfg)

    cfg["pedido"]["costos"] = costos_con(tmp_path, "faltante_caro.yaml", costo_faltante=5.0, costo_sobrante=0.25)
    tareas.generar_pedido(DIA, cfg)  # la misma tarea, otro archivo de costos
    caro = pedido(cfg).sort_values(["store_nbr", "family"], ignore_index=True)

    assert (caro["nivel_servicio"] > base["nivel_servicio"]).all()
    assert (caro["cantidad_pedido"] >= base["cantidad_pedido"]).all()
    assert caro["cantidad_pedido"].sum() > base["cantidad_pedido"].sum()
    assert (caro["costo_faltante"] == 5.0).all()
    # el pronóstico no cambia: solo la política de pedido
    pd.testing.assert_series_equal(caro["demanda_esperada"], base["demanda_esperada"])

    assert tareas.publicar(DIA, cfg)["escrito"] is True  # el manifiesto sigue al pedido vigente
    m1 = leer_manifiesto(cfg)
    assert m1["configuracion"]["costos"] != m0["configuracion"]["costos"]
    assert m1["pedido"]["hash_contenido"] != m0["pedido"]["hash_contenido"]
    assert m1["pronostico"] == m0["pronostico"]


def test_costos_de_sobrante_altos_bajan_el_pedido(entorno_pub, tmp_path):
    cfg = entorno_pub
    base = pedido(cfg)["cantidad_pedido"].sum()
    cfg["pedido"]["costos"] = costos_con(tmp_path, "sobrante_caro.yaml", costo_faltante=1.0, costo_sobrante=4.0)
    tareas.generar_pedido(DIA, cfg)
    assert pedido(cfg)["cantidad_pedido"].sum() < base


def test_mismos_costos_no_cambian_el_pedido(entorno_pub, tmp_path):
    cfg = entorno_pub
    antes = pedido(cfg)
    costos = cargar_config("costs.yaml")
    ruta = tmp_path / "igual.yaml"
    ruta.write_text("# mismo contenido, otro archivo y formato\n" + yaml.safe_dump(costos, sort_keys=False), encoding="utf-8")
    cfg["pedido"]["costos"] = str(ruta)
    tareas.generar_pedido(DIA, cfg)
    pd.testing.assert_frame_equal(pedido(cfg), antes)
    assert tareas.publicar(DIA, cfg)["escrito"] is False


def test_el_pedido_usa_el_archivo_de_costos_de_la_config():
    assert cargar_config()["pedido"]["costos"] == "costs.yaml"
    assert "costs.yaml" not in inspect.getsource(tareas.generar_pedido)  # el nombre no está escrito en el código


# --- 3. reproducibilidad ---------------------------------------------------------------


def test_regenerar_el_pronostico_desde_cero_es_identico(entorno_pub, tmp_path, zip_ejemplo):
    original = leer_manifiesto(entorno_pub)
    otro = nuevo_entorno(tmp_path, "otro", zip_ejemplo)
    regenerado = leer_manifiesto(otro)
    assert reproducir.diferencias(original, regenerado) == []
    assert original == regenerado
    a, b = (leer_asof(dir_pron(c), "pronosticos", DIA) for c in (entorno_pub, otro))
    pd.testing.assert_frame_equal(a.drop(columns="origen"), b.drop(columns="origen"))


def test_regenerar_dos_veces_en_el_mismo_lugar_no_cambia_nada(entorno_pub):
    ruta = manifiesto.ruta_manifiesto(dir_pron(entorno_pub), DIA, 1)
    antes = ruta.read_bytes()
    generar(entorno_pub)
    assert ruta.read_bytes() == antes


def test_un_dato_distinto_se_detecta(entorno_pub, tmp_path, zip_ejemplo):
    original = leer_manifiesto(entorno_pub)
    cfg = nuevo_entorno(tmp_path, "x", zip_ejemplo)
    ruta = manifiesto.ruta_manifiesto(dir_pron(cfg), DIA, 1)
    m = json.loads(ruta.read_text())
    m["pedido"]["hash_contenido"] = "otro"
    dif = reproducir.diferencias(original, m)
    assert dif == [f"pedido.hash_contenido: {original['pedido']['hash_contenido']!r} != 'otro'"]


def test_diferencias_entre_manifiestos():
    a = {"x": 1, "y": {"z": [1, 2], "w": None}}
    assert reproducir.diferencias(a, copy.deepcopy(a)) == []
    assert reproducir.diferencias(a, {"x": 2, "y": {"z": [1, 2], "w": None}}) == ["x: 1 != 2"]
    assert reproducir.diferencias(a, {"x": 1, "y": {"z": [1, 2]}, "extra": 1}) == ["extra: None != 1"]


def test_verificar_entorno(entorno_pub, monkeypatch):
    cfg = entorno_pub
    m = leer_manifiesto(cfg)
    monkeypatch.setattr(manifiesto, "estado_git", lambda raiz=None: {"sha": m["git"]["sha"], "sucio": False})
    assert manifiesto.verificar_entorno(m, cfg) == []
    # otro commit, otro dato y otra configuración se reportan por separado
    cambiado = copy.deepcopy(m)
    cambiado["git"]["sha"] = "0" * 40
    cambiado["datos"]["md5"] = "0" * 32
    cambiado["configuracion"]["pipeline"] = "x"
    dif = manifiesto.verificar_entorno(cambiado, cfg)
    assert [d.split(":")[0] for d in dif] == ["git", "datos", "configuracion.pipeline"]
    monkeypatch.setattr(manifiesto, "estado_git", lambda raiz=None: {"sha": m["git"]["sha"], "sucio": True})
    assert manifiesto.verificar_entorno(m, cfg) == ["git: hay cambios sin commitear"]


def con_manifiesto(monkeypatch, cfg, **git):
    m = leer_manifiesto(cfg)
    m["git"] = {**m["git"], **git}
    monkeypatch.setattr(reproducir, "cargar_config", lambda: cfg)
    monkeypatch.setattr(reproducir, "RAIZ", Path("/"))
    monkeypatch.setattr(reproducir.manifiesto, "leer", lambda *a, **k: copy.deepcopy(m))
    return m


def test_reproducir_rechaza_pronosticos_de_arbol_sucio(entorno_pub, monkeypatch):
    con_manifiesto(monkeypatch, entorno_pub, sucio=True)
    ok, motivo = reproducir.reproducir(DIA, regenerador=lambda sha, fecha: pytest.fail("no debe regenerar"))
    assert not ok and "sin commitear" in motivo[0]


def test_reproducir_sin_sha_no_se_puede(entorno_pub, monkeypatch):
    con_manifiesto(monkeypatch, entorno_pub, sha=None, sucio=False)
    ok, motivo = reproducir.reproducir(DIA, regenerador=lambda sha, fecha: pytest.fail("no debe regenerar"))
    assert not ok and "SHA" in motivo[0]


def test_reproducir_compara_con_el_regenerado(entorno_pub, monkeypatch):
    m = con_manifiesto(monkeypatch, entorno_pub, sucio=False)
    pedidos = []

    def regenerador(sha, fecha):
        pedidos.append((sha, fecha))
        return copy.deepcopy(m)

    assert reproducir.reproducir(DIA, regenerador=regenerador) == (True, [])
    assert pedidos == [(m["git"]["sha"], DIA)]  # se regenera en el commit del manifiesto

    def distinto(sha, fecha):
        r = copy.deepcopy(m)
        r["pronostico"]["hash_contenido"] = "distinto"
        return r

    ok, dif = reproducir.reproducir(DIA, regenerador=distinto)
    assert not ok and dif[0].startswith("pronostico.hash_contenido")
