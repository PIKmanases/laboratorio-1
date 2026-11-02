"""Los DAGs solo orquestan: sin lógica propia, solo llaman funciones de src/laboratorio."""

import ast
from pathlib import Path

import pytest

from laboratorio import orquestacion, tareas
from laboratorio.config import cargar_config

DIR_DAGS = Path(__file__).resolve().parents[1] / "dags"
DAGS = sorted(DIR_DAGS.glob("*.py"))
PROHIBIDO = (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda, ast.For, ast.While, ast.If,
             ast.ListComp, ast.DictComp, ast.SetComp, ast.GeneratorExp, ast.Try)


def test_hay_un_dag_por_cada_entrada_de_config():
    assert {p.stem for p in DAGS} == set(cargar_config()["orquestacion"]["dags"])


@pytest.mark.parametrize("ruta", DAGS, ids=lambda p: p.stem)
def test_dag_sin_logica(ruta):
    arbol = ast.parse(ruta.read_text(encoding="utf-8"))
    nodos = [n for n in ast.walk(arbol) if isinstance(n, PROHIBIDO)]
    assert not nodos, f"{ruta.name}: lógica en el DAG ({[type(n).__name__ for n in nodos]}); muévela a src/"
    for n in ast.walk(arbol):
        if isinstance(n, ast.ImportFrom):
            assert n.module.split(".")[0] in {"airflow", "laboratorio"}, n.module
        if isinstance(n, ast.Import):
            pytest.fail(f"{ruta.name}: usar 'from ... import ...' de airflow o laboratorio")


@pytest.mark.parametrize("ruta", DAGS, ids=lambda p: p.stem)
def test_callables_son_tareas_de_src(ruta):
    arbol = ast.parse(ruta.read_text(encoding="utf-8"))
    callables = [n.value for n in ast.walk(arbol) if isinstance(n, ast.keyword) and n.arg == "python_callable"]
    assert callables, f"{ruta.name} no llama a ninguna tarea"
    for c in callables:
        assert isinstance(c, ast.Attribute) and isinstance(c.value, ast.Name) and c.value.id == "tareas", ast.unparse(c)
        assert callable(getattr(tareas, c.attr, None)), f"tareas.{c.attr} no existe"


@pytest.mark.parametrize("ruta", DAGS, ids=lambda p: p.stem)
def test_dag_id_igual_al_archivo(ruta):
    assert f'dag_id="{ruta.stem}"' in ruta.read_text(encoding="utf-8")
    assert f'parametros_dag("{ruta.stem}")' in ruta.read_text(encoding="utf-8")


def test_parametros_dag():
    diario = orquestacion.parametros_dag("ingesta_validacion")
    semanal = orquestacion.parametros_dag("pronostico_pedido")
    assert diario["schedule"] == "0 6 * * *"
    assert semanal["schedule"] == "0 6 * * 1"  # lunes
    assert diario["catchup"] is False and diario["max_active_runs"] == 1
    assert diario["default_args"]["depends_on_past"] is True  # el as-of exige procesar los días en orden
    assert diario["start_date"].tzinfo is not None


def test_dagbag_sin_errores():
    """Solo donde Airflow está instalado (contenedor): los DAGs se cargan sin errores."""
    pytest.importorskip("airflow")
    from airflow.dag_processing.dagbag import DagBag

    bag = DagBag(dag_folder=str(DIR_DAGS), include_examples=False)
    assert bag.import_errors == {}
    assert set(bag.dag_ids) == {p.stem for p in DAGS}
