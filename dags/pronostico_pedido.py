"""Semanal: pronóstico del horizonte y pedido (espera los features del día)."""

from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk import DAG

from laboratorio import tareas
from laboratorio.orquestacion import parametros_dag, parametros_sensor

FECHA = {"fecha": "{{ ds }}"}

with DAG(dag_id="pronostico_pedido", doc_md=__doc__, **parametros_dag("pronostico_pedido")):
    esperar = ExternalTaskSensor(
        task_id="esperar_features",
        external_dag_id="preparacion_features",
        external_task_id="preparar_features",
        **parametros_sensor(),
    )
    pronosticar = PythonOperator(task_id="pronosticar", python_callable=tareas.pronosticar, op_kwargs=FECHA)
    pedir = PythonOperator(task_id="generar_pedido", python_callable=tareas.generar_pedido, op_kwargs=FECHA)

    esperar >> pronosticar >> pedir
