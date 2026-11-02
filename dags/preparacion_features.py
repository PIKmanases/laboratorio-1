"""Diario: predictores as-of del corte del día (espera la validación de ese día).

La primera corrida también prepara los cortes históricos que falten para entrenar.
"""

from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk import DAG

from laboratorio import tareas
from laboratorio.orquestacion import parametros_dag, parametros_sensor

FECHA = {"fecha": "{{ ds }}"}

with DAG(dag_id="preparacion_features", doc_md=__doc__, **parametros_dag("preparacion_features")):
    esperar = ExternalTaskSensor(
        task_id="esperar_validacion",
        external_dag_id="ingesta_validacion",
        external_task_id="validar",
        **parametros_sensor(),
    )
    preparar = PythonOperator(task_id="preparar_features", python_callable=tareas.preparar_features, op_kwargs=FECHA)

    esperar >> preparar
