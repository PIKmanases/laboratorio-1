"""Diario: llega la bandeja del día, se ingesta a bronze y se valida a silver.

Las revisiones (`_rev2`) que llegan se ingestan y validan como revisiones nuevas
de su partición; los DAGs siguientes las toman as-of.
"""

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from laboratorio import tareas
from laboratorio.orquestacion import parametros_dag

FECHA = {"fecha": "{{ ds }}"}

with DAG(dag_id="ingesta_validacion", doc_md=__doc__, **parametros_dag("ingesta_validacion")):
    recibir = PythonOperator(task_id="recibir_bandeja", python_callable=tareas.recibir_bandeja, op_kwargs=FECHA)
    ingestar = PythonOperator(task_id="ingestar", python_callable=tareas.ingestar, op_kwargs=FECHA)
    validar = PythonOperator(task_id="validar", python_callable=tareas.validar, op_kwargs=FECHA)

    recibir >> ingestar >> validar
