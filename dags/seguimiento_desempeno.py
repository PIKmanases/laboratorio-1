"""Diario: desempeño de los pronósticos y pedidos anteriores con las ventas conocidas.

Espera la validación del día y que existan los pronósticos previos. Reprocesa la
partición de desempeño de un pronóstico cuando llegan más días observados o una
revisión de un día ya evaluado.
"""

from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import DAG

from laboratorio import tareas
from laboratorio.orquestacion import parametros_dag, parametros_sensor

FECHA = {"fecha": "{{ ds }}"}

with DAG(dag_id="seguimiento_desempeno", doc_md=__doc__, **parametros_dag("seguimiento_desempeno")):
    esperar_validacion = ExternalTaskSensor(
        task_id="esperar_validacion",
        external_dag_id="ingesta_validacion",
        external_task_id="validar",
        **parametros_sensor(),
    )
    esperar_pronosticos = PythonSensor(
        task_id="esperar_pronosticos_previos",
        python_callable=tareas.pronosticos_previos_listos,
        op_kwargs=FECHA,
        **parametros_sensor(),
    )
    seguir = PythonOperator(task_id="seguir_desempeno", python_callable=tareas.seguir_desempeno, op_kwargs=FECHA)

    [esperar_validacion, esperar_pronosticos] >> seguir
