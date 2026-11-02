"""Parámetros de los DAGs de Airflow, leídos de `config/pipeline.yaml` (`orquestacion`).

No importa Airflow: los DAGs llaman a estas funciones para obtener `schedule`,
fechas, reintentos y parámetros de sensores.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from laboratorio.config import cargar_config


def _cron(o: dict, frecuencia: str) -> str:
    if frecuencia == "diaria":
        return f"0 {o['hora']} * * *"
    if frecuencia == "semanal":
        return f"0 {o['hora']} * * {(o['dia_semana_pronostico'] + 1) % 7}"  # cron: 0 = domingo
    raise ValueError(f"frecuencia desconocida {frecuencia!r}")


def _instante(o: dict, dia: str) -> datetime:
    return datetime.fromisoformat(dia).replace(hour=o["hora"], tzinfo=timezone.utc)


def parametros_dag(nombre: str, cfg: dict | None = None) -> dict:
    """Argumentos para `DAG(...)`: corridas solo por backfill (sin catchup) y una a la vez."""
    o = (cfg or cargar_config())["orquestacion"]
    d = o["dags"][nombre]
    return {
        "schedule": _cron(o, d["frecuencia"]),
        "start_date": _instante(o, o["inicio"]),
        "end_date": _instante(o, o["fin"]),
        "catchup": False,
        "max_active_runs": 1,
        "tags": ["laboratorio", d["frecuencia"]],
        "default_args": {
            "owner": "laboratorio",
            "depends_on_past": d["depends_on_past"],
            "retries": o["reintentos"],
            "retry_delay": timedelta(minutes=o["minutos_entre_reintentos"]),
        },
    }


def parametros_sensor(cfg: dict | None = None) -> dict:
    s = (cfg or cargar_config())["orquestacion"]["sensores"]
    return {"mode": "reschedule", "poke_interval": s["poke_interval_s"], "timeout": s["timeout_s"]}
