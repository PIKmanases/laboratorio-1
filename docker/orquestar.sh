#!/usr/bin/env bash
# Servicio de un solo uso del docker-compose: deja el flujo corriendo sin intervención manual.
#   1. espera a que Airflow haya cargado los DAGs y los activa (nacen en pausa);
#   2. crea un backfill por DAG para todo el periodo de config/pipeline.yaml (`orquestacion`).
# Es idempotente: si se vuelve a ejecutar, solo se reprocesan las corridas fallidas (--reprocess-behavior failed).
set -euo pipefail

DAGS=(ingesta_validacion preparacion_features pronostico_pedido seguimiento_desempeno)
leer() { python -c "from laboratorio.config import cargar_config as c; print(c()['orquestacion']['$1'])"; }
INICIO=$(leer inicio)
FIN=$(leer fin)

echo "[orquestar] activando DAGs"
for dag in "${DAGS[@]}"; do
  for intento in $(seq 1 60); do  # el dag-processor puede tardar en registrarlos
    if airflow dags unpause "$dag" >/dev/null 2>&1; then break; fi
    [ "$intento" -eq 60 ] && { echo "[orquestar] el DAG $dag no apareció en 5 min"; exit 1; }
    sleep 5
  done
  echo "[orquestar]   $dag activo"
done

echo "[orquestar] backfill $INICIO .. $FIN"
for dag in "${DAGS[@]}"; do
  airflow backfill create --dag-id "$dag" --from-date "$INICIO" --to-date "$FIN" \
    --reprocess-behavior failed --max-active-runs 1
done
echo "[orquestar] listo: el avance se ve en http://localhost:8080"
