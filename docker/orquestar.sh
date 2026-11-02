#!/usr/bin/env bash
# Servicio de un solo uso del docker-compose: deja el flujo corriendo sin intervención manual.
#   1. espera a que Airflow haya cargado los DAGs y los activa (nacen en pausa);
#   2. crea un backfill por DAG para todo el periodo de config/pipeline.yaml (`orquestacion`).
# Es idempotente: si se vuelve a ejecutar, solo se reprocesan las corridas fallidas (--reprocess-behavior failed).
#
# Variables opcionales (para probar con pocos días):
#   ORQUESTAR_INICIO / ORQUESTAR_FIN   periodo a procesar (AAAA-MM-DD); por defecto el de la config
#   ORQUESTAR_DAGS                     DAGs separados por espacio; por defecto los 4, en orden
set -euo pipefail

DAGS=(${ORQUESTAR_DAGS:-ingesta_validacion preparacion_features pronostico_pedido seguimiento_desempeno})
leer() { python -c "from laboratorio.config import cargar_config as c; print(c()['orquestacion']['$1'])"; }
INICIO=${ORQUESTAR_INICIO:-$(leer inicio)}
FIN=${ORQUESTAR_FIN:-$(leer fin)}
# Los DAGs corren a las 06:00 UTC: el backfill necesita el día completo (con solo AAAA-MM-DD el último día queda fuera).
DESDE="${INICIO}T00:00:00+00:00"
HASTA="${FIN}T23:59:59+00:00"
API=http://airflow-apiserver:8080

token() {
  curl -fsS -X POST "$API/auth/token" -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}' \
    | python -c 'import sys, json; print(json.load(sys.stdin)["access_token"])'
}

# Un backfill sin corridas por crear queda "abierto" y bloquea los siguientes del mismo DAG: se cancela.
cancelar_abiertos() {
  local dag=$1 t id
  t=$(token)
  for id in $(curl -fsS "$API/api/v2/backfills?dag_id=$dag" -H "Authorization: Bearer $t" \
      | python -c 'import sys, json; print(*[b["id"] for b in json.load(sys.stdin)["backfills"] if not b["completed_at"]])'); do
    curl -fsS -X PUT "$API/api/v2/backfills/$id/cancel" -H "Authorization: Bearer $t" >/dev/null
  done
}

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
  salida=$(airflow backfill create --dag-id "$dag" --from-date "$DESDE" --to-date "$HASTA" \
    --reprocess-behavior failed --max-active-runs 1 2>&1) && { echo "[orquestar]   $dag: backfill creado"; continue; } || true
  if grep -q "No runs to create" <<<"$salida"; then
    cancelar_abiertos "$dag"
    echo "[orquestar]   $dag: nada pendiente (todas las corridas ya están en verde)"
  elif grep -q "AlreadyRunningBackfill" <<<"$salida"; then
    echo "[orquestar]   $dag: ya hay un backfill en curso; no se crea otro"
  else
    echo "$salida" | tail -15
    echo "[orquestar] falló la creación del backfill de $dag"
    exit 1
  fi
done
echo "[orquestar] listo: el avance se ve en http://localhost:8080"
