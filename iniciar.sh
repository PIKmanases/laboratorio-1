#!/usr/bin/env bash
# Levanta Airflow y deja el flujo completo corriendo, sin pasos manuales.
#   Requisitos: Docker con Compose, y el zip de Kaggle en data/raw/kaggle/.
set -euo pipefail
cd "$(dirname "$0")"

ZIP=data/raw/kaggle/store-sales-time-series-forecasting.zip
if [ ! -f "$ZIP" ]; then
  echo "Falta $ZIP"
  echo "Descarga el zip de https://www.kaggle.com/competitions/store-sales-time-series-forecasting/data (sin descomprimir) y cópialo ahí."
  exit 1
fi
esperado=$(awk '/md5:/ {print $3; exit}' "$ZIP.dvc")
real=$(md5sum "$ZIP" | cut -d' ' -f1)
if [ "$esperado" != "$real" ] && [ -z "${PERMITIR_ZIP_DISTINTO:-}" ]; then
  echo "El zip no coincide con el versionado en DVC (md5 $real, esperado $esperado)."
  echo "Los resultados no serían reproducibles. Para continuar igual: PERMITIR_ZIP_DISTINTO=1 ./iniciar.sh"
  exit 1
fi

mkdir -p data forecasts models
if [ ! -f .env ]; then
  cp .env.example .env
  sed -i "s/^AIRFLOW_UID=.*/AIRFLOW_UID=$(id -u)/" .env
  sed -i "s|^AIRFLOW_JWT_SECRET=.*|AIRFLOW_JWT_SECRET=$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')|" .env
  echo ".env creado"
fi

export LABORATORIO_GIT_SHA="$(git rev-parse HEAD 2>/dev/null || true)"
docker compose up -d --build
echo
echo "Airflow: http://localhost:8080 (sin login). El backfill ya está en marcha: ver README, sección 'Seguimiento'."
