#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ROOT_DIR}/rca_app/.env"

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "Missing ${ENV_FILE}. Copy rca_app/.env.example to rca_app/.env and fill values first."
  exit 1
fi

# Load .env into current shell
set -a
source "${ENV_FILE}"
set +a

PROFILE="${1:-DEFAULT}"
TARGET="${2:-prod}"

databricks bundle deploy \
  --profile "${PROFILE}" \
  --target "${TARGET}" \
  --var "catalog=${CATALOG:-}" \
  --var "schema=${SCHEMA:-}" \
  --var "table=${TABLE:-}" \
  --var "volume=${VOLUME:-}" \
  --var "vs_index=${VS_INDEX:-}" \
  --var "app_name=${APP_NAME:-}" \
  --var "lakebase_instance_name=${LAKEBASE_INSTANCE_NAME:-}" \
  --var "serving_endpoint=${SERVING_ENDPOINT:-}" \
  --var "databricks_token=${DATABRICKS_TOKEN:-}" \
  --var "warehouse_id=${DATABRICKS_WAREHOUSE_ID:-}" \
  --var "genie_space_id=${GENIE_SPACE_ID:-}" \
  --var "mailgun_api_url=${MAILGUN_API_URL:-}" \
  --var "sender=${SENDER:-}" \
  --var "recipient=${RECIPIENT:-}"
