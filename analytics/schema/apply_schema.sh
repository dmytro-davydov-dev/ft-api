#!/usr/bin/env bash
# Apply FLO-27 BQ schema DDL to a Flowterra environment.
# Usage:  ./apply_schema.sh [env]   (default: dev)
#   env = dev | demo | prod

set -euo pipefail

ENV="${1:-dev}"
PROJECT="flowterra-${ENV}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_FILE="${SCRIPT_DIR}/bq_schema.sql"

echo "Applying BQ schema to project=${PROJECT}  dataset=flowterra_${ENV}"

# Resolve {project} and {env} placeholders, then pipe to bq
sed \
  -e "s/{project}/${PROJECT}/g" \
  -e "s/{env}/${ENV}/g" \
  "${SQL_FILE}" \
| bq query \
    --project_id="${PROJECT}" \
    --use_legacy_sql=false \
    --nouse_cache

echo "Done. Tables in flowterra_${ENV}:"
bq ls --project_id="${PROJECT}" "flowterra_${ENV}"
