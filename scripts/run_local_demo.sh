#!/usr/bin/env bash
# Corre el ejemplo mínimo local (scripts/local_smoke.py) dentro del venv local.
#
# Uso:
#   scripts/run_local_demo.sh
#   scripts/run_local_demo.sh --master local[4] --samples-dir samples/sources
set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/local_common.sh"

panopto_ensure_venv
panopto_setup_java

export PYSPARK_PYTHON="$PANOPTO_LOCAL_VENV/bin/python"
export PYSPARK_DRIVER_PYTHON="$PANOPTO_LOCAL_VENV/bin/python"

cd "$PANOPTO_ROOT"
exec "$PANOPTO_LOCAL_VENV/bin/python" "$PANOPTO_ROOT/scripts/local_smoke.py" "$@"
