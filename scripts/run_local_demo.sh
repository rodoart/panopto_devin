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

VENV_PY="$(panopto_venv_python)"

export PYSPARK_PYTHON="$VENV_PY"
export PYSPARK_DRIVER_PYTHON="$VENV_PY"

cd "$PANOPTO_ROOT"
exec "$VENV_PY" "$PANOPTO_ROOT/scripts/local_smoke.py" "$@"
