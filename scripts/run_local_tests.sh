#!/usr/bin/env bash
# Corre la suite de pytest dentro del ambiente virtual LOCAL.
#
# Uso:
#   scripts/run_local_tests.sh                      # suite completa en modo local
#   scripts/run_local_tests.sh tests/test_binning.py -v
#   PANOPTO_TEST_MODE=auto scripts/run_local_tests.sh
#
# Solo exporta variables dentro de este proceso; `pytest tests/` regular
# (modo cluster/conda) no se ve afectado.
set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/local_common.sh"

panopto_ensure_venv
panopto_setup_java

VENV_PY="$(panopto_venv_python)"

export PANOPTO_TEST_MODE="${PANOPTO_TEST_MODE:-local}"
export PYSPARK_PYTHON="$VENV_PY"
export PYSPARK_DRIVER_PYTHON="$VENV_PY"

echo "panopto: modo local (PANOPTO_TEST_MODE=$PANOPTO_TEST_MODE), venv=$PANOPTO_LOCAL_VENV"
cd "$PANOPTO_ROOT"
exec "$VENV_PY" -m pytest ${PANOPTO_LOCAL_PYTEST_ARGS:-tests/ -q} "$@"
