#!/usr/bin/env bash
# Crea el ambiente virtual LOCAL de PANOPTO e instala Spark (pyspark) vía pip.
#
# Uso:
#   scripts/setup_local_env.sh
#   PANOPTO_LOCAL_VENV=/tmp/venv PANOPTO_LOCAL_PYTHON=python3.10 scripts/setup_local_env.sh
#
# No interviene con el ambiente conda/cluster (environment.yml): todo queda
# contenido en PANOPTO_LOCAL_VENV (default .venv-local/).
set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/local_common.sh"

PYTHON="$(panopto_pick_python)"
panopto_setup_java

echo "panopto: python       = $PYTHON ($("$PYTHON" --version 2>&1))"
echo "panopto: JAVA_HOME    = ${JAVA_HOME:-<sin detectar>}"
echo "panopto: venv         = $PANOPTO_LOCAL_VENV"
echo "panopto: requirements = $PANOPTO_LOCAL_REQUIREMENTS"

"$PYTHON" -m venv "$PANOPTO_LOCAL_VENV"
"$PANOPTO_LOCAL_VENV/bin/pip" install --upgrade pip
"$PANOPTO_LOCAL_VENV/bin/pip" install -r "$PANOPTO_LOCAL_REQUIREMENTS"
# Instala el paquete panopto en modo editable para que `import panopto` funcione
# desde cualquier cwd dentro del venv.
"$PANOPTO_LOCAL_VENV/bin/pip" install -e "$PANOPTO_ROOT" --no-deps

echo "panopto: ambiente local listo. Actívalo con: source $PANOPTO_LOCAL_VENV/bin/activate"
