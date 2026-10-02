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

if ! panopto_python_supported "$PYTHON"; then
    echo "panopto: ERROR: $PYTHON no es Python 3.8-3.11 (PySpark 3.3 no soporta 3.12+)." >&2
    echo "panopto: usa PANOPTO_LOCAL_PYTHON=/ruta/a/python3.11 para forzar otro intérprete." >&2
    exit 1
fi

# Un venv existente con otra versión de Python deja site-packages incompatible;
# si el intérprete del venv difiere, hay que reconstruirlo desde cero.
EXISTING_PY="$(panopto_venv_python)"
if [[ -x "$EXISTING_PY" ]] && ! panopto_python_supported "$EXISTING_PY"; then
    echo "panopto: el venv existente usa un Python no soportado; reconstruyendo..."
    rm -rf "$PANOPTO_LOCAL_VENV"
fi

"$PYTHON" -m venv "$PANOPTO_LOCAL_VENV"
VENV_PY="$(panopto_venv_python)"
"$VENV_PY" -m pip install --upgrade pip
"$VENV_PY" -m pip install -r "$PANOPTO_LOCAL_REQUIREMENTS"
# Instala el paquete panopto en modo editable para que `import panopto` funcione
# desde cualquier cwd dentro del venv.
"$VENV_PY" -m pip install -e "$PANOPTO_ROOT" --no-deps

echo "panopto: ambiente local listo. Actívalo con: source $PANOPTO_LOCAL_VENV/$(panopto_venv_bin)/activate"
