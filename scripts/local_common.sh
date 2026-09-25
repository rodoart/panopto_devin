#!/usr/bin/env bash
# Funciones comunes para el modo de pruebas LOCAL de PANOPTO.
# Se carga con `source` desde setup_local_env.sh / run_local_tests.sh /
# run_local_demo.sh. No ejecuta nada por sí mismo.
#
# Variables parametrizables (todas opcionales):
#   PANOPTO_LOCAL_VENV          Ruta del virtualenv local. Default: <repo>/.venv-local
#   PANOPTO_LOCAL_PYTHON        Binario de Python para el venv. Default: autodetecta
#                               python3.10/3.9/3.8/3 (PySpark 3.3 soporta <=3.10).
#   PANOPTO_LOCAL_REQUIREMENTS  Requirements a instalar. Default: requirements-local.txt
#   PANOPTO_LOCAL_PYTEST_ARGS   Args base de pytest en modo local. Default: "tests/ -q"
#   JAVA_HOME                   JDK para Spark. Default: autodetecta 17/11/8.

PANOPTO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PANOPTO_LOCAL_VENV="${PANOPTO_LOCAL_VENV:-$PANOPTO_ROOT/.venv-local}"
PANOPTO_LOCAL_REQUIREMENTS="${PANOPTO_LOCAL_REQUIREMENTS:-$PANOPTO_ROOT/requirements-local.txt}"

panopto_pick_python() {
    if [[ -n "${PANOPTO_LOCAL_PYTHON:-}" ]]; then
        echo "$PANOPTO_LOCAL_PYTHON"
        return 0
    fi
    local candidate
    for candidate in python3.10 python3.9 python3.8 python3; do
        if command -v "$candidate" >/dev/null 2>&1; then
            command -v "$candidate"
            return 0
        fi
    done
    echo "panopto: no se encontró un intérprete Python (se buscó 3.10/3.9/3.8/3)" >&2
    return 1
}

panopto_setup_java() {
    # Elige un JDK compatible con Spark y, si es >= 16, exporta los --add-opens
    # requeridos por Spark 3.3 (fuerte encapsulamiento de módulos del JDK).
    if [[ -z "${JAVA_HOME:-}" ]]; then
        local jdk
        for jdk in /usr/lib/jvm/java-17-openjdk-amd64 \
                   /usr/lib/jvm/java-11-openjdk-amd64 \
                   /usr/lib/jvm/java-8-openjdk-amd64 \
                   /usr/lib/jvm/default-java; do
            if [[ -d "$jdk" ]]; then
                JAVA_HOME="$jdk"
                break
            fi
        done
        export JAVA_HOME
    fi

    local java_bin="${JAVA_HOME:+$JAVA_HOME/bin/java}"
    java_bin="${java_bin:-$(command -v java 2>/dev/null || true)}"
    [[ -n "$java_bin" ]] || return 0

    local major
    major="$("$java_bin" -version 2>&1 | head -1 | sed -E 's/.*version "(1\.)?([0-9]+).*/\2/')"
    if [[ "${major:-0}" -ge 16 ]]; then
        export JDK_JAVA_OPTIONS="${JDK_JAVA_OPTIONS:-} --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED -Dio.netty.tryReflectionSetAccessible=true"
    fi
}

panopto_ensure_venv() {
    # Crea el venv local si no existe todavía.
    if [[ ! -x "$PANOPTO_LOCAL_VENV/bin/python" ]]; then
        echo "panopto: venv local no encontrado en $PANOPTO_LOCAL_VENV; creándolo..."
        "$PANOPTO_ROOT/scripts/setup_local_env.sh"
    fi
}
