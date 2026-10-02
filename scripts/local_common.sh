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

panopto_is_windows() {
    case "$(uname -s 2>/dev/null)" in
        MINGW*|MSYS*|CYGWIN*) return 0 ;;
        *) return 1 ;;
    esac
}

panopto_venv_bin() {
    # Layout del venv: "Scripts" en Windows, "bin" en POSIX.
    if panopto_is_windows || [[ -d "$PANOPTO_LOCAL_VENV/Scripts" ]]; then
        echo "Scripts"
    else
        echo "bin"
    fi
}

panopto_venv_python() {
    local bin
    bin="$(panopto_venv_bin)"
    local py="$PANOPTO_LOCAL_VENV/$bin/python"
    if [[ "$bin" == "Scripts" ]]; then py="$py.exe"; fi
    echo "$py"
}

panopto_python_supported() {
    # PySpark 3.3 soporta Python 3.8-3.11 (3.12+ rompe por distutils/numpy).
    local py="$1" ver
    ver="$("$py" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")' 2>/dev/null)" || return 1
    case "$ver" in
        3.8|3.9|3.10|3.11) return 0 ;;
        *) return 1 ;;
    esac
}

panopto_pick_python() {
    if [[ -n "${PANOPTO_LOCAL_PYTHON:-}" ]]; then
        echo "$PANOPTO_LOCAL_PYTHON"
        return 0
    fi
    # En Windows el launcher `py` resuelve versiones concretas (python3 suele ser
    # el stub de Microsoft Store, hoy 3.13, incompatible con PySpark 3.3).
    if command -v py >/dev/null 2>&1; then
        local minor exe
        for minor in 3.11 3.10 3.9 3.8; do
            exe="$(py "-$minor" -c 'import sys; print(sys.executable)' 2>/dev/null)" || continue
            if [[ -n "$exe" ]]; then
                echo "$exe"
                return 0
            fi
        done
    fi
    local candidate
    for candidate in python3.11 python3.10 python3.9 python3.8 python3 python; do
        if command -v "$candidate" >/dev/null 2>&1 && panopto_python_supported "$candidate"; then
            command -v "$candidate"
            return 0
        fi
    done
    echo "panopto: no se encontró un intérprete Python 3.8-3.11 (PySpark 3.3 no soporta 3.12+)" >&2
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
    # Crea el venv local si no existe todavía. Si existe pero con un Python
    # no soportado (p.ej. recreado con el stub 3.13 de Microsoft Store), lo
    # descarta y lo reconstruye — un venv mezclado importa binarios rotos.
    local venv_py
    venv_py="$(panopto_venv_python)"
    if [[ -x "$venv_py" ]] && panopto_python_supported "$venv_py"; then
        return 0
    fi
    if [[ -d "$PANOPTO_LOCAL_VENV" ]]; then
        echo "panopto: venv en $PANOPTO_LOCAL_VENV tiene un Python no soportado; reconstruyendo..."
        rm -rf "$PANOPTO_LOCAL_VENV"
    else
        echo "panopto: venv local no encontrado en $PANOPTO_LOCAL_VENV; creándolo..."
    fi
    "$PANOPTO_ROOT/scripts/setup_local_env.sh"
}
