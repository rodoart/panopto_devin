"""Carga la configuración del ambiente LOCAL antes de importar ``panopto``.

Todo script del stack local hace al inicio::

    import local_env
    local_env.apply()

La fuente de verdad es ``.env.local`` (raíz del repo). Además fija
``HADOOP_HOME`` (winutils) y ``PYSPARK_PYTHON`` (el intérprete del venv local),
necesarios para Spark en Windows.
"""

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = ROOT / ".env.local"


def apply() -> None:
    """Carga ``.env.local`` y las variables de runtime del stack local."""
    try:
        from dotenv import load_dotenv

        if ENV_FILE.exists():
            load_dotenv(ENV_FILE, override=False)
    except ImportError:
        # Fallback sin python-dotenv: parseo simple KEY=VALUE.
        if ENV_FILE.exists():
            for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, value = line.partition("=")
                os.environ.setdefault(key.strip(), value.strip())

    venv_python = ROOT / ".venv-local" / "Scripts" / "python.exe"
    if not venv_python.exists():  # entorno POSIX
        venv_python = ROOT / ".venv-local" / "bin" / "python"
    os.environ.setdefault("PYSPARK_PYTHON", str(venv_python))
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", str(venv_python))
    hadoop_home = str(ROOT / ".local" / "hadoop")
    os.environ.setdefault("HADOOP_HOME", hadoop_home)
    # hadoop.dll/winutils.exe deben ser alcanzables por la JVM (PATH entra en
    # java.library.path por defecto en Windows).
    hadoop_bin = os.path.join(hadoop_home, "bin")
    if hadoop_bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")
    os.environ.setdefault("PANOPTO_DISABLE_EMAILS", "true")

    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
