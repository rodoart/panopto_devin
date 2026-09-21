"""Módulo para refrescar el ticket Kerberos de Spark/Hive."""

import shutil
import subprocess
from typing import Optional

from panopto.config import Settings
from panopto.logging import get_logger

logger = get_logger(__name__)


def refresh_ticket(keytab: Optional[str] = None, principal: Optional[str] = None) -> bool:
    """Ejecuta kinit con keytab si está configurado.

    Lee de PANOPTO_KINIT_KEYTAB y PANOPTO_KINIT_PRINCIPAL por defecto.
    Devuelve True si se ejecutó correctamente o no estaba configurado.
    """
    settings = Settings.from_env()
    keytab = keytab or getattr(settings, "kinit_keytab", None)
    principal = principal or getattr(settings, "kinit_principal", None)

    if not keytab or not principal:
        logger.debug("kerberos ticket refresh skipped: no keytab/principal configured")
        return True

    kinit = shutil.which("kinit")
    if not kinit:
        logger.warning("kinit not found in PATH; skipping kerberos refresh")
        return False

    cmd = [kinit, "-kt", keytab, principal]
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
        logger.info(f"kerberos ticket refreshed for {principal}")
        return True
    except subprocess.CalledProcessError as exc:
        logger.error(f"kinit failed: {exc.stderr.strip()}")
        return False
