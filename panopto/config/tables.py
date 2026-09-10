"""Módulo tables con la(s) clase(s) ProcessConfig."""

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict


@dataclass
class ProcessConfig:
    """Clase de datos que representa ProcessConfig."""

    model_summary_table: str = "gcprmsbx_work.panopto_model_summary_csi_psi"
    variable_metadata_table: str = "gcprmsbx_work.panopto_variable_metadata"
    model_table_config_table: str = "gcprmsbx_work.panopto_model_table_config"
    email_config_table: str = "gcprmsbx_work.panopto_email_config"
    csi_psi_table: str = "gcprmsbx_work.panopto_csi_psi_table"
    thresholds_table: str = "gcprmsbx_work.panopto_tresholds_table"
    alert_policy_table: str = "gcprmsbx_work.panopto_alert_policy"
    category_policy_table: str = "gcprmsbx_work.panopto_category_policy"
    config_changelog_table: str = "gcprmsbx_work.panopto_config_changelog"
    category_baseline_rank_table: str = "gcprmsbx_work.panopto_category_baseline_rank"
    metric_threshold_auto_table: str = "gcprmsbx_work.panopto_metric_threshold_auto"
    metric_result_table: str = "gcprmsbx_work.panopto_metric_result"
    alert_aggregate_table: str = "gcprmsbx_work.panopto_alert_aggregate"
    execution_log_table: str = "gcprmsbx_work.panopto_execution_log"
    email_log_table: str = "gcprmsbx_work.panopto_email_log"
    staging_control_table: str = "gcprmsbx_work.panopto_staging_control"
    variable_summary_table: str = "gcprmsbx_work.panopto_variable_summary"
    banamex_calendar_table: str = "gcprmsbx_work.panopto_banamex_calendar"
    external_banamex_calendar_table: str = "gcprmsbx_work.panopto_banamex_calendar_ext_d"

    banamex_calendar_sync_table: str = "banamex_calendar_sync_d"
    model_contact_table: str = "model_contact"
    red_alert_list_table: str = "red_alert_list_d"

    hdfs_staging_base: str = "/tmp/panopto/staging"
    hive_warehouse_dir: str = "/user/hive/warehouse"

    @classmethod
    def from_json(cls, path: str = None) -> "ProcessConfig":
        """
        Carga la configuración de tablas y rutas desde un archivo JSON.

        Args:
            path: ruta a ``tables.json``. Si es ``None`` se resuelve
                ``config/tables.json`` relativo a la raíz del repo.

        Las variables de entorno ``PANOPTO_HDFS_STAGING_BASE`` y
        ``PANOPTO_HIVE_WAREHOUSE_DIR`` tienen prioridad sobre los valores del JSON.
        """
        if path is None:
            repo_root = Path(__file__).resolve().parents[2]
            path = repo_root / "config" / "tables.json"
        else:
            path = Path(path)

        data: Dict[str, Any] = {}
        if path.exists():
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)

        if "hdfs_staging_base" in data:
            data["hdfs_staging_base"] = os.getenv("PANOPTO_HDFS_STAGING_BASE", data["hdfs_staging_base"])
        else:
            data["hdfs_staging_base"] = os.getenv("PANOPTO_HDFS_STAGING_BASE", cls.hdfs_staging_base)

        if "hive_warehouse_dir" in data:
            data["hive_warehouse_dir"] = os.getenv("PANOPTO_HIVE_WAREHOUSE_DIR", data["hive_warehouse_dir"])
        else:
            data["hive_warehouse_dir"] = os.getenv("PANOPTO_HIVE_WAREHOUSE_DIR", cls.hive_warehouse_dir)

        field_names = {f.name for f in cls.__dataclass_fields__.values()}
        kwargs = {k: v for k, v in data.items() if k in field_names}
        return cls(**kwargs)


PROCESS_CONFIG = ProcessConfig.from_json()
