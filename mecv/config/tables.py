"""Módulo tables con la(s) clase(s) ProcessConfig."""

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict


@dataclass
class ProcessConfig:
    """Clase de datos que representa ProcessConfig."""

    model_summary_table: str = "model_summary_csi_psi_d_t_d"
    variable_metadata_table: str = "variable_metadata_d_t_d"
    csi_psi_table: str = "csi_psi_table_d_t_d"
    thresholds_table: str = "tresholds_table_d_t_d"
    alert_policy_table: str = "alert_policy_d_t_d"
    category_policy_table: str = "category_policy_d_t_d"
    config_changelog_table: str = "config_changelog_d_t_d"
    category_baseline_rank_table: str = "category_baseline_rank_d_t_d"
    metric_threshold_auto_table: str = "metric_threshold_auto_d_t_d"
    metric_result_table: str = "mecv_metric_result_d_t_d"
    alert_aggregate_table: str = "mecv_alert_aggregate_d_t_d"
    execution_log_table: str = "mecv_execution_log_d_t_d"
    email_log_table: str = "mecv_email_log_d_t_d"
    staging_control_table: str = "mecv_staging_control_d_t_d"
    variable_summary_table: str = "mecv_variable_summary_d_t_d"
    banamex_calendar_table: str = "banamex_calendar_d_t_d"
    external_banamex_calendar_table: str = "banamex_calendar_ext_d"

    banamex_calendar_sync_table: str = "banamex_calendar_sync_d"
    model_contact_table: str = "model_contact_d_t_d"
    red_alert_list_table: str = "red_alert_list_d"

    hdfs_staging_base: str = "/tmp/mecv/staging"
    hive_warehouse_dir: str = "/user/hive/warehouse"

    @classmethod
    def from_json(cls, path: str = None) -> "ProcessConfig":
        """
        Carga la configuración de tablas y rutas desde un archivo JSON.

        Args:
            path: ruta a ``tables.json``. Si es ``None`` se resuelve
                ``config/tables.json`` relativo a la raíz del repo.

        Las variables de entorno ``MECV_HDFS_STAGING_BASE`` y
        ``MECV_HIVE_WAREHOUSE_DIR`` tienen prioridad sobre los valores del JSON.
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
            data["hdfs_staging_base"] = os.getenv("MECV_HDFS_STAGING_BASE", data["hdfs_staging_base"])
        else:
            data["hdfs_staging_base"] = os.getenv("MECV_HDFS_STAGING_BASE", cls.hdfs_staging_base)

        if "hive_warehouse_dir" in data:
            data["hive_warehouse_dir"] = os.getenv("MECV_HIVE_WAREHOUSE_DIR", data["hive_warehouse_dir"])
        else:
            data["hive_warehouse_dir"] = os.getenv("MECV_HIVE_WAREHOUSE_DIR", cls.hive_warehouse_dir)

        field_names = {f.name for f in cls.__dataclass_fields__.values()}
        kwargs = {k: v for k, v in data.items() if k in field_names}
        return cls(**kwargs)


PROCESS_CONFIG = ProcessConfig.from_json()
