# Diccionario de tablas PANOPTO


Esta guía describe las tablas de configuración, resultados y logs del framework PANOPTO. La **ruta Spark** es `gcprmsbx_work.panopto_<nombre_corto>`.

## `model_table_config`

Ruta Spark: `gcprmsbx_work.panopto_model_table_config`


Configuración a nivel tabla (conexión, llaves, particiones, transformación, ventana histórica, reading_mode).


| Campo | Tipo | Descripción |
|-------|------|-------------|
| `table_role` | string | Rol: raw, input, processed, score, target |
| `table_name` | string | Alias corto de la tabla |
| `source_type` | string | HIVE o PARQUET |
| `source_schema` | string | Esquema Hive o None |
| `source_table` | string | Nombre físico con prefijo hive: o parquet: |
| `entity_key_columns` | string | JSON con llaves de la fuente |
| `canonical_key_columns` | string | JSON con llaves unificadas |
| `date_column` | string | Columna de fecha de información |
| `date_format` | string | Formato strptime de la fecha, ej. %Y-%m-%d |
| `history_months` | int | Ventana histórica para baseline |
| `lag` | int | Meses de lag del baseline |
| `sql_transform` | string | Transformación SQL a aplicar en selectExpr |
| `data_type` | string | numeric o categorical |
| `partition_columns` | string | JSON con columnas de partición |
| `reading_mode` | string | each, first o last |
| `active` | boolean | True si la configuración está activa |
| `process_date` | string |  |
| `model_id` | string |  |

---

## `variable_metadata`

Ruta Spark: `gcprmsbx_work.panopto_variable_metadata`


Catálogo de variables por modelo. Relaciona cada variable con su tabla fuente a través de `source_table`.


| Campo | Tipo | Descripción |
|-------|------|-------------|
| `variable` | string | Nombre de la variable |
| `var_type` | string | Rol: raw, input, transformed, score, target |
| `data_type` | string | numeric o categorical |
| `source_table` | string | Nombre de tabla en table metadata (clave cruzada) |
| `source_column` | string | Columna física en la tabla fuente |
| `is_monotonic` | boolean | True si la variable es monótona |
| `process_date` | string |  |
| `model_id` | string |  |

---

## `email_config`

Ruta Spark: `gcprmsbx_work.panopto_email_config`


Configuración del remitente y prefijo de correos. Se usa `model_id=global` para remitentes globales.


| Campo | Tipo | Descripción |
|-------|------|-------------|
| `sender_name` | string | Nombre del remitente |
| `sender_email` | string | Dirección del remitente |
| `reply_to` | string | Reply-To |
| `subject_prefix` | string | Prefijo del asunto |
| `logo_url` | string | URL del logo |
| `active` | boolean | True si la config está activa |
| `process_date` | string |  |
| `model_id` | string |  |

---

## `model_summary_csi_psi`

Ruta Spark: `gcprmsbx_work.panopto_model_summary_csi_psi`


Resumen del modelo: nombre, tipo, cut_off, frecuencia, ventana, día de ejecución y desfase de target.


| Campo | Tipo | Descripción |
|-------|------|-------------|
| `model_name` | string |  |
| `model_description` | string |  |
| `model_type` | string |  |
| `status` | string |  |
| `cut_off_probability` | double |  |
| `frequency` | string |  |
| `window_value` | int |  |
| `window_unit` | string |  |
| `trigger_csi_ambar` | double |  |
| `trigger_csi_red` | double |  |
| `trigger_csi_variation_ambar` | double |  |
| `trigger_csi_variation_red` | double |  |
| `score_alert_ambar_pct` | double |  |
| `score_alert_red_pct` | double |  |
| `score_red_equivalent` | int |  |
| `execution_monthly_day` | int | Día del mes (1-31) en que se espera la carga para frecuencia mensual |
| `execution_weekday` | int | Día de la semana (0=Lunes) en que se espera la carga para frecuencia semanal |
| `target_lag_months` | int | Meses de desfase con los que la target está disponible |
| `process_date` | string |  |
| `model_id` | string |  |

---

## `tresholds_table`

Ruta Spark: `gcprmsbx_work.panopto_tresholds_table`


Umbrales manuales de PSI por variable y tipo.


| Campo | Tipo | Descripción |
|-------|------|-------------|
| `variable` | string |  |
| `type` | string |  |
| `psi_threshold_ambar` | double |  |
| `psi_threshold_red` | double |  |
| `psi_variation_threshold_ambar` | double |  |
| `psi_variation_threshold_red` | double |  |
| `process_date` | string |  |
| `model_id` | string |  |

---

## `alert_policy`

Ruta Spark: `gcprmsbx_work.panopto_alert_policy`


Política de agregación de alertas por `var_type`.


| Campo | Tipo | Descripción |
|-------|------|-------------|
| `var_type` | string |  |
| `red_equivalent` | int |  |
| `alert_ambar_pct` | double |  |
| `alert_red_pct` | double |  |
| `process_date` | string |  |
| `model_id` | string |  |

---

## `category_policy`

Ruta Spark: `gcprmsbx_work.panopto_category_policy`


Política de categorías: top_n_threshold y critical_top_k.


| Campo | Tipo | Descripción |
|-------|------|-------------|
| `variable` | string |  |
| `top_n_threshold` | int |  |
| `critical_top_k` | int |  |
| `process_date` | string |  |
| `model_id` | string |  |

---

## `csi_psi_table`

Ruta Spark: `gcprmsbx_work.panopto_csi_psi_table`


Bins de referencia para métricas PSI/CSI.


| Campo | Tipo | Descripción |
|-------|------|-------------|
| `schema` | string |  |
| `table_name` | string |  |
| `type` | string |  |
| `variable` | string |  |
| `bin` | int |  |
| `bin_type` | string |  |
| `category_value` | string |  |
| `lb` | double |  |
| `ub` | double |  |
| `lower_bound_type` | string |  |
| `upper_bound_type` | string |  |
| `count_dev` | int |  |
| `woe` | double |  |
| `information_date_column` | string |  |
| `process_date` | string |  |
| `model_id` | string |  |

---

## `metric_threshold_auto`

Ruta Spark: `gcprmsbx_work.panopto_metric_threshold_auto`


Umbrales calculados automáticamente en entrenamiento.


| Campo | Tipo | Descripción |
|-------|------|-------------|
| `variable` | string |  |
| `metric_name` | string |  |
| `threshold_ambar` | double |  |
| `threshold_red` | double |  |
| `baseline_value` | double |  |
| `baseline_std` | double |  |
| `sample_size_dev` | int |  |
| `calculation_method` | string |  |
| `process_date` | string |  |
| `model_id` | string |  |

---

## `category_baseline_rank`

Ruta Spark: `gcprmsbx_work.panopto_category_baseline_rank`


Ranking de categorías del baseline.


| Campo | Tipo | Descripción |
|-------|------|-------------|
| `variable` | string |  |
| `category_value` | string |  |
| `rank_dev` | int |  |
| `freq_dev` | double |  |
| `top_n_threshold` | int |  |
| `critical_top_k` | int |  |
| `process_date` | string |  |
| `model_id` | string |  |

---

## `banamex_calendar`

Ruta Spark: `gcprmsbx_work.panopto_banamex_calendar`


Calendario de días hábiles.


| Campo | Tipo | Descripción |
|-------|------|-------------|
| `calendar_date` | string |  |
| `is_business_day` | boolean |  |
| `is_holiday` | boolean |  |
| `holiday_name` | string |  |
| `sync_timestamp` | timestamp |  |

---

## `alert_aggregate`

Ruta Spark: `gcprmsbx_work.panopto_alert_aggregate`


Agregación de alertas por `var_type` y fecha.


| Campo | Tipo | Descripción |
|-------|------|-------------|
| `execution_id` | string |  |
| `var_type` | string |  |
| `total_metrics` | int |  |
| `count_ambar` | int |  |
| `count_red` | int |  |
| `equivalent_yellow` | double |  |
| `stress_ratio` | double |  |
| `aggregate_status` | string |  |
| `alert_sent` | boolean |  |
| `alert_type` | string |  |
| `red_equivalent_used` | int |  |
| `alert_ambar_pct_used` | double |  |
| `alert_red_pct_used` | double |  |
| `run_date` | timestamp |  |
| `information_date` | string |  |
| `model_id` | string |  |

---

## `config_changelog`

Ruta Spark: `gcprmsbx_work.panopto_config_changelog`


Auditoría de cambios de configuración.


| Campo | Tipo | Descripción |
|-------|------|-------------|
| `change_timestamp` | timestamp |  |
| `table_name` | string |  |
| `change_type` | string |  |
| `field_changed` | string |  |
| `old_value` | string |  |
| `new_value` | string |  |
| `triggered_retraining` | boolean |  |
| `error_message` | string |  |
| `executed_by_dag_id` | string |  |
| `run_id` | string |  |
| `process_date` | string |  |
| `model_id` | string |  |

---

## `email_log`

Ruta Spark: `gcprmsbx_work.panopto_email_log`


Log de correos enviados.


| Campo | Tipo | Descripción |
|-------|------|-------------|
| `email_id` | string |  |
| `execution_id` | string |  |
| `alert_type` | string |  |
| `recipients_to` | string |  |
| `recipients_bcc` | string |  |
| `subject` | string |  |
| `body_summary` | string |  |
| `sent_timestamp` | timestamp |  |
| `status` | string |  |
| `smtp_response` | string |  |
| `retry_count` | int |  |
| `information_date` | string |  |
| `model_id` | string |  |

---

## `execution_log`

Ruta Spark: `gcprmsbx_work.panopto_execution_log`


Log de ejecución de cada corrida.


| Campo | Tipo | Descripción |
|-------|------|-------------|
| `execution_id` | string |  |
| `dag_id` | string |  |
| `airflow_run_id` | string |  |
| `run_date` | timestamp |  |
| `end_date` | timestamp |  |
| `status` | string |  |
| `error_message` | string |  |
| `reason` | string |  |
| `variables_expected` | int |  |
| `variables_processed` | int |  |
| `variables_missing` | int |  |
| `metrics_calculated` | int |  |
| `metrics_failed` | int |  |
| `duration_seconds` | int |  |
| `information_date` | string |  |
| `model_id` | string |  |

---

## `metric_result`

Ruta Spark: `gcprmsbx_work.panopto_metric_result`


Resultado de cada métrica ejecutada.


| Campo | Tipo | Descripción |
|-------|------|-------------|
| `execution_id` | string |  |
| `variable` | string |  |
| `var_type` | string |  |
| `metric_name` | string |  |
| `metric_value` | double |  |
| `baseline_value` | double |  |
| `threshold_ambar` | double |  |
| `threshold_red` | double |  |
| `status` | string |  |
| `baseline_process_date` | string |  |
| `run_date` | timestamp |  |
| `dag_id` | string |  |
| `airflow_run_id` | string |  |
| `information_date` | string |  |
| `model_id` | string |  |

---

## `staging_control`

Ruta Spark: `gcprmsbx_work.panopto_staging_control`


Control de staging atómico de parquet.


| Campo | Tipo | Descripción |
|-------|------|-------------|
| `staging_id` | string |  |
| `execution_id` | string |  |
| `model_id` | string |  |
| `target_table` | string |  |
| `information_date` | string |  |
| `temp_path` | string |  |
| `final_path` | string |  |
| `row_count_temp` | int |  |
| `row_count_final` | int |  |
| `status` | string |  |
| `started_at` | timestamp |  |
| `validated_at` | timestamp |  |
| `promoted_at` | timestamp |  |
| `process_date` | string |  |

---

## `variable_summary`

Ruta Spark: `gcprmsbx_work.panopto_variable_summary`


Estadísticos descriptivos de cada variable.


| Campo | Tipo | Descripción |
|-------|------|-------------|
| `execution_id` | string |  |
| `variable` | string |  |
| `var_type` | string |  |
| `data_type` | string |  |
| `statistic` | string |  |
| `statistic_value` | double |  |
| `statistic_value_str` | string |  |
| `information_date` | string |  |
| `model_id` | string |  |

---
