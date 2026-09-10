# Diccionario de tablas PANOPTO


Esta guía describe las tablas de configuración, resultados y logs del framework PANOPTO, junto con el nombre y tipo de cada campo y un ejemplo de fila.

## `panopto_model_table_config_d_t_d`

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

**Ejemplo de fila:**
- `process_date`: `2025-10-15`
- `model_id`: `1079_cta_lvl`
- `table_role`: `raw`
- `table_name`: `raw_1079`
- `source_type`: `HIVE`
- `source_schema`: `gcprmsbx_work`
- `source_table`: `hive:gcprmsbx_work.raw_1079`
- `entity_key_columns`: `["customer_id"]`
- `canonical_key_columns`: `["customer_id"]`
- `date_column`: `information_date`
- `date_format`: ``
- `history_months`: `3`
- `lag`: `0`
- `sql_transform`: `TRIM(customer_id) AS customer_id`
- `data_type`: ``
- `partition_columns`: `[]`
- `reading_mode`: `each`
- `active`: `true`

---

## `variable_metadata_d_t_d`

Catálogo de variables por modelo. Relaciona cada variable con su tabla fuente a través de `source_table` (debe coincidir con `source_table` en `panopto_model_table_config_d_t_d`).


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

**Ejemplo de fila:**
- `process_date`: `2025-10-15`
- `model_id`: `1079_cta_lvl`
- `variable`: `mean_var_1_6m`
- `var_type`: `raw`
- `data_type`: `numeric`
- `source_table`: `hive:gcprmsbx_work.raw_1079`
- `source_column`: `mean_var_1_6m`
- `is_monotonic`: `false`

---

## `panopto_email_config_d_t_d`

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

**Ejemplo de fila:**
- `process_date`: `2025-10-15`
- `model_id`: `global`
- `sender_name`: `PANOPTO Alertas`
- `sender_email`: `alerts@example.com`
- `reply_to`: `noreply@example.com`
- `subject_prefix`: `[PANOPTO]`
- `logo_url`: ``
- `active`: `true`

---

## `model_summary_csi_psi_d_t_d`

Resumen del modelo: nombre, tipo, cut_off, frecuencia, umbrales de alerta.


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
| `process_date` | string |  |
| `model_id` | string |  |

**Ejemplo de fila:**
- `process_date`: `2025-10-15`
- `model_id`: `1079_cta_lvl`
- `model_name`: `Modelo 1079 Cuenta Level`
- `model_description`: `Modelo de nivel de cuenta`
- `model_type`: `binary`
- `status`: `active`
- `cut_off_probability`: `0.35`
- `frequency`: `daily`
- `window_value`: `3`
- `window_unit`: `weeks`
- `trigger_csi_ambar`: `0.1`
- `trigger_csi_red`: `0.2`
- `trigger_csi_variation_ambar`: `0.05`
- `trigger_csi_variation_red`: `0.1`
- `score_alert_ambar_pct`: `0.30`
- `score_alert_red_pct`: `0.15`
- `score_red_equivalent`: `2`

---

## `tresholds_table_d_t_d`

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

**Ejemplo de fila:**
- `process_date`: `2025-10-15`
- `model_id`: `1079_cta_lvl`
- `variable`: `mean_var_1_6m`
- `type`: `raw`
- `psi_threshold_ambar`: `0.10`
- `psi_threshold_red`: `0.20`
- `psi_variation_threshold_ambar`: `0.05`
- `psi_variation_threshold_red`: `0.10`

---

## `alert_policy_d_t_d`

Política de agregación de alertas por `var_type`.


| Campo | Tipo | Descripción |
|-------|------|-------------|
| `var_type` | string |  |
| `red_equivalent` | int |  |
| `alert_ambar_pct` | double |  |
| `alert_red_pct` | double |  |
| `process_date` | string |  |
| `model_id` | string |  |

**Ejemplo de fila:**
- `process_date`: `2025-10-15`
- `model_id`: `1079_cta_lvl`
- `var_type`: `raw`
- `red_equivalent`: `3`
- `alert_ambar_pct`: `0.60`
- `alert_red_pct`: `0.40`

---

## `category_policy_d_t_d`

Política de categorías: top_n_threshold y critical_top_k.


| Campo | Tipo | Descripción |
|-------|------|-------------|
| `variable` | string |  |
| `top_n_threshold` | int |  |
| `critical_top_k` | int |  |
| `process_date` | string |  |
| `model_id` | string |  |

**Ejemplo de fila:**
- `process_date`: `2025-10-15`
- `model_id`: `1079_cta_lvl`
- `variable`: `category_city`
- `top_n_threshold`: `50`
- `critical_top_k`: `5`

---

## `csi_psi_table_d_t_d`

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

**Ejemplo de fila:**
- `process_date`: `2025-10-15`
- `model_id`: `1079_cta_lvl`
- `schema`: `gcprmsbx_work`
- `table_name`: `raw_1079`
- `type`: `raw`
- `variable`: `mean_var_1_6m`
- `bin`: `1`
- `bin_type`: `NUMERIC`
- `category_value`: ``
- `lb`: `-inf`
- `ub`: `0.2`
- `lower_bound_type`: `-inf`
- `upper_bound_type`: `<=`
- `count_dev`: `200`
- `woe`: `0.05`
- `information_date_column`: `information_date`

---

## `metric_threshold_auto_d_t_d`

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

*No hay muestra aún.*

---

## `category_baseline_rank_d_t_d`

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

*No hay muestra aún.*

---

## `banamex_calendar_d_t_d`

Calendario de días hábiles.


| Campo | Tipo | Descripción |
|-------|------|-------------|
| `calendar_date` | string |  |
| `is_business_day` | boolean |  |
| `is_holiday` | boolean |  |
| `holiday_name` | string |  |
| `sync_timestamp` | timestamp |  |

*No hay muestra aún.*

---

## `config_changelog_d_t_d`

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

*No hay muestra aún.*

---

## `panopto_alert_aggregate_d_t_d`

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

*No hay muestra aún.*

---

## `panopto_email_log_d_t_d`

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

*No hay muestra aún.*

---

## `panopto_execution_log_d_t_d`

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

*No hay muestra aún.*

---

## `panopto_metric_result_d_t_d`

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

*No hay muestra aún.*

---

## `panopto_staging_control_d_t_d`

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

*No hay muestra aún.*

---

## `panopto_variable_summary_d_t_d`

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

*No hay muestra aún.*

---
