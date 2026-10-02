# Diccionario de tablas PANOPTO


Esta guía describe las tablas de configuración, resultados y logs del framework PANOPTO. La **ruta Spark** es `gcprmsbx_work.panopto_<nombre_corto>`.

## `model_table_config`

Ruta Spark: `gcprmsbx_work.panopto_model_table_config`


Configuración a nivel tabla (conexión, llaves, particiones, transformación, ventana histórica, reading_mode, use_business_days).


| Campo | Tipo | Descripción |
|-------|------|-------------|
| `table_role` | string | Rol: raw, input, processed, score, target |
| `table_name` | string | Alias corto de la tabla |
| `source_type` | string | HIVE o PARQUET |
| `source_schema` | string | Esquema Hive o None |
| `source_table` | string | Nombre físico de la fuente: `schema.tabla` para HIVE, ruta para PARQUET (el tipo lo determina `source_type`, sin prefijo URI). Es la clave de join con `variable_metadata.source_table` |
| `entity_key_columns` | string | JSON con llaves de la fuente |
| `canonical_key_columns` | string | JSON con llaves unificadas |
| `date_column` | string | Columna de fecha de información |
| `date_format` | string | Formato strptime de la fecha, ej. %Y-%m-%d |
| `history_months` | int | Ventana histórica para baseline |
| `lag` | int | Meses de lag del baseline |
| `sql_transform` | string | Transformación SQL a aplicar en selectExpr |
| `data_type` | string | numeric o categorical |
| `partition_columns` | string | JSON con columnas de partición |
| `reading_mode` | string | each, first, last, first_partition o last_partition |
| `use_business_days` | boolean | True si la resolución de fechas debe usar días hábiles; False para días calendario |
| `active` | boolean | True si la configuración está activa |
| `deadline_days` | int | Días tras el fin del periodo (vintage) para exigir la ingesta de datos; usado por `MetricRunner.check_data_availability` / Data Availability Monitoring |
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


Resumen del modelo: nombre, tipo, cut_off, frecuencia, ventana, día de ejecución.


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

## `scoring_summary`

Ruta Spark: `gcprmsbx_work.panopto_scoring_summary`


Fila canónica "Summary Scoring Monitoring" (tablero PANOPTO): una por modelo/`information_date`. La construye `panopto.metrics.summary.ScoringSummaryBuilder` agregando los `MetricResult` del mismo `MetricRunner` (sin motores de prueba separados) y la escribe el DAG `panopto_production_runner`.


| Campo | Tipo | Descripción |
|-------|------|-------------|
| `model_name` | string | Nombre del modelo |
| `scoring_date` | string | Scoring Dt oficial |
| `vintage` | string | Usage Month (primer día del mes de `scoring_date`) |
| `control_data_availability` | string | Control: Data Availability (`DONE` / `PENDING`) |
| `control_pre_scoring` | string | Control Pre Scoring (`OK` / `Reprocess`): `Reprocess` si alguna métrica de `PRE_SCORING_METRICS` (`median_shift`, `population_variation`, `completeness`) está en `RED` |
| `psi` | double | PSI del score contra bins canónicos de dev (`psi_canonical`) |
| `psi_variation` | double | PSI Variation del score contra el periodo baseline (`psi_dynamic`) |
| `csi_max` | double | CSI Max: máximo PSI canónico entre variables raw/input |
| `csi_status` | string | CSI (`OK` / `Stop`) |
| `population_scored` | double | Population Scored: conteo de la población scoreada |
| `general_status` | string | General Status (`OK` / `WARNING` / `DQR Process Pending` / `Reprocess`) |
| `execution_id` | string |  |
| `run_date` | timestamp |  |
| `information_date` | string | Partición |
| `model_id` | string | Partición |

---

## `data_availability`

Ruta Spark: `gcprmsbx_work.panopto_data_availability`


Control "Data Availability Monitoring": una fila por fuente única (`source_schema`.`source_table`) y modelo/`information_date`. Lo genera `MetricRunner.check_data_availability`, que compara la fecha máxima disponible de cada fuente contra `deadline_days`.


| Campo | Tipo | Descripción |
|-------|------|-------------|
| `schema_name` | string | Schema de la fuente |
| `source_table` | string | Source (tabla física) de la fuente |
| `scoring_date` | string |  |
| `deadline_data_ingestion` | string | Fecha límite para la ingesta de datos |
| `vintage` | string |  |
| `update_data_required` | string |  |
| `control` | string | `Latest Data Updated` / `Data ingestion in progress` / `Data Ingestation has not met the deadline` / `Not enough data to process the models` |
| `execution_id` | string |  |
| `run_date` | timestamp |  |
| `information_date` | string | Partición |
| `model_id` | string | Partición |

---

## `banamex_calendar_ext_d`

Ruta Spark: `gcprmsbx_work.panopto_banamex_calendar_ext_d`


Tabla externa de calendario que publica el equipo de datos cada año. El DAG `panopto_calendar_loader` (1 de enero) la espera hasta 7 días, la copia a `panopto_banamex_calendar` y sincroniza `banamex_calendar_sync_d` en PostgreSQL. Mismo esquema que `banamex_calendar`: `calendar_date`, `is_business_day`, `is_holiday`, `holiday_name`, `sync_timestamp`. Sin particiones.


---

## Vistas del dashboard

No son tablas físicas: las crea `panopto/dashboard/builder.py` (`CREATE OR REPLACE VIEW`) desde el DAG `panopto_output_validator`.

- **`gcprmsbx_work.panopto_dashboard_semaphore`**: `metric_result` enriquecido con `aggregate_status`/`stress_ratio` de `alert_aggregate` y `execution_status`/`variables_missing` de `execution_log`.
- **`gcprmsbx_work.panopto_dashboard_model_summary`**: un semáforo por modelo/`information_date` con el `aggregate_status` por `var_type` (score, input, raw, transformed, SYSTEM), `has_missing_data` y `var_types_evaluated`.

---

## Tablas PostgreSQL

Definidas en `sql/ddl_postgres.sql`. No llevan `model_id`/`information_date` como partición Spark; son tablas relacionales operadas por `panopto.sessions.PostgresSession`.

### `banamex_calendar_sync_d`

Réplica del calendario Banamex en PostgreSQL (la alimenta `panopto_calendar_loader`).

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `calendar_date` | date | PK |
| `is_business_day` | boolean |  |
| `is_holiday` | boolean |  |
| `holiday_name` | text |  |
| `sync_timestamp` | timestamp |  |

### `model_contact`

Contactos por modelo para el despacho de alertas.

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `model_id` | text | PK compuesta |
| `contact_email` | text | PK compuesta |
| `contact_role` | text |  |
| `notify_on_ambar` | boolean |  |
| `notify_on_red` | boolean |  |
| `notify_on_missing` | boolean |  |
| `process_date` | date | PK compuesta |

### `red_alert_list_d`

Lista global de correos que reciben todas las alertas rojas (en BCC).

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `email` | text | PK |
| `name` | text |  |
| `is_active` | boolean |  |
| `added_date` | date |  |

### Copias del dashboard (`panopto_*`)

Espejos PostgreSQL de las tablas Hive que consulta el tablero Streamlit (`DashboardData` no lee Spark). Las alimenta `panopto/dashboard/pg_sync.py` desde el DAG `panopto_dashboard_sync`, con sincronización incremental por partición `(information_date, model_id)` (reemplaza particiones nuevas/modificadas y borra huérfanas).

Sin PK formal: la clave de sincronización es la partición `(information_date, model_id)` (DELETE + INSERT por partición). Las columnas de los espejos de tablas físicas derivan de `config/output_schemas.json`; las de las vistas replican las columnas de `panopto/dashboard/builder.py`.

| Tabla PostgreSQL | Espejo de |
|------------------|-----------|
| `panopto_metric_result` | `gcprmsbx_work.panopto_metric_result` |
| `panopto_variable_summary` | `gcprmsbx_work.panopto_variable_summary` |
| `panopto_scoring_summary` | `gcprmsbx_work.panopto_scoring_summary` |
| `panopto_data_availability` | `gcprmsbx_work.panopto_data_availability` |
| `panopto_dashboard_semaphore` | vista `gcprmsbx_work.panopto_dashboard_semaphore` |
| `panopto_dashboard_model_summary` | vista `gcprmsbx_work.panopto_dashboard_model_summary` |

El DAG `panopto_dashboard_sync` no tiene schedule propio: lo disparan `panopto_production_runner` y `panopto_output_validator` cuando terminan de escribir en Hive. Acepta conf `{"information_date": ..., "model_id": ...}` para acotar el diff; sin conf hace diff completo de particiones.

---
