-- DDL de tablas Hive/Spark del modelo PANOPTO

CREATE DATABASE IF NOT EXISTS gcprmsbx_work;


CREATE TABLE IF NOT EXISTS gcprmsbx_work.panopto_model_summary_csi_psi (
    model_name STRING,
    model_description STRING,
    model_type STRING,
    status STRING,
    cut_off_probability DOUBLE,
    frequency STRING,
    window_value INT,
    window_unit STRING,
    trigger_csi_ambar DOUBLE,
    trigger_csi_red DOUBLE,
    trigger_csi_variation_ambar DOUBLE,
    trigger_csi_variation_red DOUBLE,
    score_alert_ambar_pct DOUBLE,
    score_alert_red_pct DOUBLE,
    score_red_equivalent INT,
    execution_monthly_day INT -- Día del mes (1-31) en que se espera la carga para frecuencia mensual,
    execution_weekday INT -- Día de la semana (0=Lunes) en que se espera la carga para frecuencia semanal,
    target_lag_months INT -- Meses de desfase con los que la target está disponible
)
PARTITIONED BY (
    process_date STRING,
    model_id STRING
)
STORED AS PARQUET
;


CREATE TABLE IF NOT EXISTS gcprmsbx_work.panopto_csi_psi_table (
    schema STRING,
    table_name STRING,
    type STRING,
    variable STRING,
    bin INT,
    bin_type STRING,
    category_value STRING,
    lb DOUBLE,
    ub DOUBLE,
    lower_bound_type STRING,
    upper_bound_type STRING,
    count_dev INT,
    woe DOUBLE,
    information_date_column STRING
)
PARTITIONED BY (
    process_date STRING,
    model_id STRING
)
STORED AS PARQUET
;


CREATE TABLE IF NOT EXISTS gcprmsbx_work.panopto_tresholds_table (
    variable STRING,
    type STRING,
    psi_threshold_ambar DOUBLE,
    psi_threshold_red DOUBLE,
    psi_variation_threshold_ambar DOUBLE,
    psi_variation_threshold_red DOUBLE
)
PARTITIONED BY (
    process_date STRING,
    model_id STRING
)
STORED AS PARQUET
;


CREATE TABLE IF NOT EXISTS gcprmsbx_work.panopto_alert_policy (
    var_type STRING,
    red_equivalent INT,
    alert_ambar_pct DOUBLE,
    alert_red_pct DOUBLE
)
PARTITIONED BY (
    process_date STRING,
    model_id STRING
)
STORED AS PARQUET
;


CREATE TABLE IF NOT EXISTS gcprmsbx_work.panopto_category_policy (
    variable STRING,
    top_n_threshold INT,
    critical_top_k INT
)
PARTITIONED BY (
    process_date STRING,
    model_id STRING
)
STORED AS PARQUET
;


CREATE TABLE IF NOT EXISTS gcprmsbx_work.panopto_variable_metadata (
    variable STRING -- Nombre de la variable,
    var_type STRING -- Rol: raw, input, transformed, score, target,
    data_type STRING -- numeric o categorical,
    source_table STRING -- Nombre de tabla en table metadata (clave cruzada),
    source_column STRING -- Columna física en la tabla fuente,
    is_monotonic BOOLEAN -- True si la variable es monótona
)
PARTITIONED BY (
    process_date STRING,
    model_id STRING
)
STORED AS PARQUET
;


CREATE TABLE IF NOT EXISTS gcprmsbx_work.panopto_config_changelog (
    change_timestamp TIMESTAMP,
    table_name STRING,
    change_type STRING,
    field_changed STRING,
    old_value STRING,
    new_value STRING,
    triggered_retraining BOOLEAN,
    error_message STRING,
    executed_by_dag_id STRING,
    run_id STRING
)
PARTITIONED BY (
    process_date STRING,
    model_id STRING
)
STORED AS PARQUET
;


CREATE TABLE IF NOT EXISTS gcprmsbx_work.panopto_category_baseline_rank (
    variable STRING,
    category_value STRING,
    rank_dev INT,
    freq_dev DOUBLE,
    top_n_threshold INT,
    critical_top_k INT
)
PARTITIONED BY (
    process_date STRING,
    model_id STRING
)
STORED AS PARQUET
;


CREATE TABLE IF NOT EXISTS gcprmsbx_work.panopto_metric_threshold_auto (
    variable STRING,
    metric_name STRING,
    threshold_ambar DOUBLE,
    threshold_red DOUBLE,
    baseline_value DOUBLE,
    baseline_std DOUBLE,
    sample_size_dev INT,
    calculation_method STRING
)
PARTITIONED BY (
    process_date STRING,
    model_id STRING
)
STORED AS PARQUET
;


CREATE TABLE IF NOT EXISTS gcprmsbx_work.panopto_staging_control (
    staging_id STRING,
    execution_id STRING,
    model_id STRING,
    target_table STRING,
    information_date STRING,
    temp_path STRING,
    final_path STRING,
    row_count_temp INT,
    row_count_final INT,
    status STRING,
    started_at TIMESTAMP,
    validated_at TIMESTAMP,
    promoted_at TIMESTAMP
)
PARTITIONED BY (
    process_date STRING
)
STORED AS PARQUET
;


CREATE TABLE IF NOT EXISTS gcprmsbx_work.panopto_metric_result (
    execution_id STRING,
    variable STRING,
    var_type STRING,
    metric_name STRING,
    metric_value DOUBLE,
    baseline_value DOUBLE,
    threshold_ambar DOUBLE,
    threshold_red DOUBLE,
    status STRING,
    baseline_process_date STRING,
    run_date TIMESTAMP,
    dag_id STRING,
    airflow_run_id STRING
)
PARTITIONED BY (
    information_date STRING,
    model_id STRING
)
STORED AS PARQUET
;


CREATE TABLE IF NOT EXISTS gcprmsbx_work.panopto_alert_aggregate (
    execution_id STRING,
    var_type STRING,
    total_metrics INT,
    count_ambar INT,
    count_red INT,
    equivalent_yellow DOUBLE,
    stress_ratio DOUBLE,
    aggregate_status STRING,
    alert_sent BOOLEAN,
    alert_type STRING,
    red_equivalent_used INT,
    alert_ambar_pct_used DOUBLE,
    alert_red_pct_used DOUBLE,
    run_date TIMESTAMP
)
PARTITIONED BY (
    information_date STRING,
    model_id STRING
)
STORED AS PARQUET
;


CREATE TABLE IF NOT EXISTS gcprmsbx_work.panopto_execution_log (
    execution_id STRING,
    dag_id STRING,
    airflow_run_id STRING,
    run_date TIMESTAMP,
    end_date TIMESTAMP,
    status STRING,
    error_message STRING,
    reason STRING,
    variables_expected INT,
    variables_processed INT,
    variables_missing INT,
    metrics_calculated INT,
    metrics_failed INT,
    duration_seconds INT
)
PARTITIONED BY (
    information_date STRING,
    model_id STRING
)
STORED AS PARQUET
;


CREATE TABLE IF NOT EXISTS gcprmsbx_work.panopto_email_log (
    email_id STRING,
    execution_id STRING,
    alert_type STRING,
    recipients_to STRING,
    recipients_bcc STRING,
    subject STRING,
    body_summary STRING,
    sent_timestamp TIMESTAMP,
    status STRING,
    smtp_response STRING,
    retry_count INT
)
PARTITIONED BY (
    information_date STRING,
    model_id STRING
)
STORED AS PARQUET
;


CREATE TABLE IF NOT EXISTS gcprmsbx_work.panopto_variable_summary (
    execution_id STRING,
    variable STRING,
    var_type STRING,
    data_type STRING,
    statistic STRING,
    statistic_value DOUBLE,
    statistic_value_str STRING
)
PARTITIONED BY (
    information_date STRING,
    model_id STRING
)
STORED AS PARQUET
;


CREATE TABLE IF NOT EXISTS gcprmsbx_work.panopto_model_table_config (
    table_role STRING -- Rol: raw, input, processed, score, target,
    table_name STRING -- Alias corto de la tabla,
    source_type STRING -- HIVE o PARQUET,
    source_schema STRING -- Esquema Hive o None,
    source_table STRING -- Nombre físico con prefijo hive: o parquet:,
    entity_key_columns STRING -- JSON con llaves de la fuente,
    canonical_key_columns STRING -- JSON con llaves unificadas,
    date_column STRING -- Columna de fecha de información,
    date_format STRING -- Formato strptime de la fecha, ej. %Y-%m-%d,
    history_months INT -- Ventana histórica para baseline,
    lag INT -- Meses de lag del baseline,
    sql_transform STRING -- Transformación SQL a aplicar en selectExpr,
    data_type STRING -- numeric o categorical,
    partition_columns STRING -- JSON con columnas de partición,
    reading_mode STRING -- each, first o last,
    active BOOLEAN -- True si la configuración está activa
)
PARTITIONED BY (
    process_date STRING,
    model_id STRING
)
STORED AS PARQUET
;


CREATE TABLE IF NOT EXISTS gcprmsbx_work.panopto_email_config (
    sender_name STRING -- Nombre del remitente,
    sender_email STRING -- Dirección del remitente,
    reply_to STRING -- Reply-To,
    subject_prefix STRING -- Prefijo del asunto,
    logo_url STRING -- URL del logo,
    active BOOLEAN -- True si la config está activa
)
PARTITIONED BY (
    process_date STRING,
    model_id STRING
)
STORED AS PARQUET
;


CREATE TABLE IF NOT EXISTS gcprmsbx_work.panopto_banamex_calendar (
    calendar_date STRING,
    is_business_day BOOLEAN,
    is_holiday BOOLEAN,
    holiday_name STRING,
    sync_timestamp TIMESTAMP
)
STORED AS PARQUET
;


-- Tabla externa de calendario

CREATE TABLE IF NOT EXISTS gcprmsbx_work.panopto_banamex_calendar_ext_d (
    calendar_date STRING,
    is_business_day BOOLEAN,
    is_holiday BOOLEAN,
    holiday_name STRING,
    sync_timestamp TIMESTAMP
)
STORED AS PARQUET
;

