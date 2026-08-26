CREATE TABLE IF NOT EXISTS model_summary_csi_psi_d_t_d (
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
    score_red_equivalent INT
)
PARTITIONED BY (process_date STRING, model_id STRING)
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compress' = 'SNAPPY',
    'spark.sql.sources.partitionOverwriteMode' = 'dynamic'
);

CREATE TABLE IF NOT EXISTS csi_psi_table_d_t_d (
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
PARTITIONED BY (process_date STRING, model_id STRING)
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compress' = 'SNAPPY',
    'spark.sql.sources.partitionOverwriteMode' = 'dynamic'
);

CREATE TABLE IF NOT EXISTS tresholds_table_d_t_d (
    variable STRING,
    type STRING,
    psi_threshold_ambar DOUBLE,
    psi_threshold_red DOUBLE,
    psi_variation_threshold_ambar DOUBLE,
    psi_variation_threshold_red DOUBLE
)
PARTITIONED BY (process_date STRING, model_id STRING)
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compress' = 'SNAPPY',
    'spark.sql.sources.partitionOverwriteMode' = 'dynamic'
);

CREATE TABLE IF NOT EXISTS alert_policy_d_t_d (
    var_type STRING,
    red_equivalent INT,
    alert_ambar_pct DOUBLE,
    alert_red_pct DOUBLE
)
PARTITIONED BY (process_date STRING, model_id STRING)
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compress' = 'SNAPPY',
    'spark.sql.sources.partitionOverwriteMode' = 'dynamic'
);

CREATE TABLE IF NOT EXISTS category_policy_d_t_d (
    variable STRING,
    top_n_threshold INT,
    critical_top_k INT
)
PARTITIONED BY (process_date STRING, model_id STRING)
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compress' = 'SNAPPY',
    'spark.sql.sources.partitionOverwriteMode' = 'dynamic'
);

CREATE TABLE IF NOT EXISTS variable_metadata_d_t_d (
    variable STRING,
    var_type STRING,
    data_type STRING,
    source_type STRING,
    source_schema STRING,
    source_table STRING,
    source_column STRING,
    information_date_column STRING,
    partition_columns STRING,
    is_monotonic BOOLEAN
)
PARTITIONED BY (process_date STRING, model_id STRING)
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compress' = 'SNAPPY',
    'spark.sql.sources.partitionOverwriteMode' = 'dynamic'
);

CREATE TABLE IF NOT EXISTS config_changelog_d_t_d (
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
PARTITIONED BY (process_date STRING, model_id STRING)
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compress' = 'SNAPPY',
    'spark.sql.sources.partitionOverwriteMode' = 'dynamic'
);

CREATE TABLE IF NOT EXISTS category_baseline_rank_d_t_d (
    variable STRING,
    category_value STRING,
    rank_dev INT,
    freq_dev DOUBLE,
    top_n_threshold INT,
    critical_top_k INT
)
PARTITIONED BY (process_date STRING, model_id STRING)
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compress' = 'SNAPPY',
    'spark.sql.sources.partitionOverwriteMode' = 'dynamic'
);

CREATE TABLE IF NOT EXISTS metric_threshold_auto_d_t_d (
    variable STRING,
    metric_name STRING,
    threshold_ambar DOUBLE,
    threshold_red DOUBLE,
    baseline_value DOUBLE,
    baseline_std DOUBLE,
    sample_size_dev INT,
    calculation_method STRING
)
PARTITIONED BY (process_date STRING, model_id STRING)
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compress' = 'SNAPPY',
    'spark.sql.sources.partitionOverwriteMode' = 'dynamic'
);

CREATE TABLE IF NOT EXISTS mecv_staging_control_d_t_d (
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
PARTITIONED BY (process_date STRING)
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compress' = 'SNAPPY',
    'spark.sql.sources.partitionOverwriteMode' = 'dynamic'
);

CREATE TABLE IF NOT EXISTS mecv_metric_result_d_t_d (
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
PARTITIONED BY (information_date STRING, model_id STRING)
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compress' = 'SNAPPY',
    'spark.sql.sources.partitionOverwriteMode' = 'dynamic'
);

CREATE TABLE IF NOT EXISTS mecv_alert_aggregate_d_t_d (
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
PARTITIONED BY (information_date STRING, model_id STRING)
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compress' = 'SNAPPY',
    'spark.sql.sources.partitionOverwriteMode' = 'dynamic'
);

CREATE TABLE IF NOT EXISTS mecv_execution_log_d_t_d (
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
PARTITIONED BY (information_date STRING, model_id STRING)
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compress' = 'SNAPPY',
    'spark.sql.sources.partitionOverwriteMode' = 'dynamic'
);

CREATE TABLE IF NOT EXISTS mecv_email_log_d_t_d (
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
PARTITIONED BY (information_date STRING, model_id STRING)
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compress' = 'SNAPPY',
    'spark.sql.sources.partitionOverwriteMode' = 'dynamic'
);

CREATE TABLE IF NOT EXISTS mecv_variable_summary_d_t_d (
    execution_id STRING,
    variable STRING,
    var_type STRING,
    data_type STRING,
    statistic STRING,
    statistic_value DOUBLE,
    statistic_value_str STRING
)
PARTITIONED BY (information_date STRING, model_id STRING)
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compress' = 'SNAPPY',
    'spark.sql.sources.partitionOverwriteMode' = 'dynamic'
);

CREATE OR REPLACE VIEW mecv_dashboard_semaphore AS
SELECT
    m.information_date,
    m.model_id,
    m.var_type,
    m.metric_name,
    m.metric_value,
    m.status,
    a.aggregate_status,
    a.stress_ratio,
    e.status AS execution_status,
    e.variables_missing
FROM mecv_metric_result_d_t_d m
LEFT JOIN mecv_alert_aggregate_d_t_d a
    ON m.model_id = a.model_id
    AND m.information_date = a.information_date
LEFT JOIN mecv_execution_log_d_t_d e
    ON m.model_id = e.model_id
    AND m.information_date = e.information_date;

CREATE OR REPLACE VIEW mecv_dashboard_model_summary AS
SELECT
    information_date,
    model_id,
    MAX(CASE WHEN var_type = 'score' THEN aggregate_status END) AS score_status,
    MAX(CASE WHEN var_type = 'input' THEN aggregate_status END) AS input_status,
    MAX(CASE WHEN var_type = 'raw' THEN aggregate_status END) AS raw_status,
    MAX(CASE WHEN var_type = 'transformed' THEN aggregate_status END) AS transformed_status,
    MAX(CASE WHEN var_type = 'SYSTEM' THEN aggregate_status END) AS system_status,
    MAX(CASE WHEN execution_status = 'MISSING_DATA' THEN 1 ELSE 0 END) AS has_missing_data,
    COUNT(DISTINCT var_type) AS var_types_evaluated
FROM (
    SELECT
        a.information_date,
        a.model_id,
        a.var_type,
        a.aggregate_status,
        e.status AS execution_status
    FROM mecv_alert_aggregate_d_t_d a
    LEFT JOIN mecv_execution_log_d_t_d e
        ON a.model_id = e.model_id
        AND a.information_date = e.information_date
) sub
GROUP BY information_date, model_id;
