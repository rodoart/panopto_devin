CREATE TABLE IF NOT EXISTS banamex_calendar_sync_d (
    calendar_date DATE PRIMARY KEY,
    is_business_day BOOLEAN NOT NULL,
    is_holiday BOOLEAN NOT NULL,
    holiday_name TEXT,
    sync_timestamp TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS model_contact (
    model_id TEXT NOT NULL,
    contact_email TEXT NOT NULL,
    contact_role TEXT,
    notify_on_ambar BOOLEAN,
    notify_on_red BOOLEAN,
    notify_on_missing BOOLEAN,
    process_date DATE NOT NULL,
    PRIMARY KEY (model_id, contact_email, process_date)
);

CREATE TABLE IF NOT EXISTS red_alert_list_d (
    email TEXT PRIMARY KEY,
    name TEXT,
    is_active BOOLEAN NOT NULL,
    added_date DATE NOT NULL
);

-- Espejos en PostgreSQL de las tablas/vistas Hive que alimenta el dashboard
-- (los crea/mantiene también panopto.dashboard.pg_sync con CREATE IF NOT EXISTS).

CREATE TABLE IF NOT EXISTS panopto_metric_result (
    execution_id TEXT,
    variable TEXT,
    var_type TEXT,
    metric_name TEXT,
    metric_value DOUBLE PRECISION,
    baseline_value DOUBLE PRECISION,
    threshold_ambar DOUBLE PRECISION,
    threshold_red DOUBLE PRECISION,
    status TEXT,
    baseline_process_date TEXT,
    run_date TIMESTAMP,
    dag_id TEXT,
    airflow_run_id TEXT,
    information_date TEXT,
    model_id TEXT
);

CREATE TABLE IF NOT EXISTS panopto_variable_summary (
    execution_id TEXT,
    variable TEXT,
    var_type TEXT,
    data_type TEXT,
    statistic TEXT,
    statistic_value DOUBLE PRECISION,
    statistic_value_str TEXT,
    information_date TEXT,
    model_id TEXT
);

CREATE TABLE IF NOT EXISTS panopto_scoring_summary (
    model_name TEXT,
    scoring_date TEXT,
    vintage TEXT,
    control_data_availability TEXT,
    control_pre_scoring TEXT,
    psi DOUBLE PRECISION,
    psi_variation DOUBLE PRECISION,
    csi_max DOUBLE PRECISION,
    csi_status TEXT,
    population_scored DOUBLE PRECISION,
    general_status TEXT,
    execution_id TEXT,
    run_date TIMESTAMP,
    information_date TEXT,
    model_id TEXT
);

CREATE TABLE IF NOT EXISTS panopto_data_availability (
    schema_name TEXT,
    source_table TEXT,
    scoring_date TEXT,
    deadline_data_ingestion TEXT,
    vintage TEXT,
    update_data_required TEXT,
    control TEXT,
    execution_id TEXT,
    run_date TIMESTAMP,
    information_date TEXT,
    model_id TEXT
);

CREATE TABLE IF NOT EXISTS panopto_dashboard_semaphore (
    information_date TEXT,
    model_id TEXT,
    var_type TEXT,
    metric_name TEXT,
    metric_value DOUBLE PRECISION,
    status TEXT,
    aggregate_status TEXT,
    stress_ratio DOUBLE PRECISION,
    execution_status TEXT,
    variables_missing INTEGER
);

CREATE TABLE IF NOT EXISTS panopto_dashboard_model_summary (
    information_date TEXT,
    model_id TEXT,
    score_status TEXT,
    input_status TEXT,
    raw_status TEXT,
    transformed_status TEXT,
    system_status TEXT,
    has_missing_data INTEGER,
    var_types_evaluated INTEGER
);
