CREATE TABLE IF NOT EXISTS banamex_calendar_sync_d (
    calendar_date DATE PRIMARY KEY,
    is_business_day BOOLEAN NOT NULL,
    is_holiday BOOLEAN NOT NULL,
    holiday_name TEXT,
    sync_timestamp TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS model_contact_d_t_d (
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
