# panopto_devin

Módulo de pruebas de estabilidad y calidad de variables (MECV) para modelos en producción.

## Instalación

```bash
conda env create -f environment.yml
conda activate panopto
cp .env.example .env
# Edita .env con las credenciales reales (nunca las subas al repo).
```

## Ubicación de tablas

| Tablas | Motor |
|--------|-------|
| Configuración (`model_summary_csi_psi_d_t_d`, `csi_psi_table_d_t_d`, `tresholds_table_d_t_d`, `alert_policy_d_t_d`, `category_policy_d_t_d`, `variable_metadata_d_t_d`) | Hive / Parquet |
| Calendario (`banamex_calendar_d_t_d`) | Hive / Parquet |
| Estado y resultados (`config_changelog_d_t_d`, `category_baseline_rank_d_t_d`, `metric_threshold_auto_d_t_d`, `mecv_metric_result_d_t_d`, `mecv_alert_aggregate_d_t_d`, `mecv_execution_log_d_t_d`, `mecv_email_log_d_t_d`, `mecv_staging_control_d_t_d`, `mecv_variable_summary_d_t_d`) | Hive / Parquet |
| Calendario (`banamex_calendar_sync_d`) | PostgreSQL |
| Contactos (`model_contact_d_t_d`) | PostgreSQL |
| Lista roja global (`red_alert_list_d`) | PostgreSQL |

Los nombres y rutas anteriores se centralizan en `config/tables.json` y se exponen a través de `mecv.config.tables.ProcessConfig`. Para usarlos:

```python
from mecv.config.tables import PROCESS_CONFIG

table = PROCESS_CONFIG.metric_result_table
staging = PROCESS_CONFIG.hdfs_staging_base
```

`ProcessConfig.from_json()` lee `config/tables.json` y permite sobrescribir `hdfs_staging_base` y `hive_warehouse_dir` mediante las variables de entorno `MECV_HDFS_STAGING_BASE` y `MECV_HIVE_WAREHOUSE_DIR`.

## Muestras

Los archivos en `samples/config/` y `samples/sources/` contienen datos de ejemplo para el modelo `1079_cta_lvl` con fecha de proceso `2025-10-15`.

## Paquete `mecv`

- `mecv.config`: carga de variables de entorno.
- `mecv.sessions`: constructores de `SparkSession` y conexión a PostgreSQL.
- `mecv.calendar`: `BanamexCalendar` para días hábiles y fechas esperadas de información.
- `mecv.logging`: configuración de logging (`mecv.logging.get_logger`) con `MECV_LOG_LEVEL`.
- `mecv.data.sources` y `mecv.data.reader`: lectura de fuentes `hive:` y `parquet:` a partir de `variable_metadata`.
- `mecv.binning`: bines canónicos, categóricos y cálculo de WoE.
- `mecv.training`: `TrainingMode` para generar `csi_psi_table`, `metric_threshold_auto` y `category_baseline_rank`.
- `mecv.metrics`: motor de métricas con `MetricRegistry` y métricas de calidad, estabilidad, score y conjugadas.
- `mecv.alerts`: agregador de alertas (`AlertAggregator`), constructor HTML de emails (`EmailBuilder`) y despachador (`EmailDispatcher`).

## Estructura de `source_table`

El campo `source_table` de `variable_metadata_d_t_d` usa un prefijo URI:

- `hive:schema.tabla` para tablas Hive.
- `parquet:/ruta/externa` o `parquet:/ruta/information_date=2025-10-15` para archivos parquet.

## Credenciales

Todas las credenciales se leen desde variables de entorno (`mecv.config.Settings.from_env()`). No deben hardcodearse.

## Motor de métricas

```python
from mecv.metrics.registry import MetricRegistry

MetricCls = MetricRegistry.get("null_rate")
result = MetricCls().calculate(
    df=current_df,
    baseline=baseline_df,
    thresholds={"threshold_ambar": 0.05, "threshold_red": 0.10},
    model_id="1079_cta_lvl",
    information_date="2025-10-15",
    variable="mean_var_1_6m",
    var_type="raw",
    execution_id="exec-123",
)

print(result)
```

Métricas registradas actualmente: `null_rate`, `cardinality_ratio`, `outlier_rate`, `dominant_category_rate`, `category_composition_drift`, `psi_canonical`, `psi_dynamic`, `ks_vs_dev`, `correlation_drift`, `range_violation`, `entropy`, `approval_rate`, `tail_shift`, `concentration_gini`, `psi_approved`, `psi_rejected`, `auc`, `gini`, `brier_score`, `lift_top_decile`, `event_rate`, `psi_target`, `calibration_slope`, `ks_score_target`.

`mecv_variable_summary_d_t_d` guarda estadísticos descriptivos por variable: `count_total`, `count_non_null`, `count_null`, `min`, `max`, `mean`, `std`, deciles (`p10` ... `p90`) para numéricas; `distinct_count`, `top_category`, `top_category_count` para categóricas.

## MetricRunner

```python
from mecv.sessions import SparkSessionBuilder
from mecv.data.reader import DataReader
from mecv.metrics.runner import MetricRunner

spark = SparkSessionBuilder().build()
reader = DataReader(spark)
runner = MetricRunner(spark, reader, join_keys=["customer_id"])

results = runner.run(
    model_id="1079_cta_lvl",
    information_date="2025-10-15",
    execution_id="exec-123",
    baseline_date="2025-10-14",
)

for r in results:
    print(r)
```

`MetricRunner` lee `variable_metadata`, `model_summary`, `csi_psi_table`, `tresholds_table`, `metric_threshold_auto` y `category_policy` (última partición), ejecuta las métricas correspondientes por tipo de variable y, si existen `score` y `target`, genera las métricas conjugadas (`auc`, `gini`, `brier_score`, `lift_top_decile`).

El campo `reading_mode` de `variable_metadata_d_t_d` controla qué filas del periodo leer:
- `each` (default): un solo `information_date`.
- `first`: primer día hábil del periodo (semana/mes).
- `last`: último día hábil del periodo.

El periodo se deriva de `model_summary.frequency` (`weekly`/`monthly`). `MetricRunner` calcula automáticamente la línea base del periodo anterior cuando el modo no es `each`.

## Alertas y notificaciones

El remitente se configura en `config/email_config.json` (usa `config/email_config.example.json` como base):

```json
{
  "sender_name": "MECV Alertas",
  "sender_email": "alerts@example.com",
  "reply_to": "noreply@example.com",
  "subject_prefix": "[MECV]"
}
```

```python
from mecv.alerts.aggregator import AlertAggregator
from mecv.alerts.dispatcher import EmailDispatcher

aggregator = AlertAggregator()
aggregate_alerts = aggregator.aggregate(results)

dispatcher = EmailDispatcher()
log = dispatcher.dispatch(
    model_id="1079_cta_lvl",
    information_date="2025-10-15",
    aggregate_alerts=aggregate_alerts,
    metric_results=results,
    model_name="Modelo 1079 Cuenta Level",
    execution_id="exec-123",
)

print(log)
```

`EmailDispatcher` lee `model_contact_d_t_d` y `red_alert_list_d` desde PostgreSQL, arma un email HTML con `EmailBuilder` y lo envía por SMTP usando las credenciales de `.env`.

## DAGs de Airflow

Los DAGs están en `dags/`:

| DAG | Frecuencia | Propósito |
|-----|------------|-----------|
| `mecv_config_watcher` | Cada 30 min | Sincroniza calendario Hive → Postgres, detecta nuevos modelos y ejecuta `TrainingMode` para calcular bins y umbrales. |
| `mecv_production_runner` | Diaria | Ejecuta `MetricRunner` usando `BanamexCalendar`, persiste resultados/alertas/logs y dispara `mecv_alert_dispatcher`. |
| `mecv_alert_dispatcher` | Diaria | Genera agregados, arma emails HTML y despacha notificaciones; soporta alertas `MISSING_DATA`. |
| `mecv_output_validator` | Diaria | Valida que existan datos del día en `mecv_metric_result` y `mecv_alert_aggregate`; placeholder para refresco de Tableau. |
| `mecv_orphan_cleanup` | Semanal | Elimina directorios HDFS de `/tmp/mecv_staging` con más de 7 días. |
| `mecv_calendar_loader` | 1 de enero, 00:00 | Espera a `banamex_calendar_ext_d`, convierte a `banamex_calendar_d_t_d` y sincroniza a `banamex_calendar_sync_d`. Si en 2 días no se actualiza, pausa los DAGs `mecv_*` y alerta a la lista roja. |

## Logging

`mecv/logging.py` configura `logging` del paquete con formato `[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s`. El nivel se controla con `MECV_LOG_LEVEL` (default `INFO`). En Airflow los logs se escriben a `stdout` y se capturan en los logs de tareas.

```python
from mecv.logging import get_logger

logger = get_logger(__name__)
logger.info("mensaje informativo")
logger.warning("advertencia")
```

Para activarlos en Airflow, asegúrate de que `PYTHONPATH` incluya la raíz del repo y que `dags/` esté en `AIRFLOW__CORE__DAGS_FOLDER`.
