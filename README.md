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
| Configuración (`model_summary_csi_psi_d_t_d`, `mecv_model_table_config_d_t_d`, `csi_psi_table_d_t_d`, `tresholds_table_d_t_d`, `alert_policy_d_t_d`, `category_policy_d_t_d`, `variable_metadata_d_t_d`) | Hive / Parquet |
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

## Esquemas de tablas de salida

Los esquemas de todas las tablas de salida se centralizan en `config/output_schemas.json` y se exponen a través de `mecv.config.schemas.OutputSchemas`:

```python
from mecv.config.schemas import OutputSchemas
from mecv.config.tables import PROCESS_CONFIG

schema = OutputSchemas().get(PROCESS_CONFIG.metric_result_table)
df = spark.createDataFrame(rows, schema=schema)
```

Esto evita definir `StructType` o columnas hardcodeadas en el código de negocio y facilita versionar los esquemas junto al DDL (`sql/ddl_hive.sql`).

## Configuración de tablas fuente por modelo

Toda la configuración de las tablas `score`, `target`, `raw`, `input` y `processed` vive en Hive, en `mecv_model_table_config_d_t_d`. Ahí se define por modelo:

- `table_role`: rol de la tabla (`score`, `target`, `raw`, `input`, `processed`).
- `source_table`: referencia URI (`hive:schema.tabla` o `parquet:/ruta`).
- `entity_key_columns`: llaves tal como se llaman en la tabla fuente (ej. `["num_cliente"]`).
- `canonical_key_columns`: llaves canónicas del modelo para unir tablas (ej. `["customer_id"]`).
- `date_column` y `date_format`: columna de fecha y formato (`%Y-%m`, `%y%m`, `%Y%m`, `%y-%m`, etc.).
- `history_months`: ventana de historia que se considera para la tabla.
- `lag`: desfase en meses respecto a la fecha actual.
- `sql_transform`: expresiones del `SELECT` (sin la cláusula `FROM`) para limpiar/renombrar columnas antes de leer.
- `partition_columns`: columnas de partición adicionales (como JSON).

`DataReader` aplica el `sql_transform`, formatea las fechas al formato de la tabla, renombra las llaves locales a las llaves canónicas y lee solo las columnas necesarias. `MetricRunner` y `TrainingMode` cargan esta configuración automáticamente desde la última partición del modelo, por lo que un `score` con llave `customer_id` puede unirse a un `target` con llave `num_cliente` si ambas comparten las mismas `canonical_key_columns`.

Ejemplo de fila para un `target` con llave distinta:

```csv
process_date,model_id,table_role,table_name,source_type,source_schema,source_table,entity_key_columns,canonical_key_columns,date_column,date_format,history_months,lag,sql_transform,data_type,partition_columns,active
2025-10-15,1079_cta_lvl,target,target_1079,HIVE,gcprmsbx_work,hive:gcprmsbx_work.target_1079,"[""num_cliente""]","[""customer_id""]",information_date,,6,1,"TRIM(num_cliente) AS num_cliente",,[],true
```

El CSV de ejemplo completo está en `samples/config/mecv_model_table_config_d_t_d.csv`.

## Muestras

Los archivos en `samples/config/` y `samples/sources/` contienen datos de ejemplo para el modelo `1079_cta_lvl` con fecha de proceso `2025-10-15`.

## Paquete `mecv`

- `mecv.config`: carga de variables de entorno y esquemas de tablas de salida (`mecv.config.schemas.OutputSchemas`).
- `mecv.sessions`: constructores de `SparkSession` y conexión a PostgreSQL.
- `mecv.calendar`: `BanamexCalendar` para días hábiles y fechas esperadas de información.
- `mecv.logging`: configuración de logging (`mecv.logging.get_logger`) con `MECV_LOG_LEVEL`.
- `mecv.checkpoint`: persistencia temporal de DataFrames en parquet para evitar recomputar en reejecuciones.
- `mecv.data.sources` y `mecv.data.reader`: lectura de fuentes `hive:` y `parquet:` a partir de `variable_metadata`.
- `mecv.binning`: bines canónicos, categóricos y cálculo de WoE.
- `mecv.training`: `TrainingMode` para generar `csi_psi_table`, `metric_threshold_auto` y `category_baseline_rank`. También usa `mecv.checkpoint.Checkpoint` para no recomputar bins, umbrales y rankings si ya existen artefactos para el modelo y `process_date`.
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

`MetricRunner` almacena resultados y resúmenes en parquet temporal usando `mecv.checkpoint.Checkpoint`. Si ya existe un checkpoint válido para la combinación `model_id` + `information_date` + `baseline_date` + `frequency`, la segunda ejecución lee del parquet y no vuelve a computar. La ruta base se configura con `MECV_CHECKPOINT_BASE` (por defecto `<MECV_HDFS_STAGING_BASE>/mecv_checkpoints`). Esto es útil en clústeres con recursos limitados o cuando se coordinan reejecuciones de pruebas.

```python
from mecv.checkpoint import Checkpoint

checkpoint = Checkpoint(spark, base_path="/tmp/mecv/checkpoints")
runner = MetricRunner(spark, reader, join_keys=["customer_id"], checkpoint=checkpoint)
```

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

Para desactivar completamente el envío de correos (por ejemplo, en entornos de prueba o cuando el clúster está inestable), define `MECV_DISABLE_EMAILS=true`. En ese caso `dispatch` retorna inmediatamente un `EmailLog` con `status=DISABLED` sin intentar la conexión SMTP.

## DAGs de Airflow

Los DAGs están en `dags/`:

| DAG | Frecuencia | Propósito |
|-----|------------|-----------|
| `mecv_config_watcher` | Cada 30 min | Sincroniza calendario Hive → Postgres, detecta nuevos modelos y ejecuta `TrainingMode` para calcular bins y umbrales. |
| `mecv_production_runner` | Diaria | Ejecuta `MetricRunner` usando `BanamexCalendar`, persiste resultados/alertas/logs y dispara `mecv_alert_dispatcher`. |
| `mecv_alert_dispatcher` | Diaria | Genera agregados, arma emails HTML y despacha notificaciones; soporta alertas `MISSING_DATA`. |
| `mecv_output_validator` | Diaria | Valida que existan datos del día en `mecv_metric_result` y `mecv_alert_aggregate`; placeholder para refresco de Tableau. |
| `mecv_orphan_cleanup` | Semanal | Elimina directorios HDFS de `/tmp/mecv_staging` con más de 7 días. |
| `mecv_calendar_loader` | 1 de enero, 00:00 | Espera a `banamex_calendar_ext_d` hasta 7 días, convierte a `banamex_calendar_d_t_d` y sincroniza a `banamex_calendar_sync_d`. Si se agota el tiempo, pausa los DAGs `mecv_*` sin enviar correos. |

Todos los DAGs tienen `email_on_failure=False` y `email_on_retry=False` para evitar enviar correos por fallas transitorias del clúster, y `retries` elevado para reintentar automáticamente hasta alcanzar el éxito. Las excepciones genéricas en `mecv_production_runner`, `mecv_alert_dispatcher` y `mecv_config_watcher` se propagan para que Airflow reactive la tarea, mientras que `MissingDataError` se registra y continúa con el siguiente modelo.

## Logging

`mecv/logging.py` configura `logging` del paquete con formato `[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s`. El nivel se controla con `MECV_LOG_LEVEL` (default `INFO`). En Airflow los logs se escriben a `stdout` y se capturan en los logs de tareas.

```python
from mecv.logging import get_logger

logger = get_logger(__name__)
logger.info("mensaje informativo")
logger.warning("advertencia")
```

Para activarlos en Airflow, asegúrate de que `PYTHONPATH` incluya la raíz del repo y que `dags/` esté en `AIRFLOW__CORE__DAGS_FOLDER`.

## Tests

```bash
pip install -e ".[dev]"
python3 -m pytest tests/ -q
```

El directorio `tests/` contiene un suite de pruebas unitarias con fixtures compartidas (`spark`, `postgres_connection`, `sample_data`, `checkpoint`). El fixture `checkpoint` devuelve una instancia de `mecv.checkpoint.Checkpoint` apuntando a un directorio temporal, lo que permite verificar que las corridas de `MetricRunner` se reutilizan en reejecuciones. Algunos tests dependen de un entorno local con PySpark; si el runtime de Spark/HDFS no está disponible, al menos ejecuta `python3 -m py_compile tests/**/*.py` para validar la sintaxis.

## Agregar un nuevo modelo

La mínima información para registrar un modelo nuevo se carga con `scripts/onboard_model.py`. Antes de correrlo edita el diccionario `MODEL` al inicio del script con:

1. `model_id` y `model_summary` (nombre, frecuencia, cut_off, ventana, umbrales).
2. Las tablas fuente en `tables` con su rol, llaves canónicas, formato de fecha e historia.
3. Las variables en `variables` (cada variable apunta a una `source_table` y a un `source_column`).
4. Los contactos en `contacts` para alertas.

Luego ejecuta:

```bash
python scripts/onboard_model.py
```

Esto inserta las filas en `mecv_model_table_config_d_t_d`, `variable_metadata_d_t_d`, `model_summary_csi_psi_d_t_d` (Hive) y `model_contact_d_t_d` (PostgreSQL). Después el DAG `mecv_config_watcher` calcula bins, rankings y umbrales automáticos en el próximo ciclo.

## Tabla de motores: Hive vs PostgreSQL

| Motor | Tablas |
|-------|--------|
| **Hive / Parquet** | `mecv_model_table_config_d_t_d`, `model_summary_csi_psi_d_t_d`, `variable_metadata_d_t_d`, `tresholds_table_d_t_d`, `category_policy_d_t_d`, `alert_policy_d_t_d`, `csi_psi_table_d_t_d`, `category_baseline_rank_d_t_d`, `metric_threshold_auto_d_t_d`, `mecv_metric_result_d_t_d`, `mecv_alert_aggregate_d_t_d`, `mecv_execution_log_d_t_d`, `mecv_email_log_d_t_d`, `mecv_variable_summary_d_t_d`, `mecv_staging_control_d_t_d`, `banamex_calendar_d_t_d`, `config_changelog_d_t_d` |
| **PostgreSQL** | `banamex_calendar_sync_d`, `model_contact_d_t_d`, `red_alert_list_d` |

## Umbrales para score y target no binarias

La tabla `mecv_metric_result_d_t_d` no depende de que `target` sea binario. Las métricas de calidad (`null_rate`, `outlier_rate`, `psi_canonical`, etc.) y las de score (`entropy`, `concentration_gini`, `tail_shift`, etc.) se calculan sobre la distribución propia de la variable.

Cuando `target` es binaria, las métricas conjugadas (`auc`, `gini`, `brier_score`, `lift_top_decile`, `event_rate`, `ks_score_target`, `calibration_slope`) son directas:

- `target = 1` es el evento, `target = 0` el no-evento.
- `cut_off_probability` (de `model_summary_csi_psi_d_t_d`) separa aprobados/rechazados para `approval_rate`, `psi_approved` y `psi_rejected`.

Si `target` es continua o multicase, las métricas conjugadas que requieren dos clases no se computan. En su lugar se usan métricas de estabilidad y calidad del `score` y de la variable `target` por separado. Los umbrales en `tresholds_table_d_t_d` y `metric_threshold_auto_d_t_d` siguen aplicándose con `threshold_ambar` y `threshold_red` según la naturaleza de la métrica; por ejemplo, `psi_canonical` compara distribuciones sin importar si `target` es binario.

Para targets continuas, el campo `model_type` en `model_summary_csi_psi_d_t_d` debe reflejar `regression` y `MetricRunner` omite las métricas binarias (`auc`, `brier_score`, `ks_score_target`) si no detecta una columna `target` binaria.

## Dashboard con Streamlit

El tablero consume las vistas `mecv_dashboard_semaphore` y `mecv_dashboard_model_summary` y se ejecuta con Streamlit. Es accesible directamente desde el navegador sin Tableau.

```bash
streamlit run mecv/dashboard/app.py
```

Se abre en `http://localhost:8501`.

### Contenido

- Selector de **modelo** y rango de **fechas**.
- KPIs de semáforo: 🔴 rojas, 🟠 ámbar, 🟢 verdes y fecha más reciente.
- Gráfico de **tendencia** de `stress_ratio` y conteo de alertas por día.
- **Pie chart** de distribución de estados.
- **Barras** de alertas por tipo de variable.
- Tabla de **últimas métricas** por variable.
- Tabla de **resumen** por tipo de variable.

### Estructura

- `mecv/dashboard/data.py`: conexión a Spark y lectura de las vistas.
- `mecv/dashboard/app.py`: aplicación Streamlit con Plotly.
