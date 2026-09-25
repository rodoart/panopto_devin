# panopto_devin

Módulo de pruebas de estabilidad y calidad de variables (PANOPTO) para modelos en producción.

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
| Configuración (`gcprmsbx_work.panopto_model_summary_csi_psi`, `gcprmsbx_work.panopto_model_table_config`, `gcprmsbx_work.panopto_csi_psi_table`, `gcprmsbx_work.panopto_tresholds_table`, `gcprmsbx_work.panopto_alert_policy`, `gcprmsbx_work.panopto_category_policy`, `gcprmsbx_work.panopto_variable_metadata`) | Hive / Parquet |
| Calendario (`gcprmsbx_work.panopto_banamex_calendar`) | Hive / Parquet |
| Estado y resultados (`gcprmsbx_work.panopto_config_changelog`, `gcprmsbx_work.panopto_category_baseline_rank`, `gcprmsbx_work.panopto_metric_threshold_auto`, `gcprmsbx_work.panopto_metric_result`, `gcprmsbx_work.panopto_alert_aggregate`, `gcprmsbx_work.panopto_execution_log`, `gcprmsbx_work.panopto_email_log`, `gcprmsbx_work.panopto_staging_control`, `gcprmsbx_work.panopto_variable_summary`) | Hive / Parquet |
| Tablero PANOPTO (`gcprmsbx_work.panopto_scoring_summary`, `gcprmsbx_work.panopto_data_availability`) | Hive / Parquet |
| Calendario (`banamex_calendar_sync_d`) | PostgreSQL |
| Contactos (`model_contact`) | PostgreSQL |
| Lista roja global (`red_alert_list_d`) | PostgreSQL |

Los nombres y rutas anteriores se centralizan en `config/tables.json` y se exponen a través de `panopto.config.tables.ProcessConfig`. Para usarlos:

```python
from panopto.config.tables import PROCESS_CONFIG

table = PROCESS_CONFIG.metric_result_table
staging = PROCESS_CONFIG.hdfs_staging_base
```

`ProcessConfig.from_json()` lee `config/tables.json` y permite sobrescribir `hdfs_staging_base` y `hive_warehouse_dir` mediante las variables de entorno `PANOPTO_HDFS_STAGING_BASE` y `PANOPTO_HIVE_WAREHOUSE_DIR`.

## Esquemas de tablas de salida

Los esquemas de todas las tablas de salida se centralizan en `config/output_schemas.json` y se exponen a través de `panopto.config.schemas.OutputSchemas`:

```python
from panopto.config.schemas import OutputSchemas
from panopto.config.tables import PROCESS_CONFIG

schema = OutputSchemas().get(PROCESS_CONFIG.metric_result_table)
df = spark.createDataFrame(rows, schema=schema)
```

Esto evita definir `StructType` o columnas hardcodeadas en el código de negocio y facilita versionar los esquemas junto al DDL (`sql/ddl_hive.sql`).

## Configuración de tablas fuente por modelo

Toda la configuración de las tablas `score`, `target`, `raw`, `input` y `processed` vive en Hive, en `gcprmsbx_work.panopto_model_table_config`. Ahí se define por modelo:

- `table_role`: rol de la tabla (`score`, `target`, `raw`, `input`, `processed`).
- `source_table`: referencia URI (`hive:schema.tabla` o `parquet:/ruta`).
- `entity_key_columns`: llaves tal como se llaman en la tabla fuente (ej. `["num_cliente"]`).
- `canonical_key_columns`: llaves canónicas del modelo para unir tablas (ej. `["customer_id"]`).
- `date_column` y `date_format`: columna de fecha y formato (`%Y-%m`, `%y%m`, `%Y%m`, `%y-%m`, etc.).
- `history_months`: ventana de historia que se considera para la tabla.
- `lag`: desfase en meses respecto a la fecha actual.
- `sql_transform`: expresiones del `SELECT` (sin la cláusula `FROM`) para limpiar/renombrar columnas antes de leer.
- `partition_columns`: columnas de partición adicionales (como JSON).
- `deadline_days`: días tras el fin del periodo (vintage) por los que se tolera la ausencia de datos de esa fuente antes de marcarla como incumplida en el control "Data Availability Monitoring" del dashboard.

`DataReader` aplica el `sql_transform`, formatea las fechas al formato de la tabla, renombra las llaves locales a las llaves canónicas y lee solo las columnas necesarias. `MetricRunner` y `TrainingMode` cargan esta configuración automáticamente desde la última partición del modelo, por lo que un `score` con llave `customer_id` puede unirse a un `target` con llave `num_cliente` si ambas comparten las mismas `canonical_key_columns`.

Ejemplo de fila para un `target` con llave distinta:

```csv
process_date,model_id,table_role,table_name,source_type,source_schema,source_table,entity_key_columns,canonical_key_columns,date_column,date_format,history_months,lag,sql_transform,data_type,partition_columns,active
2025-10-15,1079_cta_lvl,target,target_1079,HIVE,gcprmsbx_work,hive:gcprmsbx_work.target_1079,"[""num_cliente""]","[""customer_id""]",information_date,,6,1,"TRIM(num_cliente) AS num_cliente",,[],true
```

El CSV de ejemplo completo está en `samples/config/gcprmsbx_work.panopto_model_table_config.csv`.

## Muestras

Los archivos en `samples/config/` y `samples/sources/` contienen datos de ejemplo para el modelo `1079_cta_lvl` con fecha de proceso `2025-10-15`.

## Paquete `panopto`

- `panopto.config`: carga de variables de entorno y esquemas de tablas de salida (`panopto.config.schemas.OutputSchemas`).
- `panopto.sessions`: constructores de `SparkSession` y conexión a PostgreSQL.
- `panopto.calendar`: `BanamexCalendar` para días hábiles y fechas esperadas de información.
- `panopto.logging`: configuración de logging (`panopto.logging.get_logger`) con `PANOPTO_LOG_LEVEL`.
- `panopto.checkpoint`: persistencia temporal de DataFrames en parquet para evitar recomputar en reejecuciones.
- `panopto.data.sources` y `panopto.data.reader`: lectura de fuentes `hive:` y `parquet:` a partir de `variable_metadata`.
- `panopto.binning`: bines canónicos, categóricos y cálculo de WoE.
- `panopto.training`: `TrainingMode` para generar `csi_psi_table`, `metric_threshold_auto` y `category_baseline_rank`. `process_date` se usa como partición de salida; `information_date` se deriva de `process_date` usando `frequency`, `execution_monthly_day` y `execution_weekday`, pero puede sobreescribirse manualmente para entrenar como si fuera un día anterior.
- `panopto.metrics`: motor de métricas con `MetricRegistry` y métricas de calidad, estabilidad, score y conjugadas.
- `panopto.alerts`: agregador de alertas (`AlertAggregator`), constructor HTML de emails (`EmailBuilder`) y despachador (`EmailDispatcher`).

## Entrenamiento con fecha de información distinta a `process_date`

`TrainingMode.run` acepta un parámetro opcional `information_date`:

```python
from panopto.sessions import SparkSessionBuilder
from panopto.data.reader import DataReader
from panopto.training import TrainingMode

spark = SparkSessionBuilder(app_name="manual_training").build()
tm = TrainingMode(spark, DataReader(spark))

# Entrenar con process_date=hoy pero leer datos como si fuera 2026-08-07:
tm.run(
    model_id="test_camelot_st",
    process_date="2026-09-14",
    execution_id="manual_001",
    information_date="2026-08-07",
)
```

- `process_date` se usa para: cargar la última partición de `variable_metadata` y `category_policy`, formar la clave del `Checkpoint` y escribir las tablas de salida con `process_date` como partición.
- `information_date` se usa para: calcular `history_date_range` y leer las tablas fuente (`_read_dev_data`). Si no se indica, se deriva de `process_date` con `_reference_date` usando `frequency`, `execution_monthly_day` y `execution_weekday`.

Esto permite hacer **backfill** o reproducir un entrenamiento histórico sin cambiar las particiones de salida ni la configuración del modelo.

### Desde Airflow

El DAG `panopto_config_watcher` lee `information_date` del campo `conf` del `dag_run`. Ejemplo de activación manual:

```bash
airflow dags trigger panopto_config_watcher --conf '{"information_date":"2026-08-07"}'
```

Si `information_date` no está en `conf`, se usa el comportamiento por defecto (`process_date = context["ds"]`).

## Estructura de `source_table`

El campo `source_table` de `gcprmsbx_work.panopto_variable_metadata` usa un prefijo URI:

- `hive:schema.tabla` para tablas Hive.
- `parquet:/ruta/externa` o `parquet:/ruta/information_date=2025-10-15` para archivos parquet.

## Credenciales

Todas las credenciales se leen desde variables de entorno (`panopto.config.Settings.from_env()`). No deben hardcodearse.

### Kerberos

Si el cluster Hive/HDFS usa Kerberos, configure `PANOPTO_KINIT_KEYTAB` y `PANOPTO_KINIT_PRINCIPAL`:

```bash
PANOPTO_KINIT_KEYTAB=/etc/security/keytabs/panopto.keytab
PANOPTO_KINIT_PRINCIPAL=panopto@EXAMPLE.COM
```

- `SparkSessionBuilder.build()` ejecuta `kinit -kt $PANOPTO_KINIT_KEYTAB $PANOPTO_KINIT_PRINCIPAL` antes de construir la sesión, así cada corrida de DAG renueva el ticket.
- El DAG `panopto_kinit` corre cada 7 horas como red de seguridad, para que el ticket nunca expire si hay huecos entre ejecuciones.

## Motor de métricas

```python
from panopto.metrics.registry import MetricRegistry

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

Métricas registradas actualmente: `null_rate`, `cardinality_ratio`, `outlier_rate`, `dominant_category_rate`, `category_composition_drift`, `median_shift`, `population_variation`, `completeness`, `psi_canonical`, `psi_dynamic`, `ks_vs_dev`, `correlation_drift`, `range_violation`, `entropy`, `approval_rate`, `tail_shift`, `concentration_gini`, `psi_approved`, `psi_rejected`, `auc`, `gini`, `brier_score`, `lift_top_decile`, `event_rate`, `psi_target`, `calibration_slope`, `ks_score_target`.

`median_shift`, `population_variation` y `completeness` alimentan el control oficial "Pre Scoring" y "Population Scored" del dashboard (ver sección Dashboard más abajo); no requieren un motor de pruebas distinto, se calculan y agregan igual que cualquier otra métrica del `MetricRegistry`.

`gcprmsbx_work.panopto_variable_summary` guarda estadísticos descriptivos por variable: `count_total`, `count_non_null`, `count_null`, `min`, `max`, `mean`, `std`, deciles (`p10` ... `p90`) para numéricas; `distinct_count`, `top_category`, `top_category_count` para categóricas.

## MetricRunner

```python
from panopto.sessions import SparkSessionBuilder
from panopto.data.reader import DataReader
from panopto.metrics.runner import MetricRunner

spark = SparkSessionBuilder().build()
reader = DataReader(spark)
runner = MetricRunner(spark, reader)

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

`MetricRunner` almacena resultados y resúmenes en parquet temporal usando `panopto.checkpoint.Checkpoint`. Si ya existe un checkpoint válido para la combinación `model_id` + `information_date` + `baseline_date` + `frequency`, la segunda ejecución lee del parquet y no vuelve a computar. La ruta base se configura con `PANOPTO_CHECKPOINT_BASE` (por defecto `<PANOPTO_HDFS_STAGING_BASE>/panopto_checkpoints`). Esto es útil en clústeres con recursos limitados o cuando se coordinan reejecuciones de pruebas.

```python
from panopto.checkpoint import Checkpoint

checkpoint = Checkpoint(spark, base_path="/tmp/panopto/checkpoints")
runner = MetricRunner(spark, reader, checkpoint=checkpoint)
```

El campo `reading_mode` de `gcprmsbx_work.panopto_variable_metadata` controla qué filas del periodo leer:
- `first`: primer día del periodo (primer día hábil si `use_business_days=true`, primer día calendario si no).
- `last`: último día del periodo (último día hábil si `use_business_days=true`, último día calendario si no).
- `each`: todos los días del periodo. Para frecuencia `daily` es un solo día; para `weekly`/`monthly` son todos los días hábiles o calendario según `use_business_days`.

El periodo se deriva de `model_summary.frequency` (`weekly`/`monthly`). `MetricRunner` desplaza el `current` por el `lag` de `model_table_config` y calcula la línea base un periodo antes del `current`.

## Alertas y notificaciones

El remitente se configura en `config/email_config.json` (usa `config/email_config.example.json` como base):

```json
{
  "sender_name": "PANOPTO Alertas",
  "sender_email": "alerts@example.com",
  "reply_to": "noreply@example.com",
  "subject_prefix": "[PANOPTO]"
}
```

```python
from panopto.alerts.aggregator import AlertAggregator
from panopto.alerts.dispatcher import EmailDispatcher

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

`EmailDispatcher` lee `model_contact` y `red_alert_list_d` desde PostgreSQL, arma un email HTML con `EmailBuilder` y lo envía por SMTP usando las credenciales de `.env`.

Para desactivar completamente el envío de correos (por ejemplo, en entornos de prueba o cuando el clúster está inestable), define `PANOPTO_DISABLE_EMAILS=true`. En ese caso `dispatch` retorna inmediatamente un `EmailLog` con `status=DISABLED` sin intentar la conexión SMTP.

## DAGs de Airflow

Los DAGs están en `dags/`:

| DAG | Frecuencia | Propósito |
|-----|------------|-----------|
| `panopto_config_watcher` | Cada 30 min | Sincroniza calendario Hive → Postgres, detecta nuevos modelos y ejecuta `TrainingMode` para calcular bins y umbrales. |
| `panopto_production_runner` | Diaria | Ejecuta `MetricRunner` usando `BanamexCalendar`, persiste resultados/alertas/logs y dispara `panopto_alert_dispatcher`. |
| `panopto_alert_dispatcher` | Diaria | Genera agregados, arma emails HTML y despacha notificaciones; soporta alertas `MISSING_DATA`. |
| `panopto_output_validator` | Diaria | Valida que existan datos del día en `panopto_metric_result` y `panopto_alert_aggregate`; placeholder para refresco de Tableau. |
| `panopto_orphan_cleanup` | Semanal | Elimina directorios HDFS de `/tmp/panopto_staging` con más de 7 días. |
| `panopto_calendar_loader` | 1 de enero, 00:00 | Espera a `banamex_calendar_ext_d` hasta 7 días, convierte a `gcprmsbx_work.panopto_banamex_calendar` y sincroniza a `banamex_calendar_sync_d`. Si se agota el tiempo, pausa los DAGs `panopto_*` sin enviar correos. |

Todos los DAGs tienen `email_on_failure=False` y `email_on_retry=False` para evitar enviar correos por fallas transitorias del clúster, y `retries` elevado para reintentar automáticamente hasta alcanzar el éxito. Las excepciones genéricas en `panopto_production_runner`, `panopto_alert_dispatcher` y `panopto_config_watcher` se propagan para que Airflow reactive la tarea, mientras que `MissingDataError` se registra y continúa con el siguiente modelo.

## Logging

`panopto/logging.py` configura `logging` del paquete con formato `[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s`. El nivel se controla con `PANOPTO_LOG_LEVEL` (default `INFO`). En Airflow los logs se escriben a `stdout` y se capturan en los logs de tareas.

```python
from panopto.logging import get_logger

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

El directorio `tests/` contiene un suite de pruebas unitarias con fixtures compartidas (`spark`, `postgres_connection`, `sample_data`, `checkpoint`). El fixture `checkpoint` devuelve una instancia de `panopto.checkpoint.Checkpoint` apuntando a un directorio temporal, lo que permite verificar que las corridas de `MetricRunner` se reutilizan en reejecuciones. Algunos tests dependen de un entorno local con PySpark; si el runtime de Spark/HDFS no está disponible, al menos ejecuta `python3 -m py_compile tests/**/*.py` para validar la sintaxis.

### Modo local (virtualenv + Spark local)

Existe una opción de pruebas **local** que crea un virtualenv aislado (`.venv-local/`) e instala Spark vía `pip` (`pyspark==3.3.2`, la versión pineada en `pyproject.toml`). No interviene con el ambiente conda/cluster: todo queda dentro del venv y de las variables que exporta el propio script.

```bash
make local-env      # crea .venv-local e instala requirements-local.txt
make test-local     # corre pytest dentro del venv (PANOPTO_TEST_MODE=local)
make demo-local     # ejemplo mínimo: métricas reales sobre samples/sources en Spark local[*]
# equivalente sin make: scripts/setup_local_env.sh, scripts/run_local_tests.sh,
# scripts/run_local_demo.sh (este último ejecuta scripts/local_smoke.py)
```

El modo es parametrizable:

- `pytest --mode={auto,local,cluster}` o `PANOPTO_TEST_MODE` (default `auto`, comportamiento actual). En `local` se saltan tests marcados `@pytest.mark.cluster` y viceversa.
- `PANOPTO_LOCAL_VENV`, `PANOPTO_LOCAL_PYTHON` (default: autodetecta python3.10/3.9/3.8 — PySpark 3.3 requiere ≤3.10), `PANOPTO_LOCAL_REQUIREMENTS`, `PANOPTO_LOCAL_PYTEST_ARGS`.
- `PANOPTO_TEST_SPARK_MASTER` (default `local[*]`) y `PANOPTO_TEST_SPARK_CONF` (JSON con conf extra de Spark para la sesión de tests).
- Si el JDK detectado es ≥16, `scripts/run_local_tests.sh` exporta los `--add-opens` que Spark 3.3 necesita (`JDK_JAVA_OPTIONS`); con Java 8/11 no hace falta.

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

Esto inserta las filas en `gcprmsbx_work.panopto_model_table_config`, `gcprmsbx_work.panopto_variable_metadata`, `gcprmsbx_work.panopto_model_summary_csi_psi` (Hive) y `model_contact` (PostgreSQL). Después el DAG `panopto_config_watcher` calcula bins, rankings y umbrales automáticos en el próximo ciclo.

## Tabla de motores: Hive vs PostgreSQL

| Motor | Tablas |
|-------|--------|
| **Hive / Parquet** | `gcprmsbx_work.panopto_model_table_config`, `gcprmsbx_work.panopto_model_summary_csi_psi`, `gcprmsbx_work.panopto_variable_metadata`, `gcprmsbx_work.panopto_tresholds_table`, `gcprmsbx_work.panopto_category_policy`, `gcprmsbx_work.panopto_alert_policy`, `gcprmsbx_work.panopto_csi_psi_table`, `gcprmsbx_work.panopto_category_baseline_rank`, `gcprmsbx_work.panopto_metric_threshold_auto`, `gcprmsbx_work.panopto_metric_result`, `gcprmsbx_work.panopto_alert_aggregate`, `gcprmsbx_work.panopto_execution_log`, `gcprmsbx_work.panopto_email_log`, `gcprmsbx_work.panopto_variable_summary`, `gcprmsbx_work.panopto_staging_control`, `gcprmsbx_work.panopto_banamex_calendar`, `gcprmsbx_work.panopto_config_changelog` |
| **PostgreSQL** | `banamex_calendar_sync_d`, `model_contact`, `red_alert_list_d` |

## Umbrales para score y target no binarias

La tabla `gcprmsbx_work.panopto_metric_result` no depende de que `target` sea binario. Las métricas de calidad (`null_rate`, `outlier_rate`, `psi_canonical`, etc.) y las de score (`entropy`, `concentration_gini`, `tail_shift`, etc.) se calculan sobre la distribución propia de la variable.

Cuando `target` es binaria, las métricas conjugadas (`auc`, `gini`, `brier_score`, `lift_top_decile`, `event_rate`, `ks_score_target`, `calibration_slope`) son directas:

- `target = 1` es el evento, `target = 0` el no-evento.
- `cut_off_probability` (de `gcprmsbx_work.panopto_model_summary_csi_psi`) separa aprobados/rechazados para `approval_rate`, `psi_approved` y `psi_rejected`.

Si `target` es continua o multicase, las métricas conjugadas que requieren dos clases no se computan. En su lugar se usan métricas de estabilidad y calidad del `score` y de la variable `target` por separado. Los umbrales en `gcprmsbx_work.panopto_tresholds_table` y `gcprmsbx_work.panopto_metric_threshold_auto` siguen aplicándose con `threshold_ambar` y `threshold_red` según la naturaleza de la métrica; por ejemplo, `psi_canonical` compara distribuciones sin importar si `target` es binario.

Para targets continuas, el campo `model_type` en `gcprmsbx_work.panopto_model_summary_csi_psi` debe reflejar `regression` y `MetricRunner` omite las métricas binarias (`auc`, `brier_score`, `ks_score_target`) si no detecta una columna `target` binaria.

## Dashboard con Streamlit

El tablero se ejecuta con Streamlit (no usa Tableau) y reproduce, con la misma nomenclatura oficial, la estructura de pestañas del tablero de referencia. **No existen motores de prueba separados**: todas las pestañas leen resultados del mismo `MetricRunner` / `MetricRegistry` descrito arriba.

```bash
streamlit run panopto/dashboard/app.py
```

Se abre en `http://localhost:8501`.

### Pestañas

| Pestaña | Contenido | Fuente |
|---------|-----------|--------|
| **Summary Management** | Fila canónica "Summary Scoring Monitoring": Model, Scoring Dt, Vintage, Control Data Availability, Control Pre Scoring, PSI, PSI Variation, CSI Max, CSI, Population Scored, General Status. | `gcprmsbx_work.panopto_scoring_summary` |
| **Pre Scoring Summary** | Resultado (PASS/WARNING/FAIL) de los 3 chequeos oficiales: %Raw variables above median variation thresholds, Observation windows availability, %Raw sources with growth rt within Thresholds. | `panopto_metric_result` (`median_shift`, `population_variation`, `completeness` sobre `raw`/`input`) |
| **Data Availability** | Control por fuente (Schema, Source, Scoring Date, Deadline Data Ingestion, Vintage, Update Data Required, Control). | `gcprmsbx_work.panopto_data_availability` |
| **Median Raw Variables** | Mediana histórica por variable raw/input. | `panopto_variable_summary` (`statistic='p50'`) |
| **Raw Variable Distribution** | Percentiles de la última fecha por variable. | `panopto_variable_summary` |
| **Raw Sources** | Population Growth / Completeness / Null Rate por fuente en el tiempo. | `panopto_metric_result` |
| **Features & Score distribution** | Métricas y percentiles del score. | `panopto_metric_result`, `panopto_variable_summary` |
| **CSI** | `psi_canonical` por variable raw/input (Characteristic Stability Index). | `panopto_metric_result` |
| **PSI** | `psi_canonical` (PSI) y `psi_dynamic` (PSI Variation) del score. | `panopto_metric_result` |
| **About** | Descripción de las fuentes y del semáforo. | — |

### Control Pre Scoring y Population Scored (mismo motor)

`MetricRunner` reutiliza el mismo `MetricRegistry` para calcular las 3 pruebas oficiales de Pre Scoring y "Population Scored", sin agregar un motor de pruebas separado:

- `median_shift` (`panopto/metrics/quality.py`): variación relativa de la mediana vs. baseline por variable `raw`/`input` numérica → "%Raw variables above median variation thresholds".
- `completeness` (`panopto/metrics/quality.py`): fracción de fechas esperadas (ventana de observación, según `history_months`/`lag`) sin datos → "Observation windows availability".
- `population_variation` (`panopto/metrics/quality.py`): `|población_baseline / población_actual - 1|`, aplicada a `raw`/`input` (→ "%Raw sources with growth rt within Thresholds") y a `score` (→ "Population Scored").

Los tres se registran igual que cualquier otra métrica (`MetricRegistry.register(...)`) y sus umbrales se configuran igual que las demás, vía `gcprmsbx_work.panopto_metric_threshold_auto` o los defaults de `DEFAULT_THRESHOLDS` en `panopto/metrics/runner.py`.

`panopto/metrics/summary.ScoringSummaryBuilder` construye la fila canónica agregando esos mismos `MetricResult` (no recalcula nada): PSI/PSI Variation salen de `psi_canonical`/`psi_dynamic` sobre `score`; CSI/CSI Max salen de `psi_canonical` sobre `raw`/`input`; Control Pre Scoring es `Reprocess` si alguno de los 3 chequeos anteriores rompe su umbral (status `RED`); Population Scored es el conteo real de la población scoreada (`count_total` en `panopto_variable_summary`).

### Data Availability Monitoring

`MetricRunner.check_data_availability(model_id, information_date)` reutiliza la misma configuración (`panopto_model_table_config`) y el mismo `DataReader` para comparar, por cada fuente única (`source_schema`.`source_table`), la fecha máxima disponible contra `deadline_days` (nueva columna de `panopto_model_table_config`, días tras el fin del periodo/vintage para exigir la ingesta). El resultado (`Latest Data Updated` / `Data ingestion in progress` / `Data Ingestation has not met the deadline` / `Not enough data to process the models`) se persiste en `gcprmsbx_work.panopto_data_availability`.

### Backfill histórico

Para mostrar meses anteriores en el dashboard, se debe ejecutar backfill del DAG `panopto_production_runner` a las fechas deseadas. Cada corrida genera los resultados de `panopto_metric_result`, `panopto_alert_aggregate`, `panopto_execution_log`, `panopto_scoring_summary` y `panopto_data_availability` para ese `information_date`.

```bash
airflow dags backfill panopto_production_runner \
  --start-date 2026-01-07 \
  --end-date 2026-08-07 \
  --reset-dagruns
```

Por defecto, el dashboard ya inicia el selector de fechas en el rango completo del modelo seleccionado.

## Restructuración de tablas de configuración

La configuración se divide ahora en dos tablas principales:

- **`gcprmsbx_work.panopto_model_table_config`**: toda la configuración a nivel tabla (conexión, llaves, `partition_columns`, `reading_mode`, `history_months`, `lag`, `sql_transform`, `date_column`, `date_format`).
- **`gcprmsbx_work.panopto_variable_metadata`**: solo atributos de la variable (`variable`, `var_type`, `data_type`, `source_table`, `source_column`, `is_monotonic`). La relación con la tabla se hace por `source_table`.

La configuración de correo también vive en Hive: **`gcprmsbx_work.panopto_email_config`** (`model_id=global` para remitente global). `EmailDispatcher` primero lee Hive y, si no hay fila, cae a `PANOPTO_EMAIL_CONFIG_PATH` (JSON).

Ver documentación detallada en:

- `notebook/panopto_tablas.md` — campos, tipos y ejemplos de cada tabla.
- `notebook/panopto_metricas.md` — catálogo completo de métricas.

### Estructura

- `panopto/dashboard/data.py`: conexión a Spark y lectura de `panopto_scoring_summary`, `panopto_data_availability`, `panopto_metric_result` y `panopto_variable_summary`.
- `panopto/dashboard/app.py`: aplicación Streamlit con `st.tabs()` y Plotly, una pestaña por control oficial.
- `panopto/dashboard/builder.py`: vistas auxiliares (`panopto_dashboard_semaphore`, `panopto_dashboard_model_summary`) usadas por reportes de tendencia adicionales fuera del tablero oficial.
