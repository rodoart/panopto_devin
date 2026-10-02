# Cuaderno de requisitos tecnológicos vs. PANOPTO actual

**Origen:** imágenes en `tmp/requerimientos_tecnologia/`.  
**Proyecto analizado:** `panopto_devin` (módulo `panopto`).  
**Fecha de análisis:** 2026-09-25.  
**Última revisión contra el código:** 2026-10-01 (se actualizan R1, R2, R4, R5, R6 y R8, ya cubiertos por `MetricRunner.check_data_availability`, `panopto_data_availability`, las métricas `median_shift`/`population_variation`/`completeness`, `ScoringSummaryBuilder` y las vistas del dashboard).

---

## 1. Resumen de requisitos extraídos de las imágenes

De las capturas se identifican los siguientes requisitos funcionales y técnicos para una plataforma de monitoreo de modelos de scoring:

| # | Requisito | Descripción clave | Imágenes de origen |
|---|-----------|-------------------|--------------------|
| R1 | **Disponibilidad de datos** | Verificar que las fuentes estén actualizadas antes de correr. Estatus: GREEN (todo al día), YELLOW (falta alguna fuente para DQR), RED (fuente no actualizada tras la fecha de scoring). Incluye fecha esperada, fecha recibida, demora. | `IMG_20260925_115028`, `IMG_20260925_115538`, `IMG_20260925_115651` |
| R2 | **Pre-Scoring Validation** | Validar anomalías antes de ejecutar el scoring: *Population Growth*, *Median Shift* y *Completeness*. Resultados: PASS / WARNING / FAIL. | `IMG_20260925_115556` |
| R3 | **PSI (Population Stability Index)** | Detectar cambios en la distribución del score. Umbrales sugeridos: `<0.10 GREEN`, `0.10-0.25 YELLOW`, `>0.25 RED`. Debe ser por bin canónico. | `IMG_20260925_115605` |
| R4 | **CSI (Characteristic Stability Index)** | Detectar *drift* por cada característica (variable) crítica. Implica calcular PSI por variable y luego el % de variables que fallan. Umbral ejemplo: `>0.25` crítico. | `IMG_20260925_115605`, `IMG_20260925_115621` |
| R5 | **Population Scored** | Controlar cambios de volumen de la población scoreada. Fórmula: `|población_anterior / población_actual - 1|`. Umbrales ejemplo: amarillo 15%, rojo 30%. | `IMG_20260925_115621` |
| R6 | **Motor de estados (semáforo)** | Cada métrica genera GREEN/YELLOW/RED. Estado general: si alguna métrica es RED → RED; si no, si alguna es YELLOW → YELLOW; si no, GREEN. | `IMG_20260925_115632`, `IMG_20260925_115639` |
| R7 | **Parametrización de umbrales** | Todos los umbrales deben ser parametrizables, sin valores *hardcodeados*. Ejemplo: `data_availability_yellow=1`, `data_availability_red=3`, `psi_warning=0.10`, `psi_issue=0.25`, `csi_warning=0.10`, `csi_issue=0.25`, `population_scored_warning=0.15`, `population_scored_issue=0.30`. | `IMG_20260925_115632` |
| R8 | **Dashboard en Tableau** | Vistas: Executive Summary (una fila por modelo, estados), Pre-Scoring Summary, Data Availability, Stability Monitoring (gráficas históricas de PSI, CSI, Population Scored), alertas. Tabs visibles: Summary, Pre Scoring, Data Availability, Median Raw Variables, Raw Variable Distribution, Raw Sources, Features & Score distribution, CSI, PSI. | `IMG_20260925_115001`, `IMG_20260925_115028`, `IMG_20260925_115651`, `IMG_20260925_115659` |
| R9 | **Sistema de alertas con priorización y SLA** | Clasificar modelos por riesgo (LOW / MEDIUM / HIGH). Los alertas heredan prioridad. SLA: HIGH < 24h, MEDIUM 5-7 días, LOW 7-30 días. Tipos: WARNING (PSI/CSI warning, demora moderada, anomalía de volumen) e ISSUE (sin datos, PSI/CSI crítico, modelo no ejecutable). | `IMG_20260925_115659`, `IMG_20260925_115708` |
| R10 | **Diseño técnico por capas** | Capa de configuración (`model_config`: modelo, riesgo, umbrales, fuentes requeridas). Capa de métricas (`monitoring_metrics`: model_name, execution_date, availability_status, prescoring_status, psi, psi_status, csi, csi_status, population_change, population_status). Capa de alertas (`monitoring_alerts`: alert_id, model_name, severity, metric, description, created_at, resolved_at). | `IMG_20260925_115719`, `IMG_20260925_115731` |
| R11 | **Resultado esperado: 8 preguntas diarias** | Ejemplo: "¿Qué modelos están en rojo?", "¿Qué variables están fallando?", "¿Se recibieron todas las fuentes?", etc. | `IMG_20260925_115731`, `IMG_20260925_115737` |

---

## 2. Mapeo al proyecto `panopto` actual

### 2.1 Componentes existentes relevantes

- Motor de métricas: `panopto/metrics/runner.py`, `panopto/metrics/base.py`, `panopto/metrics/quality.py`, `panopto/metrics/stability.py`, `panopto/metrics/score.py`, `panopto/metrics/target.py`, `panopto/metrics/conjugate.py`.
- Agregación de alertas: `panopto/alerts/aggregator.py`, `panopto/alerts/dispatcher.py`, `panopto/alerts/email_builder.py`.
- Dashboard: `panopto/dashboard/app.py` (Streamlit), `panopto/dashboard/data.py`.
- Configuración: `config/tables.json`, `panopto/config/model_tables.py`, `panopto/config/schemas.py`, `panopto/config/tables.py`, `sql/ddl_hive.sql`.
- Ejecución: `panopto/dags/panopto_production_runner.py`, `panopto/dags/panopto_alert_dispatcher.py`.

### 2.2 Matriz de cumplimiento

| ID | Requisito | ¿Cumple? | Evidencia / Gaps |
|----|-----------|----------|------------------|
| **R1** | **Disponibilidad de datos** | 🟢 **Sí** | `MetricRunner.check_data_availability` (`panopto/metrics/runner.py`) compara, por cada fuente única (`source_schema`.`source_table`), la fecha máxima disponible contra `deadline_days` (columna de `panopto_model_table_config`, días tras el fin del vintage). Los estados (`Latest Data Updated` / `Data ingestion in progress` / `Data Ingestation has not met the deadline` / `Not enough data to process the models`) se persisten en `gcprmsbx_work.panopto_data_availability` y se muestran en la pestaña "Data Availability" del dashboard y en `control_data_availability` de `panopto_scoring_summary`. Además persiste el mecanismo reactivo `MissingDataError` → `MISSING_DATA` en `panopto_execution_log`. |
| **R2** | **Pre-Scoring Validation** | 🟢 **Sí** | Las tres validaciones oficiales existen como métricas del mismo `MetricRegistry` (`PRE_SCORING_METRICS` en `panopto/metrics/runner.py`): `median_shift` (Median Shift), `population_variation` (Population Growth) y `completeness` (Observation windows availability), en `panopto/metrics/quality.py`. `ScoringSummaryBuilder` las agrega en `control_pre_scoring` (`OK` / `Reprocess`) y el dashboard las expone en la pestaña "Pre Scoring Summary" con estatus PASS/WARNING/FAIL. |
| **R3** | **PSI** | 🟢 **Sí / Parcial** | Existen `PSICanonicalMetric`, `PSIDynamicMetric` (`panopto/metrics/stability.py`) y `PSIApprovedMetric`, `PSIRejectedMetric`, `PSITargetMetric` (`panopto/metrics/score.py`, `panopto/metrics/target.py`). Los umbrales se cargan de `panopto_tresholds_table`, `panopto_metric_threshold_auto` o `DEFAULT_THRESHOLDS`. El ejemplo de la imagen pedía `>0.25` rojo, pero el *default* es `0.20`; aunque es parametrizable, conviene alinearlo. |
| **R4** | **CSI** | 🟢 **Sí** | El CSI se calcula como `psi_canonical` por variable `raw`/`input` sobre los bins canónicos de `panopto_csi_psi_table`. `ScoringSummaryBuilder` (`panopto/metrics/summary.py`) agrega el máximo en `csi_max` y deriva `csi_status` (`OK`/`Stop`) con los campos `trigger_csi_*` de `panopto_model_summary_csi_psi`; el dashboard tiene pestaña **CSI** dedicada. No hay una clase `CSIMetric` separada ni un "% de variables críticas" explícito, pero el requerimiento queda cubierto funcionalmente. |
| **R5** | **Population Scored** | 🟢 **Sí** | `PopulationVariationMetric` (`panopto/metrics/quality.py`) implementa `|población_baseline / población_actual − 1|` con umbrales por defecto 15%/30% (coincidentes con el ejemplo). Corre sobre `score` (Population Scored) y sobre `raw`/`input` (Population Growth del Pre Scoring). El conteo real se expone en `population_scored` de `panopto_scoring_summary`. |
| **R6** | **Motor de estados (semáforo)** | 🟢 **Sí** | `Metric._make_result` (`panopto/metrics/base.py`) asigna `GREEN` / `AMBAR` / `RED` (o `NOT_APPLICABLE` cuando no hay umbrales) por métrica. `AlertAggregator.aggregate` (`panopto/alerts/aggregator.py`) agrupa y calcula un estatus por `var_type`. `EmailDispatcher._overall_status` y el dashboard usan la regla "peor estatus gana", coincidiendo con el requerimiento. |
| **R7** | **Parametrización de umbrales** | 🟢 **Sí** | Los umbrales se cargan desde tablas (`panopto_tresholds_table`, `panopto_metric_threshold_auto`, `panopto_alert_policy`) o se usan `DEFAULT_THRESHOLDS` como fallback. La estructura `MetricResult` guarda `threshold_ambar` y `threshold_red` por corrida. |
| **R8** | **Dashboard en Tableau** | 🟡 **Parcial** | El proyecto tiene un dashboard en **Streamlit** (`panopto/dashboard/app.py`) que reproduce las pestañas oficiales (Summary Management, Pre Scoring Summary, Data Availability, Median Raw Variables, Raw Variable Distribution, Raw Sources, Features & Score distribution, CSI, PSI, About) leyendo `panopto_scoring_summary`, `panopto_data_availability`, `panopto_metric_result` y `panopto_variable_summary`. No es Tableau. Las vistas `panopto_dashboard_semaphore` y `panopto_dashboard_model_summary` ya no faltan: las crea `panopto/dashboard/builder.py` con `CREATE OR REPLACE VIEW` desde el DAG `panopto_output_validator` (por eso no aparecen en `sql/ddl_hive.sql`). |
| **R9** | **Alertas con priorización y SLA** | 🟡 **Parcial** | Existe `EmailDispatcher` (`panopto/alerts/dispatcher.py`) que envía correos según contactos del modelo y lista roja. Sin embargo, no hay clasificación de modelos por riesgo (LOW/MEDIUM/HIGH) ni cálculo de SLA (24h, 5-7 días, 7-30 días). Las alertas no heredan prioridad automáticamente. |
| **R10** | **Diseño técnico por capas** | 🟡 **Parcial** | El proyecto tiene equivalentes a las tres capas: configuración (`panopto_model_summary_csi_psi`, `panopto_model_table_config`, `panopto_variable_metadata`, `panopto_alert_policy`), métricas (`panopto_metric_result`) y alertas (`panopto_alert_aggregate`, `panopto_email_log`). Las tablas se centralizan en `config/tables.json`. La nomenclatura no coincide exactamente con `model_config`, `monitoring_metrics` y `monitoring_alerts`, pero el modelo es funcionalmente cercano. |
| **R11** | **8 preguntas diarias** | 🟡 **Parcial** | El dashboard y los reportes por correo permiten responder parcialmente las preguntas, pero no hay un módulo explícito de "preguntas del día" ni un resumen estructurado con las 8 respuestas. |

---

## 3. Detalles por área

### 3.1 Data Availability (R1)

**Requerimiento:** Control por fuente con `expected_date`, `actual_date`, estatus GREEN/YELLOW/RED.

**Estado actual:** implementado. `MetricRunner.check_data_availability` evalúa proactivamente cada fuente antes de/al correr métricas, comparando la fecha máxima disponible contra `deadline_days` (columna de `panopto_model_table_config`, días tras el fin del vintage).

**Archivos relevantes:**
- `panopto/metrics/runner.py` — `check_data_availability` genera una fila por fuente única (`source_schema`.`source_table`) y `MissingDataError` sigue marcando variables sin datos.
- `panopto/dags/panopto_production_runner.py` — persiste las filas en `gcprmsbx_work.panopto_data_availability` y `MISSING_DATA` en `panopto_execution_log`.
- `panopto/metrics/summary.py` — `ScoringSummaryBuilder` consolida el control en `control_data_availability` (`DONE`/`PENDING`) de `panopto_scoring_summary`.
- `panopto/dashboard/app.py` — pestaña "Data Availability".
- `panopto/dags/panopto_alert_dispatcher.py` — envía alerta `MISSING_DATA` cuando el log indica datos faltantes.

**Gap residual:** los estados son de texto (`Latest Data Updated`, `Data ingestion in progress`, `Data Ingestation has not met the deadline`, `Not enough data to process the models`) en lugar del semáforo literal GREEN/YELLOW/RED del requerimiento; el dashboard los mapea visualmente.

### 3.2 Pre-Scoring Validation (R2)

**Requerimiento:** Population Growth, Median Shift, Completeness con PASS/WARNING/FAIL.

**Estado actual:** implementado como métricas del mismo motor (no como paso separado). Las tres pruebas oficiales son métricas registradas en `panopto/metrics/quality.py` y se ejecutan sobre variables `raw`/`input`: `median_shift` (Median Shift), `population_variation` (Population Growth) y `completeness` (Observation windows availability). El conjunto se marca con `PRE_SCORING_METRICS` en `panopto/metrics/runner.py`.

**Archivos relevantes:**
- `panopto/metrics/quality.py` — `MedianShiftMetric`, `PopulationVariationMetric`, `CompletenessMetric`.
- `panopto/metrics/summary.py` — `ScoringSummaryBuilder` agrega `control_pre_scoring` (`OK`/`Reprocess`) en `panopto_scoring_summary`.
- `panopto/dashboard/app.py` — pestaña "Pre Scoring Summary" con estatus PASS/WARNING/FAIL por chequeo.

**Nota:** se decidió reutilizar el `MetricRunner`/`MetricRegistry` en lugar de crear un `PreScoringValidator` separado; el resultado funcional es equivalente.

### 3.3 PSI (R3)

**Requerimiento:** PSI por población y/o score con umbrales canónicos.

**Estado actual:** múltiples implementaciones.

**Archivos relevantes:**
- `panopto/metrics/stability.py` — `PSICanonicalMetric` (bins predefinidos) y `PSIDynamicMetric` (bins dinámicos por percentiles).
- `panopto/metrics/score.py` — `PSIApprovedMetric` y `PSIRejectedMetric` (población aprobada/rechazada por `cut_off`).
- `panopto/metrics/target.py` — `PSITargetMetric` (distribución de la variable objetivo).

**Gap:** el umbral por defecto `psi_canonical` es `threshold_red=0.20`; la imagen sugiere `0.25`. Es configurable, pero conviene documentar la diferencia.

### 3.4 CSI (R4)

**Requerimiento:** PSI por variable, agregar % de variables críticas.

**Estado actual:** implementado funcionalmente. El CSI por variable es `psi_canonical` sobre bins canónicos (`panopto_csi_psi_table`); `ScoringSummaryBuilder` toma el máximo (`csi_max`) y lo clasifica con `trigger_csi_ambar`/`trigger_csi_red` en `csi_status` (`OK`/`Stop`).

**Archivos relevantes:**
- `panopto/metrics/stability.py` — `PSICanonicalMetric` calcula el PSI por variable contra los bins de entrenamiento.
- `panopto/metrics/summary.py` — `ScoringSummaryBuilder.build` agrega `csi_max`/`csi_status` en `panopto_scoring_summary`.
- `panopto/dashboard/app.py` — pestaña "CSI".
- `config/output_schemas.json` — `panopto_scoring_summary` incluye `csi_max` y `csi_status`.

**Gap residual:** no hay una clase `CSIMetric` ni un porcentaje de variables críticas; el agregado es el máximo, no un conteo de fallas.

### 3.5 Population Scored (R5)

**Requerimiento:** `|pob_base / pob_actual - 1|` con umbrales 15% / 30%.

**Estado actual:** implementado. `PopulationVariationMetric` (`panopto/metrics/quality.py`) calcula `|count_baseline / count_current − 1|` comparando directamente los DataFrames de `score` (Population Scored) y de `raw`/`input` (Population Growth). Umbrales por defecto: ambar 0.15, red 0.30.

### 3.6 Motor de estados (R6)

**Requerimiento:** Semáforo GREEN/AMBAR/RED por métrica y estado general.

**Estado actual:** implementado.

**Archivos relevantes:**
- `panopto/metrics/base.py` — `Metric._make_result` define `RED`, `AMBAR`, `GREEN` y `NOT_APPLICABLE`.
- `panopto/alerts/aggregator.py` — `AlertAggregator.aggregate` calcula `stress_ratio` y `aggregate_status` por `var_type`.
- `panopto/alerts/dispatcher.py` — `_overall_status` devuelve el peor estatus del conjunto.

### 3.7 Dashboard (R8)

**Requerimiento:** Dashboard en Tableau con múltiples vistas y gráficas históricas.

**Estado actual:** dashboard en Streamlit que reproduce la estructura oficial de pestañas.

**Archivos relevantes:**
- `panopto/dashboard/app.py` — Streamlit con `st.tabs()`: Summary Management, Pre Scoring Summary, Data Availability, Median Raw Variables, Raw Variable Distribution, Raw Sources, Features & Score distribution, CSI, PSI, About.
- `panopto/dashboard/data.py` — `DashboardData` lee `panopto_scoring_summary`, `panopto_data_availability`, `panopto_metric_result`, `panopto_variable_summary` y las vistas `panopto_dashboard_*`.
- `panopto/dashboard/builder.py` — `build_dashboard_views` crea las vistas `panopto_dashboard_semaphore` y `panopto_dashboard_model_summary` (invocado por el DAG `panopto_output_validator`).

**Gaps:**
- No es Tableau (decisión de diseño: el reemplazo por Streamlit quedó explícito).
- Las vistas `dashboard_*` son auxiliares para reportes de tendencia; el tablero oficial lee directamente las tablas de resultado.

### 3.8 Sistema de alertas y SLA (R9)

**Requerimiento:** Priorización por riesgo y SLA diferenciados.

**Estado actual:** despacho de correos implementado, sin priorización.

**Archivos relevantes:**
- `panopto/alerts/dispatcher.py` — `EmailDispatcher` construye destinatarios a partir de `model_contact` y `red_alert_list_d`, envía con SMTP.
- `panopto/alerts/aggregator.py` — `DEFAULT_ALERT_POLICY` define `red_equivalent`, `alert_ambar_pct` y `alert_red_pct` por `var_type`.
- `panopto/dags/panopto_alert_dispatcher.py` — DAG diario que dispara correos.

**Gaps:**
- No hay campo `riesgo` (LOW/MEDIUM/HIGH) en `panopto_model_summary_csi_psi` ni en `panopto_alert_policy`.
- No se respetan SLA por severidad.
- No se generan tickets con `created_at` / `resolved_at` en una tabla de `monitoring_alerts`.

### 3.9 Capas de configuración y métricas (R10)

**Requerimiento:** `model_config`, `monitoring_metrics`, `monitoring_alerts`.

**Estado actual:** el proyecto ya sigue un diseño en capas.

**Archivos relevantes:**
- `panopto/config/model_tables.py` — `ModelTableConfig` y `load_model_table_config_map`.
- `config/tables.json` — centraliza nombres de tablas.
- `sql/ddl_hive.sql` — DDLs de tablas de configuración, métricas, alertas y logs.

**Equivalencias aproximadas:**

| Requerido | Tabla existente en `panopto` | Coincidencia |
|-----------|------------------------------|--------------|
| `model_config` | `panopto_model_summary_csi_psi` + `panopto_model_table_config` + `panopto_alert_policy` | Parcial (falta `riesgo` y `fuentes requeridas` explícitas) |
| `monitoring_metrics` | `panopto_metric_result` + `panopto_alert_aggregate` + `panopto_execution_log` | Parcial (más granular) |
| `monitoring_alerts` | `panopto_alert_aggregate` + `panopto_email_log` | Parcial (falta `alert_id`, `resolved_at`) |

---

## 4. Conclusiones

| Categoría | ¿Qué ya cumple? | ¿Qué falta? |
|-----------|----------------|-------------|
| **Motor de métricas** | Semáforo por métrica, agregación de alertas, PSI, CSI (vía `psi_canonical` + `csi_max`/`csi_status`), Population Scored (`population_variation`), métricas de calidad, score y conjugadas. | `% de variables críticas` para CSI como agregado alternativo (hoy es el máximo). |
| **Disponibilidad de datos** | Monitoreo proactivo por fuente (`check_data_availability` + `panopto_data_availability` + `deadline_days`) y detección reactiva (`MissingDataError`, `MISSING_DATA`). | Estados en texto plano en lugar de semáforo literal GREEN/YELLOW/RED por fuente. |
| **Pre-Scoring** | Las tres validaciones oficiales corren como métricas del `MetricRegistry` (`median_shift`, `population_variation`, `completeness`), agregadas en `control_pre_scoring` y la pestaña "Pre Scoring Summary". | — |
| **Dashboard** | Streamlit con las pestañas oficiales sobre `panopto_scoring_summary`, `panopto_data_availability`, `panopto_metric_result` y `panopto_variable_summary`; vistas `dashboard_*` creadas por `builder.py`. | Migración/integración a Tableau si aún se requiere esa plataforma. |
| **Alertas** | Email por estatus, contactos y lista roja. | Priorización por riesgo, SLA y tabla de `monitoring_alerts` con cierre/resolución. |

### Recomendaciones priorizadas

1. ~~Implementar `PopulationVariationMetric`~~ — **hecho** (R5).
2. ~~Crear motor de CSI~~ — **hecho** funcionalmente: `psi_canonical` por variable + `csi_max`/`csi_status` en `panopto_scoring_summary` (R4).
3. ~~Crear `DataAvailabilityMonitor`~~ — **hecho**: `MetricRunner.check_data_availability` + `panopto_data_availability` (R1).
4. ~~Crear `PreScoringValidator`~~ — **hecho** como métricas del mismo motor + pestaña "Pre Scoring Summary" (R2).
5. ~~Definir `panopto_dashboard_semaphore` y `panopto_dashboard_model_summary`~~ — **hecho** como vistas (`panopto/dashboard/builder.py`, DAG `panopto_output_validator`) (R8).
6. **Añadir `riesgo` a `panopto_model_summary_csi_psi`** y lógica de SLA en `panopto_alert_dispatcher` (R9).
7. **Revisar y documentar los umbrales por defecto** de PSI para que coincidan con 0.10/0.25 si es necesario (R3, R7).
8. **Módulo explícito de "8 preguntas diarias"** o resumen estructurado si se requiere (R11).

---

## 5. Nota sobre las imágenes consultadas

Se revisaron las 15 imágenes en `tmp/requerimientos_tecnologia/`:

- `IMG_20260925_115001.jpg`
- `IMG_20260925_115028.jpg`
- `IMG_20260925_115530.jpg`
- `IMG_20260925_115538.jpg`
- `IMG_20260925_115556.jpg`
- `IMG_20260925_115605.jpg`
- `IMG_20260925_115621.jpg`
- `IMG_20260925_115632.jpg`
- `IMG_20260925_115639.jpg`
- `IMG_20260925_115651.jpg`
- `IMG_20260925_115659.jpg`
- `IMG_20260925_115708.jpg`
- `IMG_20260925_115719.jpg`
- `IMG_20260925_115731.jpg`
- `IMG_20260925_115737.jpg`
