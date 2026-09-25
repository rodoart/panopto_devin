# Cuaderno de requisitos tecnológicos vs. PANOPTO actual

**Origen:** imágenes en `tmp/requerimientos_tecnologia/`.  
**Proyecto analizado:** `/home/rodolfo/projects/panopto_devin` (módulo `panopto`).  
**Fecha de análisis:** 2026-09-25.

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
| **R1** | **Disponibilidad de datos** | 🟡 **Parcial** | `MetricRunner` levanta `MissingDataError` cuando no encuentra datos para una variable y `panopto_production_runner` registra `MISSING_DATA` en `panopto_execution_log`. Sin embargo, no hay un módulo explícito que, por cada fuente, compare `expected_date` vs `actual_date` y asigne GREEN/YELLOW/RED *antes* de correr. No se genera la tabla de estados de disponibilidad por fuente tal como se propone. |
| **R2** | **Pre-Scoring Validation** | 🔴 **No / Parcial** | No existe un paso de "Pre-Scoring" con las tres validaciones: *Population Growth*, *Median Shift* y *Completeness* con estatus PASS/WARNING/FAIL. El proyecto tiene métricas relacionadas (`null_rate`, `outlier_rate`, resúmenes de `VariableSummaryBuilder`), pero no la validación previa unificada. |
| **R3** | **PSI** | 🟢 **Sí / Parcial** | Existen `PSICanonicalMetric`, `PSIDynamicMetric` (`panopto/metrics/stability.py`) y `PSIApprovedMetric`, `PSIRejectedMetric`, `PSITargetMetric` (`panopto/metrics/score.py`, `panopto/metrics/target.py`). Los umbrales se cargan de `panopto_tresholds_table`, `panopto_metric_threshold_auto` o `DEFAULT_THRESHOLDS`. El ejemplo de la imagen pedía `>0.25` rojo, pero el *default* es `0.20`; aunque es parametrizable, conviene alinearlo. |
| **R4** | **CSI** | 🟡 **Parcial / No** | No hay una clase `CSIMetric` o un cálculo por variable que reporte `% de variables que fallan`. El proyecto tiene `CategoryCompositionDriftMetric` y `PSICanonicalMetric` por variable, lo cual cubre parcialmente el *drift*, pero no reproduce exactamente la lógica de "CSI por feature y porcentaje crítico" del requerimiento. Los campos `trigger_csi_*` en `panopto_model_summary_csi_psi` sugieren que se contemplaba, pero no se encuentra el motor. |
| **R5** | **Population Scored** | 🔴 **No** | No se encontró ninguna métrica o módulo que calcule la variación de población (`población_anterior / población_actual - 1`). `panopto/metrics/summary.py` cuenta registros por variable, pero no compara volúmenes entre periodos. |
| **R6** | **Motor de estados (semáforo)** | 🟢 **Sí** | `Metric._make_result` (`panopto/metrics/base.py`) asigna `OK` / `AMBAR` / `RED` por métrica. `AlertAggregator.aggregate` (`panopto/alerts/aggregator.py`) agrupa y calcula un estatus por `var_type`. `EmailDispatcher._overall_status` y el dashboard usan la regla "peor estatus gana", coincidiendo con el requerimiento. |
| **R7** | **Parametrización de umbrales** | 🟢 **Sí** | Los umbrales se cargan desde tablas (`panopto_tresholds_table`, `panopto_metric_threshold_auto`, `panopto_alert_policy`) o se usan `DEFAULT_THRESHOLDS` como fallback. La estructura `MetricResult` guarda `threshold_ambar` y `threshold_red` por corrida. |
| **R8** | **Dashboard en Tableau** | 🟡 **Parcial** | El proyecto tiene un dashboard en **Streamlit** (`panopto/dashboard/app.py`) con KPIs, tendencias, gráficas de pastel/barras y resúmenes. No es Tableau. Además, el dashboard consulta `panopto_dashboard_semaphore` y `panopto_dashboard_model_summary`, pero estas tablas **no aparecen en `sql/ddl_hive.sql`**, por lo que faltan las vistas/tablas respaldo. |
| **R9** | **Alertas con priorización y SLA** | 🟡 **Parcial** | Existe `EmailDispatcher` (`panopto/alerts/dispatcher.py`) que envía correos según contactos del modelo y lista roja. Sin embargo, no hay clasificación de modelos por riesgo (LOW/MEDIUM/HIGH) ni cálculo de SLA (24h, 5-7 días, 7-30 días). Las alertas no heredan prioridad automáticamente. |
| **R10** | **Diseño técnico por capas** | 🟡 **Parcial** | El proyecto tiene equivalentes a las tres capas: configuración (`panopto_model_summary_csi_psi`, `panopto_model_table_config`, `panopto_variable_metadata`, `panopto_alert_policy`), métricas (`panopto_metric_result`) y alertas (`panopto_alert_aggregate`, `panopto_email_log`). Las tablas se centralizan en `config/tables.json`. La nomenclatura no coincide exactamente con `model_config`, `monitoring_metrics` y `monitoring_alerts`, pero el modelo es funcionalmente cercano. |
| **R11** | **8 preguntas diarias** | 🟡 **Parcial** | El dashboard y los reportes por correo permiten responder parcialmente las preguntas, pero no hay un módulo explícito de "preguntas del día" ni un resumen estructurado con las 8 respuestas. |

---

## 3. Detalles por área

### 3.1 Data Availability (R1)

**Requerimiento:** Control por fuente con `expected_date`, `actual_date`, estatus GREEN/YELLOW/RED.

**Estado actual:** El flujo reacciona a la ausencia de datos, pero no monitorea proactivamente cada fuente.

**Archivos relevantes:**
- `panopto/data/reader.py` — lee fuentes y aplica `sql_transform`, formatea fechas.
- `panopto/metrics/runner.py` — levanta `MissingDataError` si no hay datos para una variable.
- `panopto/dags/panopto_production_runner.py` — captura `MissingDataError` y escribe `MISSING_DATA` en `panopto_execution_log`.
- `panopto/dags/panopto_alert_dispatcher.py` — envía alerta `MISSING_DATA` cuando el log indica datos faltantes.

**Gap principal:** no existe una tabla `panopto_staging_control` poblada con estados de disponibilidad por fuente ni un módulo que evalúe la demora vs. la fecha esperada antes de leer métricas.

### 3.2 Pre-Scoring Validation (R2)

**Requerimiento:** Population Growth, Median Shift, Completeness con PASS/WARNING/FAIL.

**Estado actual:** no hay un paso previo unificado. Las métricas de calidad existentes (`null_rate`, `outlier_rate`, `category_composition_drift`, etc.) corren en la misma etapa que las demás.

**Archivos relevantes:**
- `panopto/metrics/quality.py` — contiene `NullRateMetric`, `OutlierRateMetric`, `DominantCategoryRateMetric`, `CategoryCompositionDriftMetric`.
- `panopto/metrics/summary.py` — `VariableSummaryBuilder` genera estadísticos básicos (count, mean, percentiles) que podrían alimentar Pre-Scoring.

**Recomendación:** crear un `PreScoringValidator` que use `VariableSummaryBuilder` y calcule las tres pruebas antes de invocar `MetricRunner`.

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

**Estado actual:** no hay un motor CSI dedicado.

**Archivos relevantes:**
- `panopto/metrics/stability.py` — `PSICanonicalMetric` / `PSIDynamicMetric` podrían calcular el PSI de cada variable.
- `panopto/metrics/quality.py` — `CategoryCompositionDriftMetric` mide cambio de categorías principales (Jaccard), un proxy de CSI.
- `config/output_schemas.json` — `panopto_model_summary_csi_psi` incluye `trigger_csi_ambar`, `trigger_csi_red`, `trigger_csi_variation_*`, lo que indica que se planificó, pero el cálculo no se encontró en los módulos revisados.

### 3.5 Population Scored (R5)

**Requerimiento:** `|pob_base / pob_actual - 1|` con umbrales 15% / 30%.

**Estado actual:** no implementado. Ninguna clase contiene la palabra `population` en `panopto/`.

**Recomendación:** agregar `PopulationVariationMetric` en el registro de métricas, leyendo `count_total` de `VariableSummaryBuilder` o calculando `count()` del `score_df` vs. `score_baseline`.

### 3.6 Motor de estados (R6)

**Requerimiento:** Semáforo GREEN/AMBAR/RED por métrica y estado general.

**Estado actual:** implementado.

**Archivos relevantes:**
- `panopto/metrics/base.py` — `Metric._make_result` define `RED`, `AMBAR`, `OK`.
- `panopto/alerts/aggregator.py` — `AlertAggregator.aggregate` calcula `stress_ratio` y `aggregate_status` por `var_type`.
- `panopto/alerts/dispatcher.py` — `_overall_status` devuelve el peor estatus del conjunto.

### 3.7 Dashboard (R8)

**Requerimiento:** Dashboard en Tableau con múltiples vistas y gráficas históricas.

**Estado actual:** dashboard en Streamlit con funcionalidades básicas.

**Archivos relevantes:**
- `panopto/dashboard/app.py` — página principal con selectores de modelo, rango de fechas, KPIs, tendencias y tablas.
- `panopto/dashboard/data.py` — `DashboardData` lee `panopto_dashboard_semaphore` y `panopto_dashboard_model_summary`.
- `config/tables.json` y `sql/ddl_hive.sql` — las tablas `dashboard_*` aparecen en el JSON de configuración pero **no en el DDL**.

**Gaps:**
- No es Tableau.
- Faltan las tablas/vistas `panopto_dashboard_semaphore` y `panopto_dashboard_model_summary` en el DDL.
- No hay vistas separadas de Pre-Scoring, Data Availability o Stability Monitoring con gráficas de 12 meses.

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
| **Motor de métricas** | Semáforo por métrica, agregación de alertas, PSI, métricas de calidad, score y conjugadas. | CSI por variable, Population Scored. |
| **Disponibilidad de datos** | Detección reactiva de datos faltantes (`MissingDataError`, `MISSING_DATA`). | Monitoreo proactivo por fuente con fecha esperada vs. recibida y tabla de estados. |
| **Pre-Scoring** | Métricas de calidad aisladas. | Validación previa unificada: *Population Growth*, *Median Shift*, *Completeness*. |
| **Dashboard** | Streamlit con KPIs, tendencias, filtros. | Migración/integración a Tableau (o reemplazo explícito), definición de tablas `dashboard_*` en DDL y vistas de 12 meses. |
| **Alertas** | Email por estatus, contactos y lista roja. | Priorización por riesgo, SLA y tabla de `monitoring_alerts` con cierre/resolución. |

### Recomendaciones priorizadas

1. **Implementar `PopulationVariationMetric`** para cubrir R5.
2. **Crear motor de CSI** (por variable y agregado) para cubrir R4.
3. **Crear `DataAvailabilityMonitor`** que compare fechas esperadas vs. recibidas y escriba la tabla de disponibilidad (R1).
4. **Crear `PreScoringValidator`** con las tres validaciones PASS/WARNING/FAIL (R2).
5. **Definir en DDL y poblar `panopto_dashboard_semaphore` y `panopto_dashboard_model_summary`**, o ajustar `panopto/dashboard/data.py` a tablas existentes (R8).
6. **Añadir `riesgo` a `panopto_model_summary_csi_psi`** y lógica de SLA en `panopto_alert_dispatcher` (R9).
7. **Revisar y documentar los umbrales por defecto** de PSI para que coincidan con 0.10/0.25 si es necesario (R3, R7).

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
