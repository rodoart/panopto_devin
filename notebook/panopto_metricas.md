# Catálogo de métricas PANOPTO

Este notebook/documento describe cada métrica del framework: qué mide, a qué tipos de variable aplica, los umbrales por defecto y la fórmula/concepto.

## Índice

1. [Calidad de variables](#calidad-de-variables)
2. [Estabilidad](#estabilidad)
3. [Score](#score)
4. [Conjugadas (score + target)](#conjugadas)
5. [Target](#target)

---

## Calidad de variables

| Métrica | Variables | Tipo dato | Descripción | Umbrales por defecto |
|---------|-----------|-----------|-------------|----------------------|
| `null_rate` | raw, input, transformed, score, target | numeric/categorical | Proporción de nulos: `nulos / total` | ambar 0.05, red 0.10 |
| `cardinality_ratio` | raw, input, transformed | numeric/categorical | `distinct_count / total` | red 0.95 |
| `outlier_rate` | raw, input, transformed | numeric | Proporción de valores fuera de `Q1-1.5*IQR` y `Q3+1.5*IQR` | ambar 0.03, red 0.06 |
| `dominant_category_rate` | raw, input, transformed | categorical | Proporción de la categoría más frecuente | red 0.90 |
| `category_composition_drift` | raw, input, transformed | categorical | `1 - Jaccard(top N categorías current vs baseline)` | ambar 0.10, red 0.30 |

## Estabilidad

| Métrica | Variables | Tipo dato | Descripción | Umbrales por defecto |
|---------|-----------|-----------|-------------|----------------------|
| `psi_canonical` | raw, input, transformed, score | numeric/categorical | PSI contra bins definidos en entrenamiento | ambar 0.10, red 0.20 |
| `psi_dynamic` | raw, input, transformed, score | numeric/categorical | PSI contra bins calculados dinámicamente sobre el baseline | ambar 0.10, red 0.20 |
| `ks_vs_dev` | raw, input, transformed | numeric | Máxima diferencia acumulada entre la distribución de `current` y `baseline` | ambar 0.10, red 0.20 |
| `correlation_dift` | raw, input, transformed, score | numeric | Mayor `abs(corr)` entre variables numéricas | ambar 0.10, red 0.20 |

## Score

| Métrica | Variables | Tipo dato | Descripción | Umbrales por defecto |
|---------|-----------|-----------|-------------|----------------------|
| `range_violation` | score | numeric | Proporción de score fuera de `[0,1]` | red 1e-9 |
| `entropy` | score | numeric | Entropía del score discretizado en 10 bins; se reporta la variación vs baseline | ambar 0.15, red 0.30 |
| `approval_rate` | score | numeric | Proporción con `score > cut_off`; variación vs baseline | ambar 0.10, red 0.20 |
| `tail_shift` | score | numeric | Suma de desplazamientos del P10 y P90 vs baseline | ambar 0.05, red 0.10 |
| `concentration_gini` | score | numeric | Gini sobre la concentración del score; variación vs baseline | ambar 0.10, red 0.20 |
| `psi_approved` | score | numeric | PSI de aprobados (`score > cut_off`) vs baseline | ambar 0.10, red 0.20 |
| `psi_rejected` | score | numeric | PSI de rechazados (`score <= cut_off`) vs baseline | ambar 0.10, red 0.20 |

## Conjugadas (score + target)

Requieren unir `score` con `target` usando las `canonical_key_columns`.

| Métrica | Variables | Tipo target | Descripción | Umbrales por defecto |
|---------|-----------|-------------|-------------|----------------------|
| `auc` | score/target | binario | Área bajo la curva ROC del score sobre target | - (sin umbrales) |
| `gini` | score/target | binario | `2*AUC - 1`; variación vs baseline | ambar 0.05, red 0.10 |
| `brier_score` | score/target | binario | MSE entre score y target | ambar 0.10, red 0.20 |
| `lift_top_decile` | score/target | binario | Tasa de eventos en el top decil / tasa global; variación vs baseline | ambar 0.10, red 0.20 |
| `calibration_slope` | score/target | binario | Pendiente de la regresión `target ~ score` | ambar 0.10, red 0.20 |
| `ks_score_target` | score/target | binario | KS entre las distribuciones de score para target=0 vs target=1 | ambar 0.05, red 0.10 |

## Target

| Métrica | Variables | Tipo dato | Descripción | Umbrales por defecto |
|---------|-----------|-----------|-------------|----------------------|
| `event_rate` | target | binario | Media del target; variación vs baseline | ambar 0.20, red 0.40 |
| `psi_target` | target | numeric/categorical | PSI de la distribución del target vs baseline | ambar 0.05, red 0.10 |

---

## Notas sobre umbrales

- `threshold_ambar` y `threshold_red` se definen en `metric_threshold_auto_d_t_d` (entrenamiento) o `tresholds_table_d_t_d` (manual).
- Si solo existe `threshold_red`, cualquier valor >= ese valor es `RED`; lo demás es `GREEN`.
- Si no existe ningún umbral, el estado es `NOT_APPLICABLE`.
- Métricas `gini`, `brier_score`, `lift_top_decile`, `calibration_slope`, `ks_score_target`, `event_rate`, `approval_rate`, `tail_shift`, `concentration_gini` requieren baseline. Si no hay baseline, se omiten.
