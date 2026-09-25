"""Aplicación de Streamlit para el dashboard PANOPTO.

Reproduce, con la misma nomenclatura oficial, la estructura de pestañas del
tablero de referencia (antes en Tableau): Summary Management, Pre Scoring
Summary, Data Availability, Median Raw Variables, Raw Variable Distribution,
Raw Sources, Features & Score distribution, CSI, PSI y About.

Todos los datos provienen del mismo motor de métricas (``panopto.metrics``):
no existen motores de prueba separados. La pestaña "Summary Management" lee
la fila canónica ``panopto_scoring_summary`` (construida por
``ScoringSummaryBuilder`` a partir de los ``MetricResult`` del
``MetricRunner``); el resto de pestañas leen ``panopto_metric_result`` /
``panopto_variable_summary`` / ``panopto_data_availability`` directamente.
"""

from datetime import date

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from panopto.config.tables import PROCESS_CONFIG
from panopto.dashboard.data import DashboardData

st.set_page_config(
    page_title="PANOPTO Dashboard",
    page_icon="📊",
    layout="wide",
)

if "data" not in st.session_state:
    st.session_state.data = DashboardData()

data = st.session_state.data

# Estatus -> color, coherentes con el semáforo del motor de métricas
# (Metric._make_result: RED / AMBAR / OK) y con los controles oficiales
# (DONE / OK / Stop / Reprocess / PENDING / WARNING).
STATUS_COLORS = {
    "RED": "#e74c3c",
    "REPROCESS": "#e74c3c",
    "STOP": "#e74c3c",
    "FAIL": "#e74c3c",
    "PENDING": "#e74c3c",
    "NOT ENOUGH DATA TO PROCESS THE MODELS": "#e74c3c",
    "DATA INGESTATION HAS NOT MET THE DEADLINE": "#e67e22",
    "AMBAR": "#e67e22",
    "WARNING": "#e67e22",
    "DATA INGESTION IN PROGRESS": "#f1c40f",
    "DQR PROCESS PENDING": "#f1c40f",
    "OK": "#2ecc71",
    "GREEN": "#2ecc71",
    "PASS": "#2ecc71",
    "DONE": "#2ecc71",
    "LATEST DATA UPDATED": "#2ecc71",
}


def _status_style(val: object) -> str:
    """Devuelve el color de fondo para una celda de estatus."""
    color = STATUS_COLORS.get(str(val).upper())
    return f"background-color: {color}; color: white;" if color else ""


def _style_status_columns(df: pd.DataFrame, columns: list) -> "pd.io.formats.style.Styler":
    """Aplica ``_status_style`` a las columnas de estatus indicadas."""
    cols = [c for c in columns if c in df.columns]
    return df.style.applymap(_status_style, subset=cols)


def _severity_from_metric_status(status: str) -> str:
    """Traduce el semáforo del motor (GREEN/AMBAR/RED) a PASS/WARNING/FAIL."""
    return {"GREEN": "PASS", "AMBAR": "WARNING", "RED": "FAIL"}.get(str(status).upper(), str(status))


st.sidebar.title("🔧 PANOPTO")
st.sidebar.caption("Monitoreo de Estabilidad y Calidad de Variables")

models = data.get_models()
selected_model = st.sidebar.selectbox(
    "Modelo",
    models if models else ["1079_cta_lvl"],
)

min_date_str, max_date_str = data.get_date_range(selected_model)
start_default = date.fromisoformat(min_date_str) if min_date_str else date.today()
end_default = date.fromisoformat(max_date_str) if max_date_str else date.today()
start, end = st.sidebar.date_input(
    "Rango de fechas",
    value=(start_default, end_default),
    min_value=start_default,
    max_value=end_default,
)
start_iso, end_iso = start.isoformat(), end.isoformat()

st.title(f"📊 PANOPTO — Model Scoring Monitoring — {selected_model}")

(
    tab_summary,
    tab_pre_scoring,
    tab_data_availability,
    tab_median_raw,
    tab_raw_distribution,
    tab_raw_sources,
    tab_features_score,
    tab_csi,
    tab_psi,
    tab_about,
) = st.tabs(
    [
        "Summary Management",
        "Pre Scoring Summary",
        "Data Availability",
        "Median Raw Variables",
        "Raw Variable Distribution",
        "Raw Sources",
        "Features & Score distribution",
        "CSI",
        "PSI",
        "About",
    ]
)

# ---------------------------------------------------------------------------
# Summary Management — vista canónica "Summary Scoring Monitoring"
# ---------------------------------------------------------------------------
with tab_summary:
    st.subheader("Summary Scoring Monitoring")
    st.caption(
        "Controls — El objetivo es verificar cada control antes de procesar los modelos "
        "(Data Availability, Pre Scoring, PSI / PSI Variation, CSI, Population Scored)."
    )
    with st.spinner("Cargando resumen canónico..."):
        summary_df = data.get_scoring_summary(selected_model, start_iso, end_iso)

    if summary_df.empty:
        st.warning("No hay filas en panopto_scoring_summary para el modelo y rango seleccionados.")
    else:
        latest = summary_df.sort_values("information_date").iloc[-1]
        col1, col2, col3, col4, col5 = st.columns(5)
        col1.metric("Scoring Dt", latest["scoring_date"])
        col2.metric("Control Data Availability", latest["control_data_availability"])
        col3.metric("Control Pre Scoring", latest["control_pre_scoring"])
        col4.metric("CSI", latest["csi_status"])
        col5.metric("General Status", latest["general_status"])

        display_cols = [
            "model_id",
            "scoring_date",
            "vintage",
            "control_data_availability",
            "control_pre_scoring",
            "psi",
            "psi_variation",
            "csi_max",
            "csi_status",
            "population_scored",
            "general_status",
        ]
        display_cols = [c for c in display_cols if c in summary_df.columns]
        st.dataframe(
            _style_status_columns(
                summary_df[display_cols].sort_values("scoring_date", ascending=False),
                ["control_data_availability", "control_pre_scoring", "csi_status", "general_status"],
            ),
            use_container_width=True,
        )

        fig = go.Figure()
        fig.add_trace(
            go.Scatter(x=summary_df["information_date"], y=summary_df["psi"], name="PSI", line=dict(color="blue"))
        )
        fig.add_trace(
            go.Scatter(
                x=summary_df["information_date"],
                y=summary_df["psi_variation"],
                name="PSI Variation",
                line=dict(color="purple", dash="dot"),
            )
        )
        fig.add_trace(
            go.Scatter(
                x=summary_df["information_date"], y=summary_df["csi_max"], name="CSI Max", line=dict(color="orange")
            )
        )
        fig.update_layout(
            title="Stability Monitoring — PSI / PSI Variation / CSI Max",
            xaxis_title="Scoring Dt",
            yaxis_title="Valor",
            template="plotly_white",
        )
        st.plotly_chart(fig, use_container_width=True)

        fig_pop = px.bar(
            summary_df,
            x="information_date",
            y="population_scored",
            title="Population Scored",
            template="plotly_white",
        )
        st.plotly_chart(fig_pop, use_container_width=True)

    st.caption(
        "Para más información sobre fechas de ejecución de los modelos, consulte el calendario "
        f"({PROCESS_CONFIG.banamex_calendar_table})."
    )

# ---------------------------------------------------------------------------
# Pre Scoring Summary — Population Growth / Median Shift / Completeness
# ---------------------------------------------------------------------------
with tab_pre_scoring:
    st.subheader("Pre Scoring Summary")
    st.caption(
        "El control Pre Scoring combina 3 chequeos: (1) %Raw variables above median variation "
        "thresholds, (2) Observation windows availability, (3) %Raw sources with growth rt within "
        "Thresholds. Si alguno rompe su umbral, el modelo requiere Reprocess."
    )
    pre_scoring_df = data.get_metric_results(
        selected_model,
        start_iso,
        end_iso,
        var_types=["raw", "input"],
        metric_names=["median_shift", "population_variation", "completeness"],
    )
    if pre_scoring_df.empty:
        st.warning("No hay resultados de median_shift / population_variation / completeness todavía.")
    else:
        pre_scoring_df = pre_scoring_df.copy()
        pre_scoring_df["result"] = pre_scoring_df["status"].map(_severity_from_metric_status)
        label_map = {
            "median_shift": "Median Shift (Population Growth check)",
            "population_variation": "Population Growth (Raw Sources)",
            "completeness": "Completeness (Observation windows availability)",
        }
        pre_scoring_df["check"] = pre_scoring_df["metric_name"].map(label_map)

        summary_counts = (
            pre_scoring_df.groupby(["check", "result"]).size().reset_index(name="count")
        )
        fig = px.bar(
            summary_counts,
            x="check",
            y="count",
            color="result",
            color_discrete_map={"PASS": "#2ecc71", "WARNING": "#e67e22", "FAIL": "#e74c3c"},
            barmode="stack",
            title="Resultado de los 3 chequeos Pre Scoring por variable/fuente",
        )
        fig.update_layout(template="plotly_white")
        st.plotly_chart(fig, use_container_width=True)

        latest_date = pre_scoring_df["information_date"].max()
        latest = pre_scoring_df[pre_scoring_df["information_date"] == latest_date]
        st.dataframe(
            _style_status_columns(
                latest[["variable", "check", "metric_value", "baseline_value", "result"]]
                .sort_values("check"),
                ["result"],
            ),
            use_container_width=True,
        )

# ---------------------------------------------------------------------------
# Data Availability — control por fuente
# ---------------------------------------------------------------------------
with tab_data_availability:
    st.subheader("Data Availability Monitoring")
    st.caption(
        "Corrective actions: Latest Data Updated | Data ingestion in progress | "
        "Data Ingestation has not met the deadline | Not enough data to process the models"
    )
    availability_df = data.get_data_availability(selected_model, start_iso, end_iso)
    if availability_df.empty:
        st.warning("No hay datos en panopto_data_availability para el modelo y rango seleccionados.")
    else:
        latest_date = availability_df["information_date"].max()
        latest = availability_df[availability_df["information_date"] == latest_date]
        display_cols = [
            "model_id",
            "schema_name",
            "source_table",
            "scoring_date",
            "deadline_data_ingestion",
            "vintage",
            "update_data_required",
            "control",
        ]
        display_cols = [c for c in display_cols if c in latest.columns]
        st.dataframe(
            _style_status_columns(latest[display_cols], ["control"]),
            use_container_width=True,
        )

# ---------------------------------------------------------------------------
# Median Raw Variables
# ---------------------------------------------------------------------------
with tab_median_raw:
    st.subheader("Median Raw Variables")
    median_df = data.get_variable_summary(
        selected_model, start_iso, end_iso, var_types=["raw", "input"], statistics=["p50"]
    )
    if median_df.empty:
        st.warning("No hay estadístico p50 en panopto_variable_summary todavía.")
    else:
        variables = sorted(median_df["variable"].unique())
        selected_vars = st.multiselect("Variables", variables, default=variables[: min(5, len(variables))])
        plot_df = median_df[median_df["variable"].isin(selected_vars)] if selected_vars else median_df
        fig = px.line(
            plot_df,
            x="information_date",
            y="statistic_value",
            color="variable",
            title="Mediana histórica por variable raw/input",
            template="plotly_white",
        )
        st.plotly_chart(fig, use_container_width=True)

# ---------------------------------------------------------------------------
# Raw Variable Distribution
# ---------------------------------------------------------------------------
with tab_raw_distribution:
    st.subheader("Raw Variable Distribution")
    dist_df = data.get_variable_summary(
        selected_model,
        start_iso,
        end_iso,
        var_types=["raw", "input"],
        statistics=["p10", "p20", "p30", "p40", "p50", "p60", "p70", "p80", "p90", "min", "max", "mean"],
    )
    if dist_df.empty:
        st.warning("No hay percentiles en panopto_variable_summary todavía.")
    else:
        variables = sorted(dist_df["variable"].unique())
        selected_var = st.selectbox("Variable", variables)
        latest_date = dist_df["information_date"].max()
        var_df = dist_df[(dist_df["variable"] == selected_var) & (dist_df["information_date"] == latest_date)]
        fig = px.bar(
            var_df.sort_values("statistic"),
            x="statistic",
            y="statistic_value",
            title=f"Distribución de {selected_var} — {latest_date.date() if hasattr(latest_date, 'date') else latest_date}",
            template="plotly_white",
        )
        st.plotly_chart(fig, use_container_width=True)

# ---------------------------------------------------------------------------
# Raw Sources — Population Growth / Completeness agregados por fuente
# ---------------------------------------------------------------------------
with tab_raw_sources:
    st.subheader("Raw Sources")
    sources_df = data.get_metric_results(
        selected_model,
        start_iso,
        end_iso,
        var_types=["raw", "input"],
        metric_names=["population_variation", "completeness", "null_rate"],
    )
    if sources_df.empty:
        st.warning("No hay métricas de fuentes raw/input todavía.")
    else:
        fig = px.line(
            sources_df,
            x="information_date",
            y="metric_value",
            color="variable",
            facet_row="metric_name",
            title="Population Growth / Completeness / Null Rate por fuente",
            template="plotly_white",
        )
        fig.update_layout(height=700)
        st.plotly_chart(fig, use_container_width=True)

# ---------------------------------------------------------------------------
# Features & Score distribution
# ---------------------------------------------------------------------------
with tab_features_score:
    st.subheader("Features & Score distribution")
    score_metrics_df = data.get_metric_results(
        selected_model,
        start_iso,
        end_iso,
        var_types=["score"],
        metric_names=["entropy", "approval_rate", "concentration_gini", "population_variation", "range_violation"],
    )
    if not score_metrics_df.empty:
        fig = px.line(
            score_metrics_df,
            x="information_date",
            y="metric_value",
            color="metric_name",
            title="Métricas de distribución del score",
            template="plotly_white",
        )
        st.plotly_chart(fig, use_container_width=True)

    score_dist_df = data.get_variable_summary(
        selected_model,
        start_iso,
        end_iso,
        var_types=["score"],
        statistics=["p10", "p20", "p30", "p40", "p50", "p60", "p70", "p80", "p90"],
    )
    if not score_dist_df.empty:
        latest_date = score_dist_df["information_date"].max()
        latest = score_dist_df[score_dist_df["information_date"] == latest_date]
        fig2 = px.bar(
            latest.sort_values("statistic"),
            x="statistic",
            y="statistic_value",
            title="Distribución del score (percentiles) — última fecha",
            template="plotly_white",
        )
        st.plotly_chart(fig2, use_container_width=True)

    if score_metrics_df.empty and score_dist_df.empty:
        st.warning("No hay métricas ni distribución de score todavía.")

# ---------------------------------------------------------------------------
# CSI — PSI canónico por variable raw/input (Characteristic Stability Index)
# ---------------------------------------------------------------------------
with tab_csi:
    st.subheader("CSI — Characteristic Stability Index")
    st.caption("CSI reutiliza la misma métrica psi_canonical, aplicada a cada variable raw/input.")
    csi_df = data.get_metric_results(
        selected_model, start_iso, end_iso, var_types=["raw", "input"], metric_names=["psi_canonical"]
    )
    if csi_df.empty:
        st.warning("No hay resultados de psi_canonical para variables raw/input todavía.")
    else:
        fig = px.line(
            csi_df,
            x="information_date",
            y="metric_value",
            color="variable",
            title="CSI por variable (psi_canonical)",
            template="plotly_white",
        )
        fig.add_hline(y=0.10, line_dash="dot", line_color="orange", annotation_text="csi_warning")
        fig.add_hline(y=0.25, line_dash="dot", line_color="red", annotation_text="csi_issue")
        st.plotly_chart(fig, use_container_width=True)

        latest_date = csi_df["information_date"].max()
        latest = csi_df[csi_df["information_date"] == latest_date]
        st.dataframe(
            _style_status_columns(
                latest[["variable", "metric_value", "threshold_ambar", "threshold_red", "status"]],
                ["status"],
            ),
            use_container_width=True,
        )

# ---------------------------------------------------------------------------
# PSI — psi_canonical / psi_dynamic del score
# ---------------------------------------------------------------------------
with tab_psi:
    st.subheader("PSI — Population Stability Index (score)")
    psi_df = data.get_metric_results(
        selected_model, start_iso, end_iso, var_types=["score"], metric_names=["psi_canonical", "psi_dynamic"]
    )
    if psi_df.empty:
        st.warning("No hay resultados de psi_canonical / psi_dynamic para el score todavía.")
    else:
        fig = px.line(
            psi_df,
            x="information_date",
            y="metric_value",
            color="metric_name",
            title="PSI (canónico) y PSI Variation (dinámico) del score",
            template="plotly_white",
        )
        fig.add_hline(y=0.10, line_dash="dot", line_color="orange", annotation_text="psi_warning")
        fig.add_hline(y=0.25, line_dash="dot", line_color="red", annotation_text="psi_issue")
        st.plotly_chart(fig, use_container_width=True)

# ---------------------------------------------------------------------------
# About
# ---------------------------------------------------------------------------
with tab_about:
    st.subheader("About PANOPTO")
    st.markdown(
        """
        **PANOPTO** monitorea la estabilidad y calidad de los modelos en producción usando un único
        motor de métricas (`panopto.metrics`). Todas las pestañas de este tablero leen resultados
        producidos por ese mismo motor:

        - **Summary Management**: fila canónica `panopto_scoring_summary` (Model, Scoring Dt, Vintage,
          Control Data Availability, Control Pre Scoring, PSI, PSI Variation, CSI Max, CSI,
          Population Scored, General Status).
        - **Pre Scoring Summary**: `median_shift`, `population_variation` y `completeness` sobre
          variables raw/input.
        - **Data Availability**: `panopto_data_availability`, comparando la fecha máxima disponible
          por fuente contra el `deadline_days` configurado en `panopto_model_table_config`.
        - **Median Raw Variables / Raw Variable Distribution / Raw Sources / Features & Score
          distribution**: `panopto_variable_summary` y `panopto_metric_result`.
        - **CSI / PSI**: `psi_canonical` (CSI, por variable raw/input) y `psi_canonical`/`psi_dynamic`
          (PSI / PSI Variation, sobre el score).

        **Semáforo:** cada métrica se clasifica en `OK` (verde), `AMBAR` (amarillo/naranja) o `RED`
        (rojo) según los umbrales configurados en `panopto_tresholds_table` /
        `panopto_metric_threshold_auto`, sin valores hardcodeados.
        """
    )
    st.caption(
        f"Tablas fuente: {PROCESS_CONFIG.scoring_summary_table}, {PROCESS_CONFIG.data_availability_table}, "
        f"{PROCESS_CONFIG.metric_result_table}, {PROCESS_CONFIG.variable_summary_table}."
    )
