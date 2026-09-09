"""Aplicación de Streamlit para el dashboard MECV."""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from mecv.dashboard.data import DashboardData

st.set_page_config(
    page_title="MECV Dashboard",
    page_icon="📊",
    layout="wide",
)

if "data" not in st.session_state:
    st.session_state.data = DashboardData()

data = st.session_state.data

st.sidebar.title("🔧 MECV")
st.sidebar.caption("Monitoreo de Estabilidad y Calidad de Variables")

models = data.get_models()
selected_model = st.sidebar.selectbox(
    "Modelo",
    models if models else ["1079_cta_lvl"],
)

start = st.sidebar.date_input("Fecha inicio")
end = st.sidebar.date_input("Fecha fin")

with st.spinner("Cargando datos..."):
    sem = data.get_semaphore(
        selected_model,
        start.isoformat(),
        end.isoformat(),
    )
    summ = data.get_summary(
        selected_model,
        start.isoformat(),
        end.isoformat(),
    )

st.title(f"📊 Dashboard MECV — {selected_model}")

if sem.empty:
    st.warning("No hay datos para el modelo y rango seleccionados.")
    st.stop()

# KPIs
latest = sem.iloc[-1]
col1, col2, col3, col4 = st.columns(4)
col1.metric("Última fecha", latest["information_date"].strftime("%Y-%m-%d"))
col2.metric("🔴 Rojas", int((sem["status"] == "RED").sum()))
col3.metric("🟠 Ámbar", int((sem["status"] == "AMBAR").sum()))
col4.metric("🟢 Verde", int((sem["status"] == "OK").sum()))

# Tendencia de alertas y stress ratio
daily = (
    sem.groupby("information_date")
    .agg(
        stress_ratio=("stress_ratio", "mean"),
        red=("status", lambda x: (x == "RED").sum()),
        ambar=("status", lambda x: (x == "AMBAR").sum()),
        green=("status", lambda x: (x == "OK").sum()),
    )
    .reset_index()
)

fig = go.Figure()
fig.add_trace(
    go.Scatter(
        x=daily["information_date"],
        y=daily["stress_ratio"],
        name="Stress Ratio",
        line=dict(color="blue", width=3),
    )
)
fig.add_trace(
    go.Bar(
        x=daily["information_date"],
        y=daily["red"],
        name="Rojas",
        marker_color="red",
    )
)
fig.add_trace(
    go.Bar(
        x=daily["information_date"],
        y=daily["ambar"],
        name="Ámbar",
        marker_color="orange",
    )
)
fig.add_trace(
    go.Bar(
        x=daily["information_date"],
        y=daily["green"],
        name="Verdes",
        marker_color="green",
    )
)
fig.update_layout(
    barmode="stack",
    title="Tendencia de alertas y stress ratio",
    xaxis_title="Fecha",
    yaxis_title="Cantidad / Stress",
    template="plotly_white",
)
st.plotly_chart(fig, use_container_width=True)

# Distribución de estados y alertas por var_type
c1, c2 = st.columns(2)

status_counts = sem["status"].value_counts().reset_index()
status_counts.columns = ["status", "count"]
fig_pie = px.pie(
    status_counts,
    values="count",
    names="status",
    color="status",
    color_discrete_map={"RED": "red", "AMBAR": "orange", "OK": "green"},
    title="Distribución de estados",
    hole=0.4,
)
fig_pie.update_layout(template="plotly_white")
c1.plotly_chart(fig_pie, use_container_width=True)

var_counts = (
    sem[sem["status"].isin(["RED", "AMBAR"])]
    .groupby(["var_type", "status"])
    .size()
    .reset_index(name="count")
)
fig_bar = px.bar(
    var_counts,
    x="var_type",
    y="count",
    color="status",
    color_discrete_map={"RED": "red", "AMBAR": "orange"},
    title="Alertas por tipo de variable",
    barmode="group",
)
fig_bar.update_layout(template="plotly_white")
c2.plotly_chart(fig_bar, use_container_width=True)

# Últimas métricas
st.subheader("Últimas métricas por variable")
latest_metrics = (
    sem.sort_values("information_date")
    .groupby(["var_type", "metric_name"])
    .last()
    .reset_index()
    [["var_type", "metric_name", "metric_value", "status", "aggregate_status"]]
)
st.dataframe(latest_metrics, use_container_width=True)

if not summ.empty:
    st.subheader("Resumen por tipo de variable")
    st.dataframe(
        summ.sort_values("information_date", ascending=False).head(10),
        use_container_width=True,
    )

st.caption(
    "MECV — Datos extraídos de mecv_dashboard_semaphore y mecv_dashboard_model_summary"
)
