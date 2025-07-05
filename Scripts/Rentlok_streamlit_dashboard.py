import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import plotly.express as px

# ————— PAGE CONFIG —————
st.set_page_config(page_title="Rentlok Dashboard", layout="wide")

# ————— CUSTOM CSS —————
st.markdown(
    """
    <style>
    .reportview-container, .main {
        background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
    }
    .sidebar .sidebar-content {
        background: #2c3e50; color: #ecf0f1;
    }
    .css-1v3fvcr h1 { color: #34495e; }
    </style>
    """,
    unsafe_allow_html=True
)

# ————— HEADER —————
st.title("🏠 Rentlok Dashboard")

# ————— SNOWFLAKE SESSION —————
session = get_active_session()

@st.cache_data(ttl=300)
def load_monthly_requests():
    df = session\
        .sql("SELECT year, property_name, month, total_requests FROM vw_monthly_requests")\
        .to_pandas()
    df.columns = [c.lower() for c in df.columns]
    return df

@st.cache_data(ttl=300)
def load_completed_bookings():
    df = session\
        .sql("SELECT year, property_name, total_bookings FROM vw_completed_bookings")\
        .to_pandas()
    df.columns = [c.lower() for c in df.columns]
    return df

@st.cache_data(ttl=300)
def load_total_tenants():
    df = session\
        .sql("SELECT property_name, tenants FROM vw_total_tenants")\
        .to_pandas()
    df.columns = [c.lower() for c in df.columns]
    return df

@st.cache_data(ttl=300)
def load_total_revenue():
    df = session\
        .sql("SELECT property_name, year, revenue FROM vw_total_revenue")\
        .to_pandas()
    df.columns = [c.lower() for c in df.columns]
    return df

# ————— LOAD DATA —————
req_df = load_monthly_requests()
bk_df  = load_completed_bookings()
tt_df  = load_total_tenants()
rev_df = load_total_revenue()

# ————— SIDEBAR FILTERS —————
st.sidebar.header("Filters")
# Requests filters
years_req = sorted(req_df["year"].unique())
year_req  = st.sidebar.selectbox("Requests: Year", years_req, index=len(years_req)-1)
props_req = sorted(req_df[req_df["year"]==year_req]["property_name"].unique())
sel_req   = st.sidebar.multiselect("Properties (Requests)", props_req, default=props_req)
# Bookings filters
years_bk = sorted(bk_df["year"].unique())
year_bk  = st.sidebar.selectbox("Bookings: Year", years_bk, index=len(years_bk)-1)
props_bk = sorted(bk_df[bk_df["year"]==year_bk]["property_name"].unique())
sel_bk   = st.sidebar.multiselect("Properties (Bookings)", props_bk, default=props_bk)
# Tenants: reuse bookings properties filter
sel_tt   = sel_bk
# Revenue: reuse bookings properties filter
sel_rev  = sel_bk

# ————— FILTERED DATA —————
filtered_req = req_df[
    (req_df["year"] == year_req) &
    (req_df["property_name"].isin(sel_req))
]
filtered_bk = bk_df[
    (bk_df["year"] == year_bk) &
    (bk_df["property_name"].isin(sel_bk))
]
filtered_tt = tt_df[
    tt_df["property_name"].isin(sel_tt)
]
filtered_rev = rev_df[
    rev_df["property_name"].isin(sel_rev)
]

# ————— FIRST ROW: REQUESTS CHARTS —————
st.subheader(f"Monthly Room Requests in {year_req}")
col1, col2 = st.columns(2)

with col1:
    fig_req_line = px.line(
        filtered_req,
        x="month", y="total_requests",
        color="property_name", markers=True,
        labels={"month":"Month","total_requests":"Requests"},
        template="plotly_dark", height=300
    )
    fig_req_line.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig_req_line, use_container_width=True)

with col2:
    pivot = (
        filtered_req
        .pivot_table(index="property_name", columns="month", values="total_requests", fill_value=0)
    )
    fig_req_heat = px.imshow(
        pivot, labels=dict(x="Month", y="Property", color="Requests"),
        x=pivot.columns, y=pivot.index,
        aspect="auto", color_continuous_scale="Viridis", height=300
    )
    fig_req_heat.update_layout(template="plotly_white", paper_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig_req_heat, use_container_width=True)

# ————— SECOND ROW: BOOKINGS & TENANTS CHARTS —————
st.subheader(f"Completed Bookings & Total Tenants")
col3, col4, col5 = st.columns([1,1,1])

with col3:
    fig_bk_bar = px.bar(
        filtered_bk,
        x="property_name", y="total_bookings",
        labels={"property_name":"Property","total_bookings":"Bookings"},
        template="plotly_dark", height=300
    )
    fig_bk_bar.update_layout(xaxis_tickangle=-45, plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig_bk_bar, use_container_width=True)

with col4:
    trend = (
        filtered_bk.groupby("year")["total_bookings"]
        .sum().reset_index()
    )
    fig_bk_line = px.line(
        trend, x="year", y="total_bookings",
        markers=True, labels={"year":"Year","total_bookings":"Bookings"},
        template="plotly_dark", height=300
    )
    fig_bk_line.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig_bk_line, use_container_width=True)

with col5:
    fig_tt_bar = px.bar(
        filtered_tt,
        x="property_name", y="tenants",
        labels={"property_name":"Property","tenants":"Tenants Served"},
        template="plotly_dark", height=300
    )
    fig_tt_bar.update_layout(xaxis_tickangle=-45, plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig_tt_bar, use_container_width=True)

# ————— THIRD ROW: REVENUE CHART & METRIC —————
st.subheader("Total Revenue")
col6, col7 = st.columns([1,2])

with col6:
    total_rev = filtered_rev["revenue"].sum()
    st.metric("Aggregate Revenue", f"Rs.{total_rev:,.2f}")

with col7:
    fig_rev = px.line(
        filtered_rev,
        x="year", y="revenue",
        color="property_name", markers=True,
        labels={"year":"Year","revenue":"Revenue"},
        template="plotly_dark", height=300
    )
    fig_rev.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig_rev, use_container_width=True)
