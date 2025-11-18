import time
from datetime import datetime
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Real-Time Transactions Dashboard", layout="wide")
st.title("🏦 Real-Time Transactions Dashboard")

DATABASE_URL = "postgresql://kafka_user:kafka_password@localhost:5432/kafka_db"


@st.cache_resource
def get_engine(url: str):
    return create_engine(url, pool_pre_ping=True)


engine = get_engine(DATABASE_URL)


def load_data(status_filter: str | None = None, limit: int = 200) -> pd.DataFrame:
    base_query = "SELECT * FROM transactions"
    params = {}
    if status_filter and status_filter != "All":
        base_query += " WHERE status = :status"
        params["status"] = status_filter
    base_query += " ORDER BY timestamp DESC LIMIT :limit"
    params["limit"] = limit

    try:
        df = pd.read_sql_query(text(base_query), con=engine.connect(), params=params)
        return df
    except Exception as e:
        st.error(f"Error loading data from database: {e}")
        return pd.DataFrame()


# Sidebar controls
status_options = ["All", "Pending", "Confirmed", "Rejected", "Reversed"]
selected_status = st.sidebar.selectbox("Filter by Status", status_options)
update_interval = st.sidebar.slider(
    "Update Interval (seconds)", min_value=2, max_value=20, value=5
)
limit_records = st.sidebar.number_input(
    "Number of records to load", min_value=50, max_value=2000, value=200, step=50
)

if st.sidebar.button("Refresh now"):
    st.rerun()

placeholder = st.empty()

# Initialize session state for unique keys
if "chart_keys" not in st.session_state:
    st.session_state.chart_keys = {"category": 0, "city": 0, "status": 0}

while True:
    df_transactions = load_data(selected_status, limit=int(limit_records))

    with placeholder.container():
        if df_transactions.empty:
            st.warning("No records found. Waiting for data...")
            time.sleep(update_interval)
            continue

        if "timestamp" in df_transactions.columns:
            df_transactions["timestamp"] = pd.to_datetime(df_transactions["timestamp"])

        # KPIs
        total_transactions = len(df_transactions)
        total_value = df_transactions["value"].sum()
        average_value = (
            total_value / total_transactions if total_transactions > 0 else 0.0
        )
        confirmed = len(df_transactions[df_transactions["status"] == "Confirmed"])
        rejected = len(df_transactions[df_transactions["status"] == "Rejected"])
        reversed_count = len(df_transactions[df_transactions["status"] == "Reversed"])
        success_rate = (
            (confirmed / total_transactions * 100) if total_transactions > 0 else 0.0
        )

        st.subheader(
            f"Displaying {total_transactions} transactions (Filter: {selected_status})"
        )

        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("Total Transactions", total_transactions)
        k2.metric("Total Value", f"${total_value:,.2f}")
        k3.metric("Average Value", f"${average_value:,.2f}")
        k4.metric("Success Rate", f"{success_rate:,.2f}%")
        k5.metric("Rejected/Reversed", rejected + reversed_count)

        st.markdown("### Raw Data (Top 10)")
        st.dataframe(df_transactions.head(10), use_container_width=True)

        # Charts with unique keys
        chart_col1, chart_col2 = st.columns(2)

        with chart_col1:
            # Value by Category
            grouped_category = (
                df_transactions.groupby("category")["value"]
                .sum()
                .reset_index()
                .sort_values("value", ascending=False)
            )
            fig_category = px.bar(
                grouped_category,
                x="category",
                y="value",
                title="Transaction Value by Category",
                color="category",
            )
            st.session_state.chart_keys["category"] += 1
            st.plotly_chart(
                fig_category,
                use_container_width=True,
                key=f"category_chart_{st.session_state.chart_keys['category']}",
            )

        with chart_col2:
            # Value by City
            grouped_city = df_transactions.groupby("city")["value"].sum().reset_index()
            fig_city = px.pie(
                grouped_city,
                values="value",
                names="city",
                title="Transaction Value by City",
            )
            st.session_state.chart_keys["city"] += 1
            st.plotly_chart(
                fig_city,
                use_container_width=True,
                key=f"city_chart_{st.session_state.chart_keys['city']}",
            )

        # Additional chart: Transactions by Status
        chart_col3, chart_col4 = st.columns(2)

        with chart_col3:
            # Transactions by Status
            status_counts = df_transactions["status"].value_counts().reset_index()
            status_counts.columns = ["status", "count"]
            fig_status = px.pie(
                status_counts,
                values="count",
                names="status",
                title="Transactions by Status",
            )
            st.session_state.chart_keys["status"] += 1
            st.plotly_chart(
                fig_status,
                use_container_width=True,
                key=f"status_chart_{st.session_state.chart_keys['status']}",
            )

        with chart_col4:
            # Payment Method Distribution
            payment_counts = (
                df_transactions["payment_method"].value_counts().reset_index()
            )
            payment_counts.columns = ["payment_method", "count"]
            fig_payment = px.bar(
                payment_counts,
                x="payment_method",
                y="count",
                title="Transactions by Payment Method",
                color="payment_method",
            )
            st.plotly_chart(
                fig_payment,
                use_container_width=True,
                key=f"payment_chart_{datetime.now().timestamp()}",
            )

        st.markdown("---")
        st.caption(
            f"Last updated: {datetime.now().isoformat()} • Auto-refresh: {update_interval}s"
        )

    time.sleep(update_interval)
