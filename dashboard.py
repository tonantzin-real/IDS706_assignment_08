import time
from datetime import datetime
import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Real-Time Transactions Dashboard", layout="wide")
st.title("🏦 Real-Time Bank Transactions Dashboard")

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
        reversed = len(df_transactions[df_transactions["status"] == "Reversed"])
        approval_rate = (
            (confirmed / total_transactions * 100) if total_transactions > 0 else 0.0
        )

        st.subheader(
            f"Displaying {total_transactions} transactions (Filter: {selected_status})"
        )

        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("Total Transactions", total_transactions)
        k2.metric("Total Value", f"${total_value:,.2f}")
        k3.metric("Average Value", f"${average_value:,.2f}")
        k4.metric("Approval Rate", f"{approval_rate:,.2f}%")
        k5.metric("Rejected/Reversed", rejected + reversed)

        st.markdown("### Raw Data (Top 10)")
        st.dataframe(df_transactions.head(10), use_container_width=True)

        # Charts
        grouped_category = (
            df_transactions.groupby("category")["value"]
            .sum()
            .reset_index()
            .sort_values("value", ascending=False)
        )
        fig_category = px.bar(
            grouped_category, x="category", y="value", title="Value by Category"
        )

        grouped_city = df_transactions.groupby("city")["value"].sum().reset_index()
        fig_city = px.pie(
            grouped_city, values="value", names="city", title="Value by City"
        )

        chart_col1, chart_col2 = st.columns(2)
        with chart_col1:
            st.plotly_chart(fig_category, use_container_width=True)
        with chart_col2:
            st.plotly_chart(fig_city, use_container_width=True)

        st.markdown("---")
        st.caption(
            f"Last updated: {datetime.now().isoformat()} • Auto-refresh: {update_interval}s"
        )

    time.sleep(update_interval)
