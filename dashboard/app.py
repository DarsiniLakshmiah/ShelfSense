"""
ShelfSense Streamlit Dashboard — dashboard/app.py

Surfaces:
  1. Price history chart per item × store
  2. Buy-now / wait recommendations (from mart_buy_signals)
  3. External signals (USDA/BLS/NOAA) trend panel
  4. Promo calendar heat-map
  5. Causal inference summary cards

Run locally:
    streamlit run dashboard/app.py
"""

from __future__ import annotations

import logging
import os

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Snowflake helpers
# ---------------------------------------------------------------------------


def _new_conn():
    """Create a fresh Snowflake connection (not cached — avoids stale sessions)."""
    import snowflake.connector

    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database=os.environ.get("SNOWFLAKE_DATABASE", "SHELFSENSE"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        role=os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    )


def _query(sql: str) -> pd.DataFrame:
    """Run a SQL query and return a DataFrame. Raises on error so callers can display it."""
    conn = _new_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        return cur.fetch_pandas_all()
    finally:
        conn.close()


@st.cache_data(ttl=300)
def load_price_history(item_id: str, store_id: str, days: int = 90) -> pd.DataFrame:
    return _query(f"""
        SELECT price_date, regular_price, promo_price, is_on_sale,
               price_delta_7d, price_delta_30d, price_vs_90d_avg
        FROM SHELFSENSE.MARTS.MART_PRICE_HISTORY
        WHERE item_id = '{item_id}'
          AND store_id = '{store_id}'
          AND price_date >= DATEADD('day', -{days}, CURRENT_DATE())
        ORDER BY price_date
    """)


@st.cache_data(ttl=300)
def load_buy_signals(limit: int = 100) -> pd.DataFrame:
    return _query(f"""
        SELECT item_id, store_id, signal_date, regular_price,
               predicted_price_7d, predicted_price_14d,
               buy_now_probability, recommendation, model_version
        FROM SHELFSENSE.MARTS.MART_BUY_SIGNALS
        WHERE signal_date = (SELECT MAX(signal_date) FROM SHELFSENSE.MARTS.MART_BUY_SIGNALS)
        ORDER BY buy_now_probability DESC NULLS LAST
        LIMIT {limit}
    """)


@st.cache_data(ttl=3600)
def load_external_signals(days: int = 90) -> pd.DataFrame:
    return _query(f"""
        SELECT signal_date, signal_type, commodity, value, yoy_change_pct
        FROM SHELFSENSE.MARTS.MART_EXTERNAL_SIGNALS
        WHERE signal_date >= DATEADD('day', -{days}, CURRENT_DATE())
        ORDER BY signal_date DESC
    """)


@st.cache_data(ttl=300)
def load_items_and_stores() -> tuple[list, list]:
    # store_name is not in the mart — use store_id only
    df = _query("""
        SELECT DISTINCT item_id, item_name, store_id
        FROM SHELFSENSE.MARTS.MART_PRICE_HISTORY
        ORDER BY item_name
        LIMIT 500
    """)
    items = df[["ITEM_ID", "ITEM_NAME"]].drop_duplicates().values.tolist()
    stores = df[["STORE_ID"]].drop_duplicates().values.tolist()
    return items, stores


# ---------------------------------------------------------------------------
# Dashboard layout
# ---------------------------------------------------------------------------


def render_recommendation_badge(rec: str) -> str:
    colors = {"BUY_NOW": "🟢", "WAIT": "🔴", "NEUTRAL": "🟡"}
    return f"{colors.get(rec, '⚪')} **{rec}**"


def main() -> None:
    st.set_page_config(
        page_title="ShelfSense — Grocery Price Intelligence",
        page_icon="🛒",
        layout="wide",
    )

    st.title("🛒 ShelfSense — Grocery Price Intelligence")
    st.caption("Know *when* prices will drop and *why* they changed.")

    # ---------------------------------------------------------------------------
    # Sidebar
    # ---------------------------------------------------------------------------
    with st.sidebar:
        st.header("Filters")
        days_back = st.slider("History (days)", 14, 180, 90)
        st.divider()
        st.subheader("Select Item & Store")

        try:
            items, stores = load_items_and_stores()
        except Exception as e:
            st.error(f"Snowflake connection error: {e}")
            items, stores = [], []

        if items:
            item_labels = [f"{name} ({id_})" for id_, name in items]
            selected_item_label = st.selectbox("Item", item_labels)
            selected_item_id = items[item_labels.index(selected_item_label)][0]
        else:
            st.info("No items loaded yet — run the daily DAG first.")
            selected_item_id = None

        if stores:
            # stores is now a list of [store_id] single-element lists
            store_ids = [s[0] for s in stores]
            selected_store_id = st.selectbox("Store", store_ids)
        else:
            selected_store_id = None

    # ---------------------------------------------------------------------------
    # Tab layout
    # ---------------------------------------------------------------------------
    tab_prices, tab_signals, tab_recommendations, tab_causal = st.tabs(
        ["📈 Price History", "📡 External Signals", "🛍 Buy Recommendations", "🔬 Causal Analysis"]
    )

    # ---------------------------------------------------------------------------
    # Tab 1: Price History
    # ---------------------------------------------------------------------------
    with tab_prices:
        st.subheader("Price History")
        if selected_item_id and selected_store_id:
            with st.spinner("Loading price history..."):
                try:
                    df_prices = load_price_history(selected_item_id, selected_store_id, days_back)
                    if df_prices.empty:
                        st.warning("No price history found. Run the ingestion pipeline first.")
                    else:
                        df_prices.columns = [c.lower() for c in df_prices.columns]
                        df_prices["price_date"] = pd.to_datetime(df_prices["price_date"])

                        fig = go.Figure()
                        fig.add_trace(go.Scatter(
                            x=df_prices["price_date"],
                            y=df_prices["regular_price"],
                            name="Regular Price",
                            line=dict(color="royalblue"),
                        ))
                        fig.add_trace(go.Scatter(
                            x=df_prices[df_prices["is_on_sale"]]["price_date"],
                            y=df_prices[df_prices["is_on_sale"]]["promo_price"],
                            name="Promo Price",
                            mode="markers",
                            marker=dict(color="crimson", size=8, symbol="star"),
                        ))
                        fig.update_layout(
                            title=f"Price History — {days_back}d",
                            xaxis_title="Date",
                            yaxis_title="Price ($)",
                            hovermode="x unified",
                        )
                        st.plotly_chart(fig, use_container_width=True)

                        col1, col2, col3 = st.columns(3)
                        col1.metric("7-Day Change", f"${df_prices['price_delta_7d'].iloc[-1]:.2f}" if df_prices["price_delta_7d"].notna().any() else "N/A")
                        col2.metric("30-Day Change", f"${df_prices['price_delta_30d'].iloc[-1]:.2f}" if df_prices["price_delta_30d"].notna().any() else "N/A")
                        col3.metric("vs 90d Avg", f"${df_prices['price_vs_90d_avg'].iloc[-1]:.2f}" if df_prices["price_vs_90d_avg"].notna().any() else "N/A")

                        with st.expander("Raw data"):
                            st.dataframe(df_prices, use_container_width=True)
                except Exception as e:
                    st.error(f"Could not load price history: {e}")
        else:
            st.info("Select an item and store in the sidebar.")

    # ---------------------------------------------------------------------------
    # Tab 2: External Signals
    # ---------------------------------------------------------------------------
    with tab_signals:
        st.subheader("External Economic & Weather Signals")
        with st.spinner("Loading signals..."):
            try:
                df_sig = load_external_signals(days_back)
                if df_sig.empty:
                    st.warning("No external signals yet. Run the weekly signals DAG first.")
                else:
                    df_sig.columns = [c.lower() for c in df_sig.columns]
                    df_sig["signal_date"] = pd.to_datetime(df_sig["signal_date"])

                    signal_type = st.selectbox("Signal type", df_sig["signal_type"].unique().tolist())
                    df_filtered = df_sig[df_sig["signal_type"] == signal_type]

                    commodity_options = df_filtered["commodity"].dropna().unique().tolist()
                    if commodity_options:
                        selected_commodity = st.selectbox("Commodity / Series", commodity_options)
                        df_plot = df_filtered[df_filtered["commodity"] == selected_commodity]
                        fig = px.line(
                            df_plot.sort_values("signal_date"),
                            x="signal_date",
                            y="value",
                            title=f"{signal_type} — {selected_commodity}",
                            labels={"value": "Price / Index", "signal_date": "Date"},
                        )
                        st.plotly_chart(fig, use_container_width=True)

                    with st.expander("Raw signals"):
                        st.dataframe(df_sig, use_container_width=True)
            except Exception as e:
                st.error(f"Could not load signals: {e}")

    # ---------------------------------------------------------------------------
    # Tab 3: Buy Recommendations
    # ---------------------------------------------------------------------------
    with tab_recommendations:
        st.subheader("Latest Buy-Now Recommendations")
        with st.spinner("Loading recommendations..."):
            try:
                df_recs = load_buy_signals(limit=100)
                if df_recs.empty:
                    st.warning("No buy signals yet. Run the ML classifier first.")
                else:
                    df_recs.columns = [c.lower() for c in df_recs.columns]

                    col_filter, _ = st.columns([1, 3])
                    rec_filter = col_filter.multiselect(
                        "Filter by recommendation",
                        ["BUY_NOW", "NEUTRAL", "WAIT"],
                        default=["BUY_NOW"],
                    )
                    df_show = df_recs[df_recs["recommendation"].isin(rec_filter)] if rec_filter else df_recs

                    # Color-coded table
                    def highlight_rec(val: str) -> str:
                        colors = {"BUY_NOW": "background-color: #d4edda", "WAIT": "background-color: #f8d7da", "NEUTRAL": "background-color: #fff3cd"}
                        return colors.get(val, "")

                    st.dataframe(
                        df_show.style.applymap(highlight_rec, subset=["recommendation"]),
                        use_container_width=True,
                    )

                    # Summary cards
                    st.divider()
                    counts = df_recs["recommendation"].value_counts()
                    c1, c2, c3 = st.columns(3)
                    c1.metric("🟢 BUY NOW", counts.get("BUY_NOW", 0))
                    c2.metric("🟡 NEUTRAL", counts.get("NEUTRAL", 0))
                    c3.metric("🔴 WAIT", counts.get("WAIT", 0))
            except Exception as e:
                st.error(f"Could not load recommendations: {e}")

    # ---------------------------------------------------------------------------
    # Tab 4: Causal Analysis
    # ---------------------------------------------------------------------------
    with tab_causal:
        st.subheader("Causal Inference Results")
        st.info(
            "Run causal analyses via CLI or the weekly Airflow DAG, then results appear here. "
            "The models implemented are:\n"
            "- **DiD**: Difference-in-Differences for supply shock effects\n"
            "- **RDD**: Regression Discontinuity for promo cycle detection\n"
            "- **IV**: Instrumental Variables for commodity pass-through estimation"
        )

        st.code(
            """# Run causal analyses manually
python ml/causal/diff_in_diff.py --event bird_flu_2024 --event_date 2024-02-01
python ml/causal/rdd.py
python ml/causal/iv_analysis.py --category dairy""",
            language="bash",
        )

        # Placeholder metric cards
        with st.expander("Example: IV Commodity Pass-Through"):
            col_a, col_b = st.columns(2)
            col_a.metric("Eggs wholesale → retail", "~6.2% per 10% spike", delta="2-week lag")
            col_b.metric("Dairy wholesale → retail", "~5.8% per 10% spike", delta="2-week lag")


if __name__ == "__main__":
    main()
