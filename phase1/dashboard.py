"""Minimal Streamlit view to confirm sentiment updates as new posts arrive.

Reused for Phase 2 as well - point DB_PATH at data/phase1.db or
data/phase2.db depending on what you're running. Phase 3 replaces this
with Grafana-on-ClickHouse; this stays useful as a fast local sanity check.

Run with:  uv run streamlit run phase1/dashboard.py
"""

from __future__ import annotations

import os
import sqlite3
import time

import pandas as pd
import streamlit as st

st.set_page_config(page_title="Sentiment Pipeline - Live", layout="wide")

DB_PATH = os.environ.get("SQLITE_PATH", "data/phase1.db")
REFRESH_SECONDS = 5


def load_data(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame()
    conn = sqlite3.connect(path)
    try:
        return pd.read_sql_query(
            "SELECT * FROM scored_posts ORDER BY fetched_at DESC LIMIT 500", conn
        )
    finally:
        conn.close()


st.title("Live Sentiment Feed")
st.caption(f"Reading from `{DB_PATH}` - refreshes every {REFRESH_SECONDS}s")

placeholder = st.empty()

while True:
    df = load_data(DB_PATH)

    with placeholder.container():
        if df.empty:
            st.info("No messages scored yet. Is the producer/phase1 script running?")
        else:
            col1, col2, col3 = st.columns(3)
            col1.metric("Messages scored", len(df))
            col2.metric("Avg sentiment score", f"{df['sentiment_score'].mean():.2f}")
            col3.metric(
                "Positive / Neutral / Negative",
                f"{(df.sentiment_label == 'positive').sum()} / "
                f"{(df.sentiment_label == 'neutral').sum()} / "
                f"{(df.sentiment_label == 'negative').sum()}",
            )

            st.subheader("Sentiment label counts")
            st.bar_chart(df["sentiment_label"].value_counts())

            st.subheader("Most recent messages")
            st.dataframe(
                df[["fetched_at", "source", "author", "text", "sentiment_label", "sentiment_score"]].head(50),
                use_container_width=True,
            )

    time.sleep(REFRESH_SECONDS)
