import os
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
from dotenv import load_dotenv
import streamlit as st

load_dotenv()

DB_USER = os.getenv('DB_USER', 'iot_user')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'iot_password')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'iot_db')

CONN_STR = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

st.set_page_config(page_title="IoT Temperature Dashboard", layout="wide")
st.title("🌡️ IoT Temperature Dashboard")

engine = create_engine(CONN_STR, future=True)

@st.cache_data(ttl=60)
def load_daily():
    return pd.read_sql("SELECT * FROM v_daily_stats", engine)

@st.cache_data(ttl=60)
def load_latest():
    return pd.read_sql("SELECT * FROM v_latest_per_room", engine)

col1, col2 = st.columns(2)
with col1:
    st.subheader("Daily Overview")
    daily = load_daily()
    if not daily.empty:
        fig = px.line(daily, x="day", y="temp_avg", markers=True)
        st.plotly_chart(fig, use_container_width=True)
    st.dataframe(daily)

with col2:
    st.subheader("Latest per Room")
    latest = load_latest()
    st.dataframe(latest)
