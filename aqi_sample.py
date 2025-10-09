# Import required packages
import streamlit as st
import pandas as pd
import altair as alt
import snowflake.connector

# Page Title
st.title("AQI Trend - By State/City/Day Level")
st.write("This Streamlit app displays AQI trends using Snowflake data.")

# Function to get Snowflake connection
def get_connection():
    try:
        conn = snowflake.connector.connect(
            account=st.secrets["snowflake"]["ptcrjkr-ag14683"],
            user=st.secrets["snowflake"]["Sandeepreddy"],
            password=st.secrets["snowflake"]["Sandeepreddy@143"],
            role=st.secrets["snowflake"]["sysadmin"],
            warehouse=st.secrets["snowflake"]["streamlit_w"],
            database=st.secrets["snowflake"]["devdb"],
            schema=st.secrets["snowflake"]["publish_sc"]
        )
        return conn
    except Exception as e:
        st.error(f"Error connecting to Snowflake: {e}")
        return None

# Get connection
conn = get_connection()
if not conn:
    st.stop()

# Helper function to run query and return DataFrame
def run_query(query, params=None):
    try:
        if params:
            df = pd.read_sql(query, conn, params=params)
        else:
            df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        st.error(f"Error executing query: {e}")
        return pd.DataFrame()

# Fetch States
state_df = run_query("SELECT STATE FROM AGG_CITY_FACT_HOUR_LEVEL GROUP BY STATE ORDER BY STATE")
state_option = st.selectbox('Select State', state_df['STATE'].tolist())

if state_option:
    city_df = run_query(
        "SELECT CITY FROM AGG_CITY_FACT_HOUR_LEVEL WHERE STATE=%s GROUP BY CITY ORDER BY CITY",
        params=(state_option,)
    )
    city_option = st.selectbox('Select City', city_df['CITY'].tolist())

if city_option:
    date_df = run_query(
        "SELECT DATE(MEASUREMENT_TIME) AS MEASUREMENT_DATE "
        "FROM AGG_CITY_FACT_HOUR_LEVEL "
        "WHERE STATE=%s AND CITY=%s "
        "GROUP BY MEASUREMENT_DATE ORDER BY MEASUREMENT_DATE DESC",
        params=(state_option, city_option)
    )
    date_option = st.selectbox('Select Date', date_df['MEASUREMENT_DATE'].astype(str).tolist())

if date_option:
    trend_df = run_query(
        "SELECT HOUR(MEASUREMENT_TIME) AS HOUR, PM25_AVG, PM10_AVG, SO2_AVG, NO2_AVG, NH3_AVG, CO_AVG, O3_AVG "
        "FROM AGG_CITY_FACT_HOUR_LEVEL "
        "WHERE STATE=%s AND CITY=%s AND DATE(MEASUREMENT_TIME)=%s "
        "ORDER BY MEASUREMENT_TIME",
        params=(state_option, city_option, date_option)
    )

    trend_df.rename(columns={
        'PM25_AVG': 'PM2.5',
        'PM10_AVG': 'PM10',
        'SO2_AVG': 'SO2',
        'CO_AVG': 'CO',
        'NO2_AVG': 'NO2',
        'NH3_AVG': 'NH3',
        'O3_AVG': 'O3'
    }, inplace=True)

    st.subheader(f"AQI Trends for {city_option} on {date_option}")

    # Altair Line Chart
    chart = alt.Chart(trend_df).transform_fold(
        ['PM2.5', 'PM10', 'SO2', 'NO2', 'NH3', 'CO', 'O3'],
        as_=['Pollutant', 'Value']
    ).mark_line(point=True).encode(
        x='HOUR:O',
        y='Value:Q',
        color='Pollutant:N',
        tooltip=['HOUR', 'Pollutant', 'Value']
    ).interactive()

    st.altair_chart(chart, use_container_width=True)
