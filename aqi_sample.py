# Import required packages
import streamlit as st
import pandas as pd
import altair as alt

# Try importing Snowflake Snowpark
try:
    from snowflake.snowpark.context import get_active_session
    from snowflake.snowpark import Session
    inside_snowflake = True
except ImportError:
    inside_snowflake = False
    try:
        from snowflake.snowpark import Session
    except ImportError:
        st.error("Snowflake Snowpark package not installed. Make sure 'snowflake-snowpark-python' is in requirements.txt")
        st.stop()

# Page Title
st.title("AQI Trend - By State/City/Day Level")
st.write("This Streamlit app displays AQI trends using Snowflake data.")

# Function to get or create session
def get_session():
    if inside_snowflake:
        try:
            session = get_active_session()
            return session
        except Exception as e:
            st.error(f"Unable to get active Snowflake session: {e}")
            return None
    else:
        # Manual connection using Streamlit secrets
        connection_parameters = {
            "account": st.secrets["snowflake"]["account"],
            "user": st.secrets["snowflake"]["user"],
            "password": st.secrets["snowflake"]["password"],
            "role": st.secrets["snowflake"]["role"],
            "warehouse": st.secrets["snowflake"]["warehouse"],
            "database": st.secrets["snowflake"]["database"],
            "schema": st.secrets["snowflake"]["schema"]
        }
        try:
            session = Session.builder.configs(connection_parameters).create()
            return session
        except Exception as e:
            st.error(f"Error creating Snowflake session: {e}")
            return None

# Get session
session = get_session()
if not session:
    st.stop()

# Fetch States
state_df = session.sql("""
    SELECT STATE 
    FROM AGG_CITY_FACT_HOUR_LEVEL 
    GROUP BY STATE 
    ORDER BY 1 DESC
""").to_pandas()
state_option = st.selectbox('Select State', state_df['STATE'].tolist())

if state_option:
    city_df = session.sql(f"""
        SELECT CITY 
        FROM AGG_CITY_FACT_HOUR_LEVEL 
        WHERE STATE = '{state_option}' 
        GROUP BY CITY 
        ORDER BY 1 DESC
    """).to_pandas()
    city_option = st.selectbox('Select City', city_df['CITY'].tolist())

if city_option:
    date_df = session.sql(f"""
        SELECT DATE(MEASUREMENT_TIME) AS MEASUREMENT_DATE
        FROM AGG_CITY_FACT_HOUR_LEVEL
        WHERE STATE = '{state_option}' AND CITY = '{city_option}'
        GROUP BY MEASUREMENT_DATE
        ORDER BY 1 DESC
    """).to_pandas()
    date_option = st.selectbox('Select Date', date_df['MEASUREMENT_DATE'].astype(str).tolist())

if date_option:
    trend_df = session.sql(f"""
        SELECT 
            HOUR(MEASUREMENT_TIME) AS HOUR,
            PM25_AVG,
            PM10_AVG,
            SO2_AVG,
            NO2_AVG,
            NH3_AVG,
            CO_AVG,
            O3_AVG
        FROM AGG_CITY_FACT_HOUR_LEVEL
        WHERE STATE = '{state_option}' AND CITY = '{city_option}' 
          AND DATE(MEASUREMENT_TIME) = '{date_option}'
        ORDER BY MEASUREMENT_TIME
    """).to_pandas()

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
    ).mark_line().encode(
        x='HOUR:O',
        y='Value:Q',
        color='Pollutant:N'
    )
    st.altair_chart(chart, use_container_width=True)
