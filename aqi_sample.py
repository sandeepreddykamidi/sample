# Import required packages
import streamlit as st
import pandas as pd

# Try importing Snowflake Snowpark
try:
    from snowflake.snowpark.context import get_active_session
    from snowflake.snowpark import Session
    inside_snowflake = True
except ImportError:
    inside_snowflake = False
    from snowflake.snowpark import Session

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
        # Manual connection (works locally or on Streamlit Cloud)
        connection_parameters = {
            "account": "ptcrjkr-ag14683",
            "user": "Sandeepreddy",
            "password": "Sandeepreddy@143",
            "role": "sysadmin",
            "warehouse": "steamlit_w",
            "database": "DEVDB",
            "schema": "CONSUMPTION_SC"
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

# variables to hold the selection parameters
state_option, city_option, date_option = '', '', ''

# Query to get distinct states
state_query = """
    SELECT STATE 
    FROM DEVDB.CONSUMPTION_SC.AGG_CITY_FACT_HOUR_LEVEL 
    GROUP BY STATE 
    ORDER BY 1 DESC
"""

# Fetch states
state_df = session.sql(state_query).to_pandas()
state_option = st.selectbox('Select State', state_df['STATE'].tolist())

if state_option:
    city_query = f"""
        SELECT CITY 
        FROM DEVDB.CONSUMPTION_SC.AGG_CITY_FACT_HOUR_LEVEL 
        WHERE STATE = '{state_option}' 
        GROUP BY CITY 
        ORDER BY 1 DESC
    """
    city_df = session.sql(city_query).to_pandas()
    city_option = st.selectbox('Select City', city_df['CITY'].tolist())

if city_option:
    date_query = f"""
        SELECT DATE(MEASUREMENT_TIME) AS MEASUREMENT_DATE
        FROM DEVDB.CONSUMPTION_SC.AGG_CITY_FACT_HOUR_LEVEL
        WHERE STATE = '{state_option}' 
          AND CITY = '{city_option}'
        GROUP BY MEASUREMENT_DATE
        ORDER BY 1 DESC
    """
    date_df = session.sql(date_query).to_pandas()
    date_option = st.selectbox('Select Date', date_df['MEASUREMENT_DATE'].tolist())

if date_option:
    trend_sql = f"""
        SELECT 
            HOUR(MEASUREMENT_TIME) AS HOUR,
            PM25_AVG,
            PM10_AVG,
            SO2_AVG,
            NO2_AVG,
            NH3_AVG,
            CO_AVG,
            O3_AVG
        FROM DEVDB.CONSUMPTION_SC.AGG_CITY_FACT_HOUR_LEVEL
        WHERE 
            STATE = '{state_option}' AND
            CITY = '{city_option}' AND 
            DATE(MEASUREMENT_TIME) = '{date_option}'
        ORDER BY MEASUREMENT_TIME
    """
    sf_df = session.sql(trend_sql).to_pandas()

    # Rename columns for clarity
    sf_df.rename(columns={
        'PM25_AVG': 'PM2.5',
        'PM10_AVG': 'PM10',
        'SO2_AVG': 'SO2',
        'CO_AVG': 'CO',
        'NO2_AVG': 'NO2',
        'NH3_AVG': 'NH3',
        'O3_AVG': 'O3'
    }, inplace=True)

    # Draw charts
    st.subheader(f"AQI Trends for {city_option} on {date_option}")
    st.bar_chart(sf_df, x='HOUR')
    st.divider()
    st.line_chart(sf_df, x='HOUR')
