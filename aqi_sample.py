# Import python packages
import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

# Page Title
st.title("AQI Trend- By State/City/Day Level")
st.write("This streamlit app hosted on Snowflake Cloud Data Warehouse Platform")

# Get Session
session = get_active_session()

# variables to hold the selection parameters, initiating as empty string
state_option, city_option, date_option = '', '', ''

# query to get distinct states from agg_city_fact_hour_level table
state_query = """
    select state from devdb.CONSUMPTION_sc.AGG_CITY_FACT_HOUR_LEVEL 
    group by state 
    order by 1 desc
"""

# execute query and convert to pandas dataframe
state_list = session.sql(state_query).to_pandas()
state_option = st.selectbox('Select State', state_list['STATE'])

#check the selection
if state_option:
    city_query = f"""
        select city from devdb.CONSUMPTION_sc.AGG_CITY_FACT_HOUR_LEVEL 
        where state = '{state_option}' 
        group by city
        order by 1 desc
    """
    city_list = session.sql(city_query).to_pandas()
    city_option = st.selectbox('Select City', city_list['CITY'])

if city_option:
    date_query = f"""
        select date(measurement_time) as measurement_date 
        from devdb.CONSUMPTION_sc.AGG_CITY_FACT_HOUR_LEVEL 
        where state = '{state_option}' and city = '{city_option}'
        group by measurement_date
        order by 1 desc
    """
    date_list = session.sql(date_query).to_pandas()
    date_option = st.selectbox('Select Date', date_list['MEASUREMENT_DATE'])

if date_option:
    trend_sql = f"""
        select 
            hour(measurement_time) as Hour,
            PM25_AVG,
            PM10_AVG,
            SO2_AVG,
            NO2_AVG,
            NH3_AVG,
            CO_AVG,
            O3_AVG
        from devdb.consumption_sc.agg_city_fact_hour_level
        where 
            state = '{state_option}' and
            city = '{city_option}' and 
            date(measurement_time) = '{date_option}'
        order by measurement_time
    """
    pd_df = session.sql(trend_sql).to_pandas()

    pd_df.rename(columns={
        'PM25_AVG': 'PM2.5',
        'PM10_AVG': 'PM10',
        'SO2_AVG': 'SO2',
        'CO_AVG': 'CO',
        'NO2_AVG': 'NO2',
        'NH3_AVG': 'NH3',
        'O3_AVG': 'O3'
    }, inplace=True)
    
    st.bar_chart(pd_df, x='HOUR')
    st.divider()
    st.line_chart(pd_df, x='HOUR')
