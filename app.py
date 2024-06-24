import streamlit as st
import pandas as pd
from database.db1 import get_db1_connection
from database.db2 import get_db2_connection
from queries.queries_db1 import fetch_data_from_db1
from queries.queries_db2 import fetch_data_from_db2
from utils import convert_to_dataframe


st.set_page_config(
    page_title="Trading Dashboard",
    layout="wide",
    initial_sidebar_state="expanded")


total_trade = 0
trade_data =None
total_close_position = 0
total_open_position = 0
realised_pnl =0
unrealised_pnl =0
overall_pnl =0
currently_open =0
avg_pnl=0
avg_roi =0
sl_hit =0
total_open_size =0
closed_trades_data = None
trade_cursor_data = None

st.title('Trading Model Dashboard')

col_1, col_2, col_3 = st.columns(3)

with col_1:
    # Model selection
    model = st.selectbox('Select Model', ['experiment_v1_v2','altron'])
with col_2:
    # Date selection
    start_date_1 = st.date_input('Start Date', value=pd.to_datetime('2024-06-07'))
with col_3:
    end_date_1 = st.date_input('End Date', value=pd.to_datetime('today'))

# Database 1
try:
    conn1 = get_db1_connection()
    data1 = fetch_data_from_db1(conn1,model,start_date_1,end_date_1)
    # Create a 2-column layout for better visualization
    col1, col2, col3 = st.columns(3)
    col4, col5, col6 = st.columns(3)
    col7, col8, col9 = st.columns(3)
    col10, col11, col12 = st.columns(3)

    # Metrics
    with col1:
        overall_pnl = data1[6]
        st.metric(label='Overall PnL', value= float(overall_pnl))
    with col2:
        realised_pnl = data1[4]
        st.metric(label='Realized PnL', value= float(realised_pnl))
    with col3:
        unrealised_pnl = data1[5]
        st.metric(label='Unrealized PnL', value= float(unrealised_pnl))
    with col4:
        results = data1[7]
        if results:
            st.metric(label='Currently Open Positions', value= len(results))
        else:
            st.metric(label='Currently Open Positions', value= 0)
    with col5:
        total_close_position = data1[2]
        st.metric(label='Total Closed Positions', value=float(total_close_position))
    with col6:
        total_trade = data1[0]
        st.metric(label='Total Trades', value= total_trade)
    with col7:
        avg_pnl = data1[8]
        st.metric(label='Avg PnL Per Position', value=float(avg_pnl))
    with col8:
        avg_roi = data1[9]
        st.metric(label='Avg RoI Per Position', value=float(avg_roi))
    with col9:
        total_open_position = data1[3]
        st.metric(label='Total Open Orders', value=float(total_open_position))
    with col10:
        st.metric(label='SL Hit', value= 0)
    with col11:
        total_open_size =data1[11]
        st.metric(label='Total Open Size (USD)', value=float(total_open_size))
    with col12:
        st.metric(label='Current Leverage', value='None')

    if results is not None:
        st.header("Currently Open Trade")
        df_1 = convert_to_dataframe(results)
        #df_1.rename(columns={0: 'vid', 1: 'token', 2: 'pos_side(dydx)', 3: 'open_price(dydx)', 4: 'open_size(dydx)',5: 'open_time(dydx)', 6: 'total_open_trades'}, inplace=True)
        #df_1.drop('total_open_trades',axis = 1,inplace = True)
        st.dataframe(df_1)

    if data1[12]:
        st.header('Closed Positions')
        df= convert_to_dataframe(data1[12])
        st.dataframe(df)

    if  data1[1]:
        st.header('Total Trades')
        trade = convert_to_dataframe(data1[1])
        st.dataframe(trade)

except Exception as e:
    st.error(f"Error connecting to Database 1: {e}")

# Database 2
# st.header('Database 2')
# try:
#     conn2 = get_db2_connection()
#     data2 = fetch_data_from_db2(conn2)
#     df2 = convert_to_dataframe(data2, ['column1', 'column2', 'column3'])
#     st.write(df2)
# except Exception as e:
#     st.error(f"Error connecting to Database 2: {e}")
