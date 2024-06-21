import streamlit as st
import requests
import pandas as pd
import os
import warnings
import psycopg2
import psycopg2.extras



st.set_page_config(
    page_title="Trading Dashboard",
    layout="wide",
    initial_sidebar_state="expanded")

with open ('/home/deq/Desktop/Project/App/style.css') as f:
     st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html= True)

# Title of the dashboard
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

## style_metric_card


#Fetching data Through Postgres
conn = None
cur = None
schema = model
start_date = start_date_1
end_date   = end_date_1

try:
    conn = psycopg2.connect (
        host = 'localhost',
        database = 'postgres',
        user = 'viewer',
        password='viewer',
        port='5431'
        )
    cur = conn.cursor()
    #cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    #To fetch all data From the sheet 
    cur.execute(f'''SELECT vid,market,position_side,side,size,order_price, CAST(created_at AS TIMESTAMP) FROM {schema}.dydx_orders  
                WHERE CAST(created_at AS DATE) BETWEEN '{start_date}' AND '{end_date}' ''')
    trade_data = cur.fetchall()
    trade = pd.DataFrame(trade_data, columns=['vid','token','pos_side','side','size','order_price','dydx_createdtime'])
    #Total Trade

    cur.execute(f'''SELECT COUNT(order_id)  FROM {schema}.predicted_alpha_position_trades 
                WHERE CAST(created_at AS DATE) BETWEEN '{start_date}' AND '{end_date}' ''' ) 
    
    total_tr= cur.fetchone()

    #print("Total trades are",total_tr[0])

    #Total Close Position
    
    #In case of Long Position
    cur.execute(f'''SELECT COUNT(order_id)  FROM {schema}.predicted_alpha_position_trades 
                WHERE status = 'Close' AND position_side= 'LONG' 
                AND CAST(created_at AS DATE) BETWEEN '{start_date}' AND '{end_date}' ''' ) 
    close_l = cur.fetchone()

    #In case of Short Position
    cur.execute(f'''SELECT COUNT(order_id)  FROM {schema}.predicted_alpha_position_trades 
                WHERE status = 'OPEN' AND position_side= 'SHORT' 
                AND CAST(created_at AS DATE) BETWEEN '{start_date}' AND '{end_date}' ''' ) 
    close_s = cur.fetchone()
    #print("Total Close Position are",close_l[0]+close_s[0])

    # Total OPEN Position

    cur.execute(f'''SELECT COUNT(order_id)  FROM {schema}.predicted_alpha_position_trades
                WHERE status = 'OPEN' AND position_side= 'LONG' 
                AND CAST(created_at AS DATE) BETWEEN '{start_date}' AND '{end_date}' ''' ) 
    open_l = cur.fetchone()
    
    #in case of Short Postion
    cur.execute(f'''SELECT COUNT(order_id)  FROM {schema}.predicted_alpha_position_trades
                WHERE status = 'Close' AND position_side= 'SHORT' 
                AND CAST(created_at AS DATE) BETWEEN '{start_date}' AND '{end_date}' ''' ) 
    open_s = cur.fetchone()
    #print("Total Open Orders are", open_l[0]+open_s[0])
    
    # SL Hit

    cur.execute(f''' SELECT COUNT(*)
        FROM {schema}.dydx_orders
        WHERE type = 'STOP_LIMIT' AND CAST(created_at AS DATE) BETWEEN '{start_date}' AND '{end_date}' ''')
    
    sl_hit = cur.fetchone()
    #print("Total SL  hits are",sl_hit[0])
    
    #Total open Size
    cur.execute(f'''SELECT SUM(CAST(size AS DECIMAL))
                FROM {schema}.predicted_alpha_position_trades
              WHERE status = 'OPEN' AND CAST(created_at AS DATE) BETWEEN '{start_date}' AND '{end_date}' ''')
    
    t_op = cur.fetchone()
    #print("Total open Size is ",t_op[0])

    #Current Open

    cur.execute(f'''
            WITH filtered_trades AS (
                SELECT
                    ft.strategy, pat.token,ft.order_id, CAST(pat.price AS DECIMAL),CAST(pat.size AS DECIMAL) AS qty, pat.status,
                    ft.vid, CAST(pat.created_at AS TIMESTAMP) AS trade_date,pat.position_side
                FROM
                    {schema}.predicted_alpha_position_trades pat
                JOIN
                    {schema}.dydx_orders ft ON pat.vid = ft.vid
                WHERE
                    CAST(ft.created_at AS DATE) BETWEEN '{start_date}' AND '{end_date}'
            ),
            buy_trades AS (
                SELECT
                    order_id,strategy,token,price AS buy_price,qty AS buy_qty,trade_date AS buy_date,status,vid
                FROM
                    filtered_trades
                WHERE
                    status = 'OPEN' AND position_side = 'LONG'
            ),
            sell_trades AS (
                SELECT DISTINCT
                    strategy
                FROM
                    filtered_trades
                WHERE
                    status = 'Close' AND position_side = 'LONG'
            ),
            open_trades AS (
                SELECT
                    b.vid,b.token,b.status,b.buy_price,b.buy_qty,b.buy_date
                FROM
                    buy_trades b
                LEFT JOIN
                    sell_trades s ON b.strategy = s.strategy
                WHERE
                    s.strategy IS NULL
            )
            SELECT
                *,
                (SELECT COUNT(*) FROM open_trades) AS total_open_trades
            FROM
                open_trades
        ''')

    # Fetch and print the results
    results = cur.fetchall()
    #for row in results:
       #print(row)
    df_1 = pd.DataFrame(results)
    # if results:
    #     print("Total Open Trades:", results[0][-1])  # The count is in the last column of each row
    # else:
    #     print("Total Open Trades: 0")

    
    #Realised PnL

    cur.execute(f'''WITH filtered_trades AS (
    SELECT strategy, market, CAST(order_price AS DECIMAL) AS price, CAST(size AS DECIMAL) AS qty, side, CAST(created_at AS TIMESTAMP) AS trade_date
    FROM {schema}.dydx_orders
    WHERE CAST(created_at AS DATE) BETWEEN '{start_date}' AND '{end_date}'
    ),
    sell_trades AS (
    SELECT strategy, market, price AS sell_price, qty AS sell_qty, trade_date AS sell_date
    FROM filtered_trades
    WHERE  side = 'SELL'
   ),
    buy_trades AS (
    SELECT strategy, market, price AS buy_price, qty AS buy_qty, trade_date AS buy_date
    FROM filtered_trades
    WHERE side = 'BUY'
    )
    SELECT
        SUM((s.sell_price - b.buy_price) * b.buy_qty) AS realized_pnl
    FROM
    sell_trades s
    JOIN buy_trades b ON s.strategy = b.strategy AND s.market = b.market AND s.sell_qty = b.buy_qty AND s.sell_date > b.buy_date 
    ''')
    re_pnl= cur.fetchone()
    #print('Realised PnL',re_pnl[0])

    #Unrealized PnL

    market_price  = 3475.36  
    cur.execute(f'''
     WITH filtered_trades AS ( SELECT strategy, market, CAST(order_price AS DECIMAL) AS price, CAST(size AS DECIMAL) AS qty, side, CAST(created_at AS DATE) AS trade_date
    FROM {schema}.dydx_orders
    WHERE CAST(created_at AS DATE) BETWEEN '{start_date}' AND '{end_date}'
    ),
    buy_trades AS (
    SELECT strategy, market, price AS buy_price,qty AS buy_qty, trade_date AS buy_date
    FROM filtered_trades
    WHERE side = 'BUY'
    ),
    sell_trades AS ( SELECT DISTINCT strategy
    FROM  filtered_trades
    WHERE side = 'SELL'
    )
    SELECT  SUM(({market_price} - b.buy_price) * b.buy_qty)
    FROM buy_trades b
    LEFT JOIN sell_trades s ON b.strategy <> s.strategy
     ''')
    un_pnl= cur.fetchone() 
    #print("Unrealized PnL",un_pnl[0])

    #Overall pnL

    #print("Overall PnL is ", re_pnl[0] + un_pnl[0])


    #Average PnL per Position
    
    cur.execute(f'''WITH filtered_trades AS (
    SELECT
    strategy, market, CAST(order_price AS DECIMAL) AS price, CAST(size AS DECIMAL) AS qty, side, CAST(created_at AS TIMESTAMP) AS trade_date
    FROM {schema}.dydx_orders
    WHERE CAST(created_at AS DATE) BETWEEN '{start_date}' AND '{end_date}'
    ),
    sell_trades AS ( SELECT strategy, market, price AS sell_price, qty AS sell_qty, trade_date AS sell_date 
    FROM filtered_trades
    WHERE side = 'SELL'
    ),
    buy_trades AS ( SELECT strategy, market, price AS buy_price, qty AS buy_qty, trade_date AS buy_date
    FROM filtered_trades
    WHERE side = 'BUY'
    ),
    matched_positions AS ( SELECT s.strategy,s.market,b.buy_price,s.sell_price, b.buy_qty,
        (s.sell_price - b.buy_price) * b.buy_qty AS pnl
    FROM sell_trades s
    JOIN buy_trades b ON s.strategy = b.strategy
                          AND s.market = b.market
                          AND s.sell_qty = b.buy_qty
                         AND s.sell_date > b.buy_date
    )
    SELECT AVG(pnl) AS avg_pnl_per_position FROM matched_positions
    ''') 
     
    a_pnl=cur.fetchone()
    #print("Average Pnl per Posotion",a_pnl[0])

    #Average Roi per Position

    cur.execute(f''' WITH filtered_trades AS (SELECT strategy, market,
        CAST(order_price AS DECIMAL) AS price, CAST(size AS DECIMAL) AS qty, side, CAST(created_at AS TIMESTAMP) AS trade_date
    FROM {schema}.dydx_orders
    WHERE CAST(created_at AS DATE) BETWEEN '{start_date}' AND '{end_date}'
    ),
    sell_trades AS ( SELECT strategy, market, price AS sell_price, qty AS sell_qty, trade_date AS sell_date
    FROM  filtered_trades
    WHERE side = 'SELL'
    ),  
    buy_trades AS (SELECT strategy, market, price AS buy_price, qty AS buy_qty, trade_date AS buy_date
    FROM filtered_trades
    WHERE side = 'BUY'
    ),
    matched_positions AS ( SELECT s.strategy, s.market, b.buy_price, s.sell_price,
    ((s.sell_price - b.buy_price) / b.buy_price) * 100 AS roi
    FROM sell_trades s
    JOIN buy_trades b ON s.strategy = b.strategy
                          AND s.market = b.market
                          AND s.sell_qty = b.buy_qty WHERE  s.sell_date > b.buy_date) 
    SELECT AVG(roi) AS avg_roi_per_position FROM matched_positions
    ''') 

    roi = cur.fetchone()
    #print("Average ROI is",roi[0])



    ### making table for closed position 
    cur.execute(f'''
        WITH filtered_trades AS ( SELECT strategy, market, order_id, CAST(order_price AS DECIMAL) AS price, CAST(size AS DECIMAL) AS qty,
           side, vid, CAST(created_at AS TIMESTAMP) AS trade_date
        FROM {schema}.dydx_orders
        WHERE CAST(created_at AS DATE) BETWEEN '{start_date}' AND '{end_date}'),
        sell_trades AS (
        SELECT strategy, market, order_id, price AS sell_price, qty AS sell_qty, trade_date AS sell_date, vid
        FROM filtered_trades
        WHERE side = 'SELL' ),
        buy_trades AS (
            SELECT
                strategy, market, order_id, price AS buy_price, qty AS buy_qty,trade_date AS buy_date,vid
            FROM
                filtered_trades
            WHERE
                side = 'BUY'
        ),
        closed_trades AS (
            SELECT b.vid AS buy_vid, s.vid AS sell_vid, b.strategy,b.market, b.buy_price,s.sell_price,b.buy_qty,b.buy_date, s.sell_date, (s.sell_price - b.buy_price) * b.buy_qty AS pnl,
                'CLOSED' AS position_status
            FROM
                buy_trades b
            JOIN sell_trades s ON b.strategy = s.strategy
                            AND b.market = s.market
                            AND b.buy_qty = s.sell_qty
                            AND s.sell_date > b.buy_date
        )
        SELECT
            *
        FROM
            closed_trades
    ''')

    # Fetch and process the results
    results_closed = cur.fetchall()
    columns = ['buy_vid', 'sell_vid', 'strategy', 'market', 'buy_price', 'sell_price', 'buy_qty', 'buy_date', 'sell_date', 'pnl', 'position_status']
    df = pd.DataFrame(results_closed, columns=columns)

    # Display the DataFrame
    #print(df.head())

except Exception as error:
    print(error)

finally:
    if cur is not None:
        cur.close()  # Correctly close the cursor
    if conn is not None:
        conn.close()


# Create a 2-column layout for better visualization
col1, col2, col3 = st.columns(3)
col4, col5, col6 = st.columns(3)
col7, col8, col9 = st.columns(3)
col10, col11, col12 = st.columns(3)

# Metrics
with col1:
     st.metric(label='Overall PnL', value= float(re_pnl[0] + un_pnl[0]))
with col2:
    st.metric(label='Realized PnL', value= float(re_pnl[0]))
with col3:
    st.metric(label='Unrealized PnL', value= float(un_pnl[0]))
with col4:
    if results:
        st.metric(label='Currently Open Positions', value= float(results[0][-1]))
    else:
        st.metric(label='Currently Open Positions', value= 0)
with col5:
    st.metric(label='Total Closed Positions', value=float(close_l[0] + close_s[0]))
with col6:
    st.metric(label='Total Trades', value= float(total_tr[0]))
with col7:
    st.metric(label='Avg PnL Per Position', value=float(a_pnl[0]))
with col8:
    st.metric(label='Avg RoI Per Position', value=float(roi[0]))
with col9:
    st.metric(label='Total Open Orders', value=float(open_l[0] +open_s[0]))
with col10:
    st.metric(label='SL Hit', value=float(sl_hit[0]))
with col11:
    st.metric(label='Total Open Size (USD)', value=float(t_op[0]))
with col12:
    st.metric(label='Current Leverage', value='None')

if results:
    st.header("Currently Open Trade")
    df_1.rename(columns={0: 'vid', 1: 'token', 2: 'pos_side(dydx)', 3: 'open_price(dydx)', 4: 'open_size(dydx)',5: 'open_time(dydx)', 6: 'total_open_trades'}, inplace=True)
    df_1.drop('total_open_trades',axis = 1,inplace = True)
    st.dataframe(df_1)

if results_closed:
    st.header('Closed Positions')
    st.dataframe(df)

if trade_data:
    st.header('Trades')
    st.dataframe(trade)
