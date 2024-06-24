import streamlit as st
def fetch_data_from_db1(conn,model,start_date,end_date):
   
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
    

    curr = conn.cursor()
    
    #Total trade
    query_1 = f'''SELECT COUNT(order_id)  FROM {model}.predicted_alpha_position_trades 
                WHERE CAST(created_at AS DATE) BETWEEN '{start_date}' AND '{end_date}' ''' 
    
    curr.execute(query_1)
    temp = curr.fetchone()
    if temp:
        total_trade = temp[0]
    else:
        return [total_trade ,trade_data, total_close_position ,total_open_position ,realised_pnl, unrealised_pnl,overall_pnl,currently_open, avg_pnl,avg_roi,sl_hit,total_open_size,closed_trades_data,trade_cursor_data]
    
    #Total Trades Position
    curr.execute(f''' SELECT
        SPLIT_PART(ft.strategy, '_', 1) AS strategy_prefix,
        pat.token,
        ft.order_id,
        CAST(pat.price AS DECIMAL) AS price,
        CAST(pat.dydx_size AS DECIMAL) AS qty,
        ft.side,
        pat.status,
        ft.vid,
        CAST(pat.created_at AS TIMESTAMP) AS trade_date
    FROM
        {model}.predicted_alpha_position_trades pat
    LEFT JOIN
        {model}.dydx_orders ft ON pat.vid = ft.vid and ft.status = 'FILLED'
    WHERE
        CAST(ft.created_at AS DATE) BETWEEN '{start_date}' AND '{end_date}'
        ''')
    trade_data = curr.fetchall()



    #Total Close  and Open Position
    
    query_2_4 = f'''WITH filtered_trades AS (
    SELECT
        SPLIT_PART(ft.strategy, '_', 1) AS strategy_prefix,
        pat.token,
        ft.order_id,
        CAST(pat.price AS DECIMAL) AS price,
        CAST(pat.dydx_size AS DECIMAL) AS qty,
        ft.side,
        pat.status,
        ft.vid,
        CAST(pat.created_at AS TIMESTAMP) AS trade_date
    FROM
        {model}.predicted_alpha_position_trades pat
    LEFT JOIN
        {model}.dydx_orders ft ON pat.vid = ft.vid and ft.status = 'FILLED'
    WHERE
        CAST(ft.created_at AS DATE) BETWEEN '{start_date}' AND '{end_date}'
),
long_open_trades AS (
    SELECT
        strategy_prefix,
        price AS buy_price,
        qty AS buy_qty,
        trade_date AS buy_date,
        vid
    FROM
        filtered_trades
    WHERE
        side = 'BUY' AND status = 'OPEN'
),
short_open_trades AS (
    SELECT
        strategy_prefix,
        price AS sell_price,
        qty AS sell_qty,
        trade_date AS sell_date,
        vid
    FROM
        filtered_trades
    WHERE
        side = 'SELL' AND status = 'OPEN'
),
long_close_trades AS (
    SELECT
        strategy_prefix,
        price AS sell_price,
        qty AS sell_qty,
        trade_date AS sell_date,
        vid
    FROM
        filtered_trades
    WHERE
        side = 'SELL' AND status = 'Close'
),
short_close_trades AS (
    SELECT
        strategy_prefix,
        price AS buy_price,
        qty AS buy_qty,
        trade_date AS buy_date,
        vid
    FROM
        filtered_trades
    WHERE
        side = 'BUY' AND status = 'Close'
)

SELECT
    'OPEN' AS position_type,
    COUNT(*) AS total_open_positions
FROM
    (
        SELECT strategy_prefix, vid FROM long_open_trades
        UNION ALL
        SELECT strategy_prefix, vid FROM short_open_trades
    ) AS open_positions

UNION ALL

SELECT
    'CLOSE' AS position_type,
    COUNT(*) AS total_close_positions
FROM
    (
        SELECT strategy_prefix, vid FROM long_close_trades
        UNION ALL
        SELECT strategy_prefix, vid FROM short_close_trades
    ) AS close_positions'''
    curr.execute(query_2_4)
    temp = curr.fetchall()
    total_open_position =  temp[0][-1]
    total_close_position = temp[1][-1]


    #total open size

    
    query_4 = f'''SELECT SUM(CAST(dydx_size AS DECIMAL))
                FROM {model}.predicted_alpha_position_trades
              WHERE status = 'OPEN' AND CAST(created_at AS DATE) BETWEEN '{start_date}' AND '{end_date}' '''
    curr.execute(query_4)
    temp = curr.fetchone()   
    total_open_size = temp[0]

    #CURRENT OPEN POSITIONS 
    query_5 = f'''WITH filtered_trades AS (
    SELECT
        SPLIT_PART(ft.strategy, '_', 1) AS strategy_prefix,
        pat.token,
        ft.order_id,
        CAST(pat.price AS DECIMAL) AS price,
        CAST(pat.dydx_size AS DECIMAL) AS qty,
        ft.side,
        pat.status,
        ft.vid,
        CAST(pat.created_at AS TIMESTAMP) AS trade_date
    FROM
        {model}.predicted_alpha_position_trades pat
    LEFT JOIN
        {model}.dydx_orders ft ON pat.vid = ft.vid AND ft.status = 'FILLED'
    WHERE
        CAST(ft.created_at AS DATE) BETWEEN '{start_date}' AND '{end_date}'
),

open_positions AS (
    SELECT
        strategy_prefix,
        price AS entry_price,
        qty AS quantity,
        side,status,
        token
    FROM
        filtered_trades
    WHERE
        status = 'OPEN'
),

open_positions_no_close AS (
    SELECT
        op.strategy_prefix,
        op.side,
        op.status,
        op.token,
        op.entry_price,
        op.quantity,
        CASE
            WHEN op.side = 'BUY' THEN (3415 - op.entry_price) * op.quantity
            WHEN op.side = 'SELL' THEN (op.entry_price - 3415) * op.quantity
            ELSE 0 
        END AS unrealized_pnl
    FROM
        open_positions op
    WHERE
        NOT EXISTS (
            SELECT 1
            FROM filtered_trades ft
            WHERE ft.strategy_prefix = op.strategy_prefix
              AND ft.token = op.token
              AND ft.status = 'Close'
        )
)

SELECT
   *
FROM
    open_positions_no_close
'''
    curr.execute(query_5)
    currently_open = curr.fetchall()

    #Average Roi per Position
    query_9 = f'''WITH filtered_trades AS (
    SELECT
        SPLIT_PART(ft.strategy, '_', 1) AS strategy_prefix,
        pat.token,
        ft.order_id,
        CAST(pat.price AS DECIMAL) AS price,
        CAST(pat.dydx_size AS DECIMAL) AS qty,
        ft.side,
        pat.status,
        ft.vid,
        CAST(pat.created_at AS TIMESTAMP) AS trade_date
    FROM
        {model}.predicted_alpha_position_trades pat
    LEFT JOIN
        {model}.dydx_orders ft ON pat.vid = ft.vid and ft.status = 'FILLED'

    WHERE
        CAST(ft.created_at AS DATE) BETWEEN '{start_date}' AND '{end_date}'
),
   long_open_trades AS (
    SELECT
        strategy_prefix,
        price AS buy_price,
        qty AS buy_qty,
        trade_date AS buy_date,
        vid
    FROM
        filtered_trades
    WHERE
        side = 'BUY' AND status = 'OPEN'
),
short_open_trades AS (
    SELECT
        strategy_prefix,
        price AS sell_price,
        qty AS sell_qty,
        trade_date AS sell_date,
        vid
    FROM
        filtered_trades
    WHERE
        side = 'SELL' AND status = 'OPEN'
),
long_close_trades AS (
    SELECT
        strategy_prefix,
        price AS sell_price,
        qty AS sell_qty,
        trade_date AS sell_date,
        vid
    FROM
        filtered_trades
    WHERE
        side = 'SELL' AND status = 'Close'
),
short_close_trades AS (
    SELECT
        strategy_prefix,
        price AS buy_price,
        qty AS buy_qty,
        trade_date AS buy_date,
        vid
    FROM
        filtered_trades
    WHERE
        side = 'BUY' AND status = 'Close'
), 
long_realized_roi AS (
    SELECT
         (s.sell_price - b.buy_price) / b.buy_price * 100 AS roi
    FROM
        long_open_trades b
    JOIN
        long_close_trades s ON b.strategy_prefix = s.strategy_prefix AND s.sell_qty = b.buy_qty AND b.buy_date < s.sell_date
),
    short_realized_roi AS (
    SELECT
        (sb.buy_price - ss.sell_price) / ss.sell_price * 100 AS roi
    FROM
        short_open_trades ss
    JOIN
        short_close_trades sb ON ss.strategy_prefix = sb.strategy_prefix AND ss.sell_qty = sb.buy_qty AND sb.buy_date > ss.sell_date
        )
             SELECT
    AVG(roi) AS roi_pnl_per_position
 FROM
    (
        SELECT roi FROM long_realized_roi
        UNION ALL
        SELECT roi FROM short_realized_roi
    ) AS combined_roi
'''
    curr.execute(query_9)
    temp = curr.fetchone()
    avg_roi = temp[0]

    if avg_roi is None:
        avg_roi = 0


    ### making table for total closed position 
    query_10= f'''WITH filtered_trades AS (
    SELECT
        SPLIT_PART(ft.strategy, '_', 1) AS strategy_prefix,
        pat.token,
        ft.order_id,
        CAST(pat.price AS DECIMAL) AS price,
        CAST(pat.dydx_size AS DECIMAL) AS qty,
        ft.side,
        pat.status,
        ft.vid,
        CAST(pat.created_at AS TIMESTAMP) AS trade_date
    FROM
        {model}.predicted_alpha_position_trades pat
    LEFT JOIN
        {model}.dydx_orders ft ON pat.vid = ft.vid and ft.status = 'FILLED'
    WHERE
       CAST(ft.created_at AS DATE) BETWEEN '{start_date}' AND '{end_date}'
),
long_open_trades AS (
    SELECT
        strategy_prefix,
        price AS buy_price,
        qty AS buy_qty,
        trade_date AS buy_date,
        vid
    FROM
        filtered_trades
    WHERE
        side = 'BUY' AND status = 'OPEN'
),
short_open_trades AS (
    SELECT
        strategy_prefix,
        price AS sell_price,
        qty AS sell_qty,
        trade_date AS sell_date,
        vid
    FROM
        filtered_trades
    WHERE
        side = 'SELL' AND status = 'OPEN'
),
long_close_trades AS (
    SELECT
        strategy_prefix,
        price AS sell_price,
        qty AS sell_qty,
        trade_date AS sell_date,
        vid
    FROM
        filtered_trades
    WHERE
        side = 'SELL' AND status = 'Close'
),
short_close_trades AS (
    SELECT
        strategy_prefix,
        price AS buy_price,
        qty AS buy_qty,
        trade_date AS buy_date,
        vid
    FROM
        filtered_trades
    WHERE
        side = 'BUY' AND status = 'Close'
),
long_realized_pnl AS (
    SELECT
        *
    FROM
        long_open_trades b
    JOIN
        long_close_trades s ON b.strategy_prefix = s.strategy_prefix AND s.sell_qty = b.buy_qty AND b.buy_date < s.sell_date
),
short_realized_pnl AS (
    SELECT
       *
    FROM
        short_open_trades ss
    JOIN
        short_close_trades sb ON ss.strategy_prefix = sb.strategy_prefix AND ss.sell_qty = sb.buy_qty AND sb.buy_date > ss.sell_date
)
select  * from long_realized_pnl
union all
select *  from short_realized_pnl
     '''
    curr.execute(query_10)
    closed_trades_data = curr.fetchall()

    if closed_trades_data is None:
        closed_trades_data = None

    #Trade Cursor Table
    
    # curr.execute(f'''SELECT *  FROM {model}.trade_cursor ''')
    # #WHERE CAST(created_at AS DATE) BETWEEN '{start_date}' AND '{end_date}' 

    # trade_cursor_data  = curr.fetchall()
    # st.text(trade_cursor_data)

    # Realized PnL
    query_6 = f'''WITH filtered_trades AS (
    SELECT
        SPLIT_PART(ft.strategy, '_', 1) AS strategy_prefix,
        pat.token,
        ft.order_id,
        CAST(pat.price AS DECIMAL) AS price,
        CAST(pat.dydx_size AS DECIMAL) AS qty,
        ft.side,
        pat.status,
        ft.vid,
        CAST(pat.created_at AS TIMESTAMP) AS trade_date
    FROM
        {model}.predicted_alpha_position_trades pat
    LEFT JOIN
        {model}.dydx_orders ft ON pat.vid = ft.vid and ft.status = 'FILLED'

    WHERE
        CAST(ft.created_at AS DATE) BETWEEN '{start_date}' AND '{end_date}'
),
long_open_trades AS (
    SELECT
        strategy_prefix,
        price AS buy_price,
        qty AS buy_qty,
        trade_date AS buy_date,
        vid
    FROM
        filtered_trades
    WHERE
        side = 'BUY' AND status = 'OPEN'
),
short_open_trades AS (
    SELECT
        strategy_prefix,
        price AS sell_price,
        qty AS sell_qty,
        trade_date AS sell_date,
        vid
    FROM
        filtered_trades
    WHERE
        side = 'SELL' AND status = 'OPEN'
),
long_close_trades AS (
    SELECT
        strategy_prefix,
        price AS sell_price,
        qty AS sell_qty,
        trade_date AS sell_date,
        vid
    FROM
        filtered_trades
    WHERE
        side = 'SELL' AND status = 'Close'
),
short_close_trades AS (
    SELECT
        strategy_prefix,
        price AS buy_price,
        qty AS buy_qty,
        trade_date AS buy_date,
        vid
    FROM
        filtered_trades
    WHERE
        side = 'BUY' AND status = 'Close'
),

long_realized_pnl AS (
    SELECT
        SUM((s.sell_price - b.buy_price) * b.buy_qty) AS pnl
    FROM
        long_open_trades b
    JOIN
        long_close_trades s ON b.strategy_prefix = s.strategy_prefix AND s.sell_qty = b.buy_qty AND b.buy_date < s.sell_date
),
short_realized_pnl AS (
    SELECT
        SUM((sb.buy_price - ss.sell_price) * ss.sell_qty) AS pnl
    FROM
        short_open_trades ss
    JOIN
        short_close_trades sb ON ss.strategy_prefix = sb.strategy_prefix AND ss.sell_qty = sb.buy_qty AND sb.buy_date > ss.sell_date
)
SELECT
    COALESCE(lrp.pnl, 0) + COALESCE(srp.pnl, 0) AS realized_pnl
FROM
    long_realized_pnl lrp
    CROSS JOIN short_realized_pnl srp
'''
    curr.execute(query_6)
    temp = curr.fetchone()
    realised_pnl = temp[0]
    if realised_pnl is None:
        realised_pnl =0

    #Average PnL per Position
    query_7= f'''WITH filtered_trades AS (
    SELECT
        SPLIT_PART(ft.strategy, '_', 1) AS strategy_prefix,
        pat.token,
        ft.order_id,
        CAST(pat.price AS DECIMAL) AS price,
        CAST(pat.dydx_size AS DECIMAL) AS qty,
        ft.side,
        pat.status,
        ft.vid,
        CAST(pat.created_at AS TIMESTAMP) AS trade_date
    FROM
        {model}.predicted_alpha_position_trades pat
    LEFT JOIN
        {model}.dydx_orders ft ON pat.vid = ft.vid and ft.status = 'FILLED'

    WHERE
        CAST(ft.created_at AS DATE) BETWEEN '{start_date}' AND '{end_date}'
                ),
                long_open_trades AS (
    SELECT
        strategy_prefix,
        price AS buy_price,
        qty AS buy_qty,
        trade_date AS buy_date,
        vid
    FROM
        filtered_trades
    WHERE
        side = 'BUY' AND status = 'OPEN'
),
short_open_trades AS (
    SELECT
        strategy_prefix,
        price AS sell_price,
        qty AS sell_qty,
        trade_date AS sell_date,
        vid
    FROM
        filtered_trades
    WHERE
        side = 'SELL' AND status = 'OPEN'
),
long_close_trades AS (
    SELECT
        strategy_prefix,
        price AS sell_price,
        qty AS sell_qty,
        trade_date AS sell_date,
        vid
    FROM
        filtered_trades
    WHERE
        side = 'SELL' AND status = 'Close'
),
short_close_trades AS (
    SELECT
        strategy_prefix,
        price AS buy_price,
        qty AS buy_qty,
        trade_date AS buy_date,
        vid
    FROM
        filtered_trades
    WHERE
        side = 'BUY' AND status = 'Close'
),

    long_realized_pnl AS (
    SELECT
        (s.sell_price - b.buy_price) * b.buy_qty AS pnl
    FROM
        long_open_trades b
    JOIN
        long_close_trades s ON b.strategy_prefix = s.strategy_prefix AND s.sell_qty = b.buy_qty AND b.buy_date < s.sell_date
),
short_realized_pnl AS (
    SELECT
        (sb.buy_price - ss.sell_price) * ss.sell_qty AS pnl
    FROM
        short_open_trades ss
    JOIN
        short_close_trades sb ON ss.strategy_prefix = sb.strategy_prefix AND ss.sell_qty = sb.buy_qty AND sb.buy_date > ss.sell_date
        )
             SELECT
    AVG(pnl) AS average_pnl_per_position
 FROM
    (
        SELECT pnl FROM long_realized_pnl
        UNION ALL
        SELECT pnl FROM short_realized_pnl
    ) AS combined_pnl
                '''
    
    curr.execute(query_7)
    temp = curr.fetchone()
    avg_pnl = temp[0]
    if avg_pnl is None:
        avg_pnl =0

    #Unrealized PnL
    market_price  = 3475.36   
    ##replace with api
    query_8 = f'''WITH filtered_trades AS (
    SELECT
        SPLIT_PART(ft.strategy, '_', 1) AS strategy_prefix,
        pat.token,
        ft.order_id,
        CAST(pat.price AS DECIMAL) AS price,
        CAST(pat.dydx_size AS DECIMAL) AS qty,
        ft.side,
        pat.status,
        ft.vid,
        CAST(pat.created_at AS TIMESTAMP) AS trade_date
    FROM
        {model}.predicted_alpha_position_trades pat
    LEFT JOIN
        {model}.dydx_orders ft ON pat.vid = ft.vid AND ft.status = 'FILLED'
    WHERE
        CAST(ft.created_at AS DATE) BETWEEN '{start_date}' AND '{end_date}'
),

open_positions AS (
    SELECT
        strategy_prefix,
        price AS entry_price,
        qty AS quantity,
        side,
        token
    FROM
        filtered_trades
    WHERE
        status = 'OPEN'
),

open_positions_no_close AS (
    SELECT
        op.strategy_prefix,
        op.side,
        op.token,
        op.entry_price,
        op.quantity,
        CASE
            WHEN op.side = 'BUY' THEN ({market_price} - op.entry_price) * op.quantity
            WHEN op.side = 'SELL' THEN (op.entry_price - {market_price}) * op.quantity
            ELSE 0 
        END AS unrealized_pnl
    FROM
        open_positions op
    WHERE
        NOT EXISTS (
            SELECT 1
            FROM filtered_trades ft
            WHERE ft.strategy_prefix = op.strategy_prefix
              AND ft.token = op.token
              AND ft.status = 'Close'
        )
)

SELECT
    SUM(unrealized_pnl) AS total_unrealized_pnl
FROM
    open_positions_no_close

                '''
    curr.execute(query_8)
    temp = curr.fetchone()
    unrealised_pnl = temp[0]
    if unrealised_pnl is None:
        unrealised_pnl =0


    #Overall pnL
    overall_pnl = float(realised_pnl) + float(unrealised_pnl)

 
    return [total_trade ,trade_data,total_close_position ,total_open_position ,realised_pnl, unrealised_pnl,overall_pnl,currently_open, avg_pnl,avg_roi,sl_hit,total_open_size, closed_trades_data,trade_cursor_data]