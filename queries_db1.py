
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
    temp = curr.fetchall()
    if temp:
        total_trade = temp[0]
    else:
        return [total_trade ,trade_data, total_close_position ,total_open_position ,realised_pnl, unrealised_pnl,overall_pnl,currently_open, avg_pnl,avg_roi,sl_hit,total_open_size,closed_trades_data,trade_cursor_data]
    
    #Total Trades Position
    curr.execute(f''' SELECT
                    ft.vid, pat.token,pat.position_side,pat.status,pat.size,pat.price, CAST(pat.created_at AS TIMESTAMP) AS trade_date
                FROM
                    {model}.predicted_alpha_position_trades pat
                JOIN
                    {model}.dydx_orders ft ON pat.vid = ft.vid
                WHERE
                    CAST(ft.created_at AS DATE) BETWEEN '{start_date}' AND '{end_date}' ''')
    trade_data = curr.fetchall()
    #Total Close Position
    
    #In case of Long Position
    query_2_1 = f'''SELECT COUNT(order_id)  FROM {model}.predicted_alpha_position_trades 
                WHERE status = 'Close' AND position_side= 'LONG' 
                AND CAST(created_at AS DATE) BETWEEN '{start_date}' AND '{end_date}' ''' 
    close_l = curr.execute(query_2_1)
    
    #In case of Short Position
    query_2_2 =f'''SELECT COUNT(order_id)  FROM {model}.predicted_alpha_position_trades 
                WHERE status = 'OPEN' AND position_side= 'SHORT' 
                AND CAST(created_at AS DATE) BETWEEN '{start_date}' AND '{end_date}' ''' 
    close_s =  curr.execute(query_2_2)
    #print("Total Close Position are",close_l[0]+close_s[0])
    total_close_position = close_l[0] +close_s[0]



    # Total OPEN Position
     
    #inclase of long position
    query_3_1 = f'''SELECT COUNT(order_id)  FROM {model}.predicted_alpha_position_trades
                WHERE status = 'OPEN' AND position_side= 'LONG' 
                AND CAST(created_at AS DATE) BETWEEN '{start_date}' AND '{end_date}' ''' 
    open_l = curr.execute(query_3_1)
    
    #in case of Short Postion
    query_3_2= f'''SELECT COUNT(order_id)  FROM {model}.predicted_alpha_position_trades
                WHERE status = 'Close' AND position_side= 'SHORT' 
                AND CAST(created_at AS DATE) BETWEEN '{start_date}' AND '{end_date}' ''' 
    open_S = curr.execute(query_3_2)
    total_open_position = open_l[0] + open_S[0]

    #total open size
    query_4 = f'''SELECT SUM(CAST(size AS DECIMAL))
                FROM {model}.predicted_alpha_position_trades
              WHERE status = 'OPEN' AND CAST(created_at AS DATE) BETWEEN '{start_date}' AND '{end_date}' '''
    temp = curr.execute(query_4)
    total_open_size = temp[0]

    #CURRENT OPEN POSITIONS 
    query_5 = f'''
            WITH filtered_trades AS (
                SELECT
                    ft.strategy, pat.token,ft.order_id, CAST(pat.price AS DECIMAL),CAST(pat.size AS DECIMAL) AS qty, pat.status,
                    ft.vid, CAST(pat.created_at AS TIMESTAMP) AS trade_date,pat.position_side
                FROM
                    {model}.predicted_alpha_position_trades pat
                JOIN
                    {model}.dydx_orders ft ON pat.vid = ft.vid
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
        '''
    curr.execute(query_5)
    currently_open = curr.fetchall()
    # Realized PnL
    query_6 = f'''WITH filtered_trades AS (
                SELECT
                    ft.strategy, pat.token,ft.order_id, CAST(pat.price AS DECIMAL),CAST(pat.size AS DECIMAL) AS qty, pat.status,
                    ft.vid, CAST(pat.created_at AS TIMESTAMP) AS trade_date,pat.position_side
                FROM
                    {model}.predicted_alpha_position_trades pat
                JOIN
                    {model}.dydx_orders ft ON pat.vid = ft.vid
                WHERE
                    CAST(ft.created_at AS DATE) BETWEEN '{start_date}' AND '{end_date}'
                ),
                sell_trades AS (
                SELECT 
                    strategy, token, price AS sell_price, qty AS sell_qty, trade_date AS sell_date
                FROM 
                    filtered_trades
                WHERE  
                    status = 'Close'
                ),
                buy_trades AS (
                SELECT 
                    strategy, token, price AS buy_price, qty AS buy_qty, trade_date AS buy_date
                FROM 
                    filtered_trades
                WHERE 
                    status = 'OPEN'
                )
                SELECT
                    SUM((s.sell_price - b.buy_price) * b.buy_qty) AS realized_pnl
                FROM
                sell_trades s
                JOIN buy_trades b ON s.strategy = b.strategy AND s.token = b.token AND s.sell_qty = b.buy_qty AND s.sell_date > b.buy_date 
                '''
    curr.execute(query_6)
    temp = curr.fetchone()
    realised_pnl = temp[0] 
    #Average PnL per Position
    query_7= f'''WITH filtered_trades AS (
                SELECT
                    ft.strategy, pat.token,ft.order_id, CAST(pat.price AS DECIMAL),CAST(pat.size AS DECIMAL) AS qty, pat.status,
                    ft.vid, CAST(pat.created_at AS TIMESTAMP) AS trade_date,pat.position_side
                FROM
                    {model}.predicted_alpha_position_trades pat
                JOIN
                    {model}.dydx_orders ft ON pat.vid = ft.vid
                WHERE
                    CAST(ft.created_at AS DATE) BETWEEN '{start_date}' AND '{end_date}'
                ),
                sell_trades AS (
                SELECT 
                    strategy, token, price AS sell_price, qty AS sell_qty, trade_date AS sell_date 
                FROM 
                    filtered_trades
                WHERE status = 'Close'
                ),
                buy_trades AS ( 
                SELECT 
                    strategy, token, price AS buy_price, qty AS buy_qty, trade_date AS buy_date
                FROM filtered_trades
                WHERE side = 'BUY'
                ),
                matched_positions AS (
                SELECT 
                    s.strategy,s.token,b.buy_price,s.sell_price, b.buy_qty, (s.sell_price - b.buy_price) * b.buy_qty AS pnl
                FROM sell_trades s
                JOIN buy_trades b ON s.strategy = b.strategy
                          AND s.token = b.token
                          AND s.sell_qty = b.buy_qty
                          AND s.sell_date > b.buy_date
                )
                SELECT AVG(pnl) AS avg_pnl_per_position FROM matched_positions
            
                '''
    
    curr.execute(query_7)
    temp = curr.fetchone()
    avg_pnl = temp[0]
    
    #Unrealized PnL
    market_price  = 3475.36   
    ##replace with api
    query_8 = f'''WITH filtered_trades AS (  SELECT
                    ft.strategy, pat.token,ft.order_id, CAST(pat.price AS DECIMAL),CAST(pat.size AS DECIMAL) AS qty, pat.status,
                    ft.vid, CAST(pat.created_at AS TIMESTAMP) AS trade_date,pat.position_side
                FROM
                    {model}.predicted_alpha_position_trades pat
                JOIN
                    {model}.dydx_orders ft ON pat.vid = ft.vid
                WHERE
                    CAST(ft.created_at AS DATE) BETWEEN '{start_date}' AND '{end_date}'
                ),
                buy_trades AS (
                SELECT 
                    strategy, market, price AS buy_price,qty AS buy_qty, trade_date AS buy_date
                FROM 
                    filtered_trades
                WHERE status = 'OPEN'
                ),
                sell_trades AS ( SELECT DISTINCT strategy
                FROM  filtered_trades
                WHERE status = ''
                )
                SELECT  SUM(({market_price} - b.buy_price) * b.buy_qty)
                FROM buy_trades b
                LEFT JOIN sell_trades s ON b.strategy <> s.strategy
                '''
    curr.execute(query_8)
    temp = curr.fetchone()
    unrealised_pnl = temp[0]

    #Overall pnL
    overall_pnl = realised_pnl + unrealised_pnl

    #Average Roi per Position
    query_9 = f''' WITH filtered_trades AS ( 
                SELECT
                    ft.strategy, pat.token,ft.order_id, CAST(pat.price AS DECIMAL),CAST(pat.size AS DECIMAL) AS qty, pat.status,
                    ft.vid, CAST(pat.created_at AS TIMESTAMP) AS trade_date,pat.position_side
                FROM
                    {model}.predicted_alpha_position_trades pat
                JOIN
                    {model}.dydx_orders ft ON pat.vid = ft.vid
                WHERE
                    CAST(ft.created_at AS DATE) BETWEEN '{start_date}' AND '{end_date}'
                ),
            sell_trades AS ( 
                SELECT 
                    strategy, token, price AS sell_price, qty AS sell_qty, trade_date AS sell_date
                FROM  
                    filtered_trades
                WHERE 
                    status = 'Close'
            ),  
            buy_trades AS (
                SELECT 
                    strategy, token, price AS buy_price, qty AS buy_qty, trade_date AS buy_date
                FROM 
                    filtered_trades
                WHERE status = 'OPEN'
            ),
            matched_positions AS ( 
                SELECT s.strategy, s.token, b.buy_price, s.sell_price, ((s.sell_price - b.buy_price) / b.buy_price) * 100 AS roi
            FROM sell_trades s
            JOIN buy_trades b ON s.strategy = b.strategy
                                AND s.token = b.token
                                AND s.sell_qty = b.buy_qty WHERE  
                                s.sell_date > b.buy_date) 
            SELECT AVG(roi) AS avg_roi_per_position FROM matched_positions
            '''
    curr.execute(query_9)
    temp = curr.fetchone()
    avg_roi = temp[0]

    ### making table for total closed position 
    query_10= f'''WITH filtered_trades AS ( 
                SELECT
                    ft.strategy, pat.token,ft.order_id, CAST(pat.price AS DECIMAL),CAST(pat.size AS DECIMAL) AS qty, pat.status,
                    ft.vid, CAST(pat.created_at AS TIMESTAMP) AS trade_date,pat.position_side
                FROM
                    {model}.predicted_alpha_position_trades pat
                JOIN
                    {model}.dydx_orders ft ON pat.vid = ft.vid
                WHERE
                    CAST(ft.created_at AS DATE) BETWEEN '{start_date}' AND '{end_date}'
                ),
        sell_trades AS (
        SELECT 
            strategy, token, order_id, price AS sell_price, qty AS sell_qty, trade_date AS sell_date, vid
        FROM 
            filtered_trades
        WHERE 
            side = 'SELL' 
        ),
        buy_trades AS (
            SELECT
                strategy, token, order_id, price AS buy_price, qty AS buy_qty,trade_date AS buy_date,vid
            FROM
                filtered_trades
            WHERE
                side = 'BUY'
        ),
        closed_trades AS (
            SELECT 
                b.vid AS buy_vid, s.vid AS sell_vid, b.strategy,b.market, b.buy_price,s.sell_price,b.buy_qty,b.buy_date, s.sell_date, (s.sell_price - b.buy_price) * b.buy_qty AS pnl,
                'CLOSED' AS position_status
            FROM
                buy_trades b
            JOIN sell_trades s ON b.strategy = s.strategy
                            AND b.market = s.market
                            AND b.buy_qty = s.sell_qty
                            AND s.sell_date > b.buy_date
        )
        SELECT * FROM closed_trades
     '''
    curr.execute(query_10)
    closed_trades_data = curr.fetchall()

    #Trade Cursor Table
    
    curr.execute(f'''SELECT *  FROM {model}.trade_cursor 
                WHERE CAST(created_at AS DATE) BETWEEN '{start_date}' AND '{end_date}' ''' )

    trade_cursor_data  = curr.fetchall()

    return [total_trade ,trade_data,total_close_position ,total_open_position ,realised_pnl, unrealised_pnl,overall_pnl,currently_open, avg_pnl,avg_roi,sl_hit,total_open_size, closed_trades_data,trade_cursor_data]

