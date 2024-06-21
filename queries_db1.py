
def fetch_data_from_db1(conn,model,start_date,end_date):
    #Total trade
    query_1 = f'''SELECT COUNT(order_id)  FROM {model}.predicted_alpha_position_trades 
                WHERE CAST(created_at AS DATE) BETWEEN '{start_date}' AND '{end_date}' ''' 
    
    #Total Close Position
    
    #In case of Long Position
    query_2_1 = f'''SELECT COUNT(order_id)  FROM {model}.predicted_alpha_position_trades 
                WHERE status = 'Close' AND position_side= 'LONG' 
                AND CAST(created_at AS DATE) BETWEEN '{start_date}' AND '{end_date}' ''' 

    #In case of Short Position
    query_2_2 =f'''SELECT COUNT(order_id)  FROM {model}.predicted_alpha_position_trades 
                WHERE status = 'OPEN' AND position_side= 'SHORT' 
                AND CAST(created_at AS DATE) BETWEEN '{start_date}' AND '{end_date}' ''' 
    #print("Total Close Position are",close_l[0]+close_s[0])

    # Total OPEN Position
     
    #inclase of long position
    query_3_1 = f'''SELECT COUNT(order_id)  FROM {model}.predicted_alpha_position_trades
                WHERE status = 'OPEN' AND position_side= 'LONG' 
                AND CAST(created_at AS DATE) BETWEEN '{start_date}' AND '{end_date}' ''' 
    
    #in case of Short Postion
    query_3_2= f'''SELECT COUNT(order_id)  FROM {model}.predicted_alpha_position_trades
                WHERE status = 'Close' AND position_side= 'SHORT' 
                AND CAST(created_at AS DATE) BETWEEN '{start_date}' AND '{end_date}' ''' 
    
    #total open size
    query_4 = f'''SELECT SUM(CAST(size AS DECIMAL))
                FROM {model}.predicted_alpha_position_trades
              WHERE status = 'OPEN' AND CAST(created_at AS DATE) BETWEEN '{start_date}' AND '{end_date}' '''
    
    
    with conn.cursor() as cursor:
        cursor.execute(query_1)
        result = cursor.fetchall()
    return result
