# queries/queries_db2.py

def fetch_data_from_db2(conn):
    query = """
    SELECT * FROM schema2.table2
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()
    return result
