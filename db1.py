import psycopg2
from config import DB1_CONFIG

def get_db1_connection():
    conn = psycopg2.connect(**DB1_CONFIG)
    return conn
