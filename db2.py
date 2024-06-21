# database/db2.py

import psycopg2
from config import DB2_CONFIG

def get_db2_connection():
    conn = psycopg2.connect(**DB2_CONFIG)
    return conn
