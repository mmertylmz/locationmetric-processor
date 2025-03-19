import pyodbc
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from config import DB_CONFIG

def get_connection_string(database_name="WR"):
    if DB_CONFIG.get("trusted_connection").lower() == "yes":
        return f"DRIVER={{{DB_CONFIG['driver']}}};SERVER={DB_CONFIG['server']};DATABASE={DB_CONFIG['database']};Trusted_Connection=yes;"

    return f"DRIVER={{{DB_CONFIG('driver')}}};SERVER={DB_CONFIG['server']};DATABASE={DB_CONFIG['database']};UID={DB_CONFIG['username']};PWD={DB_CONFIG['password']};"

def create_connection():
    conn_str = get_connection_string("WR")
    try:
        conn = pyodbc.connect(conn_str)
        return conn
    except Exception as e:
        logging.error(f"Connection Error: {e}")
        return None

def get_engine():
    conn_str = get_connection_string("WR")
    connection_url = f"mssql+pyodbc:///?odbc_connect={conn_str}"

    return create_engine(
        connection_url,
        echo = False,    # True, for logging SQL Commands
        pool_pre_ping = True,
        pool_recycle = True
    )

def get_session():
    engine = get_engine()
    Session = sessionmaker(bind = engine)
    return Session()