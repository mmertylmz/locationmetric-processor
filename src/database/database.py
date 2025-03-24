import pyodbc
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from ..configurations.config import DB_CONFIG

def get_connection_string():
    return f"DRIVER={{{DB_CONFIG['driver']}}};SERVER={DB_CONFIG['server']};DATABASE={DB_CONFIG['database']};UID={DB_CONFIG['username']};PWD={DB_CONFIG['password']};"
    

def create_connection():
    conn_str = get_connection_string()
    try:
        conn = pyodbc.connect(conn_str)
        return conn
    except Exception as e:
        logging.error(f"Connection Error: {e}")
        return None

def get_engine():
    conn_str = get_connection_string()
    connection_url = f"mssql+pyodbc:///?odbc_connect={conn_str}"

    return create_engine(
        connection_url,
        echo = False,    # True, for logging SQL Commands
        pool_pre_ping = True,
        pool_recycle = True,
        use_setinputsizes=False #OleDB Error (pyodbc.Error) ('HY104', '[HY104] [Microsoft][ODBC SQL Server Driver]Invalid precision value (0) (SQLBindParameter)')
    )

def get_session():
    engine = get_engine()
    Session = sessionmaker(bind = engine)
    return Session()