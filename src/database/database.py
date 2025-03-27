import pyodbc
import logging
import time
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError
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
        pool_recycle=300,
        pool_size=10,      
        max_overflow=20,   
        use_setinputsizes=False #OleDB Error (pyodbc.Error) ('HY104', '[HY104] [Microsoft][ODBC SQL Server Driver]Invalid precision value (0) (SQLBindParameter)')
    )

def retry_on_deadlock(func):
    def wrapper(*args, **kwargs):
        max_retries = 3
        delay = 2  # seconds
        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except OperationalError as e:
                if "deadlock" in str(e).lower():
                    logging.warning(f"Deadlock detected. Retrying {attempt + 1}/{max_retries}...")
                    time.sleep(delay)
                else:
                    raise
        logging.error("Max retries reached. Operation failed due to deadlock.")
        raise
    return wrapper

@retry_on_deadlock
def execute_with_retry(session, operation):
    return operation(session)

def get_session():
    engine = get_engine()
    Session = sessionmaker(bind = engine)
    return Session()