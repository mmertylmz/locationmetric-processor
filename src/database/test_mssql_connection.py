import pyodbc
from config import DB_CONFIG

def get_connection_string(database_name="WR"):
    if DB_CONFIG.get("trusted_connection").lower() == "yes":
        return f"DRIVER={{{DB_CONFIG['driver']}}};SERVER={DB_CONFIG['server']};DATABASE={DB_CONFIG['database']};Trusted_Connection=yes;"

    return f"DRIVER={{{DB_CONFIG('driver')}}};SERVER={DB_CONFIG['server']};DATABASE={DB_CONFIG['database']};UID={DB_CONFIG['username']};PWD={DB_CONFIG['password']};"

def test_mssql_connection():
    connection_string = get_connection_string("WR")

    try:
        conn = pyodbc.connect(connection_string)

        #cursor = conn.cursor()
        #cursor.execute("SELECT @@VERSION")
        #version = cursor.fetchone()[0]

        #print("Databases:")
        #cursor.execute("SELECT name FROM sys.databases")
        #databases = cursor.fetchall()
        #for db in databases:
            #print(f" - {db[0]}")

        #print(f"Version: {version}")

        print("Connection successful!")

        conn.close()
        return True

    except Exception as e:
        print(f"Connection Error: {e}")
        return False


if __name__ == "__main__":
    print("MSSQL Connection Testing..")
    test_mssql_connection()

