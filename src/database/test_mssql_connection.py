import pyodbc

def test_mssql_connection():
    server = r"DESKTOP-RHIE06K\SQLEXPRESS"
    database = "WR"

    connection_string = f"DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;"

    try:
        conn = pyodbc.connect(connection_string)

        cursor = conn.cursor()
        cursor.execute("SELECT @@VERSION")
        version = cursor.fetchone()[0]

        print("Databases:")
        cursor.execute("SELECT name FROM sys.databases")
        databases = cursor.fetchall()
        for db in databases:
            print(f" - {db[0]}")

        print(f"Version: {version}")
        conn.close()
        return True

    except Exception as e:
        print(f"Connection Error: {e}")
        return False


if __name__ == "__main__":
    print("MSSQL Connection Testing..")
    test_mssql_connection()

