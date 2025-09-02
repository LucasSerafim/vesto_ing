import pandas as pd
import pyodbc
from dotenv import load_dotenv


def get_table_names(host, port, database, user, password):
    conn = pyodbc.connect(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={host},{port};"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password};"
        "TrustServerCertificate=yes;"
        "Trusted_Connection=No;"
    )

    query = f"SELECT name FROM sys.tables"
    df = pd.read_sql(query, conn)
    conn.close()
    return df["name"].tolist()
