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

    query = f"""SELECT 
    t.name AS name,
            SUM(p.rows) AS QtdLinhas
        FROM sys.tables t
        JOIN sys.partitions p 
            ON t.object_id = p.object_id
        WHERE p.index_id IN (0,1)   -- 0 = heap, 1 = clustered index
        GROUP BY t.name
        HAVING SUM(p.rows) > 0
        ORDER BY QtdLinhas DESC;"""
    df = pd.read_sql(query, conn)
    conn.close()
    return sorted(df["name"].tolist())
