import boto3
import requests
import pandas as pd
import datetime
import pyodbc
from dotenv import load_dotenv
import os

load_dotenv()


def get_sql_server_data(host, port, database, user, password, table_name):
    """
    Compila os dados de uma tabela do SQL Server e retorna um pandas DataFrame.

    Args:
        host (str): endereço do banco.
        port (int): porta do SQL Server.
        database (str): database.
        user (str): username para login.
        password (str): senha para login.

    Returns:
        pd.DataFrame: DataFrame contendo os dados compilados.
    """

    conn = pyodbc.connect(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=200.221.173.235,2018;"
        "DATABASE=uniquechic;"
        "UID=uniquechic;"
        "PWD=8j%07*1T3y;"
        "TrustServerCertificate=yes;"
        "Trusted_Connection=No;"
    )

    query = f"SELECT * FROM dbo.{table_name}"
    df = pd.read_sql(query, conn)
    conn.close()
    return df


def upload_file_to_s3(df, s3_bucket="s3-orq-t", format="csv", table_name="empty_table"):
    """
    Faz o upload de um arquivo local para um bucket S3.

    Args:
        df (pd.DataFrame): Dataframe que armazena os dados de retorno da API.
        s3_bucket (str): O nome do bucket S3.
        format (str): formato do arquivo final (Como padrão, utiliza-se o .csv).
    """
    date = datetime.datetime.now()
    if format == "parquet":
        out_path = "file.parquet"
        df.to_parquet(out_path, index=False)
        s3_path = f"landing/{table_name}/{date.strftime("%Y-%m-%d")}/{table_name}{date}.parquet"

    elif format == "csv":
        out_path = "file.csv"
        df.to_csv(out_path, index=False, encoding="utf-8")
        s3_path = (
            f"landing/{table_name}/{date.strftime("%Y-%m-%d")}/{table_name}_{date}.csv"
        )

    s3_bucket = "s3-orq-t"

    s3_client = boto3.client("s3")
    try:
        s3_client.upload_file(out_path, s3_bucket, s3_path)
        print(f"Arquivo {out_path} enviado para s3://{s3_bucket}/{s3_path}")
    except Exception as e:
        print(f"Erro: {e}")


HOST = os.getenv("MSSQL_HOST")
PORT = os.getenv("MSSQL_PORT")
DATABASE = os.getenv("MSSQL_DATABASE")
USER = os.getenv("MSSQL_USER")
PASSWORD = os.getenv("MSSQL_PASSWORD")


df = get_sql_server_data(
    host=HOST,
    port=PORT,
    database=DATABASE,
    user=USER,
    password=PASSWORD,
    table_name="LEITURAOTICAIT",
)


upload_file_to_s3(df, table_name="LEITURAOTICAIT", format="parquet")
