import boto3
import requests
import pandas as pd
import datetime
import pyodbc
from dotenv import load_dotenv
import os
from table_reader import get_table_names


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
        f"SERVER={host},{port};"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password};"
        "TrustServerCertificate=yes;"
        "Trusted_Connection=No;"
    )

    query = f"SELECT TOP 10 * FROM dbo.{table_name}"
    df = pd.read_sql(query, conn)
    conn.close()
    return df


def upload_file_to_s3(df, s3_bucket="s3-orq-t", format="csv", table_name="empty_table"):
    """
    Faz o upload de um DataFrame para um bucket S3 com particionamento por ano/mês/dia.
    """
    date = datetime.datetime.now()

    # Particionamento estilo Data Lake
    partition_path = f"year={date.year}/month={date.month:02d}/day={date.day:02d}"

    # Nome do arquivo com timestamp para evitar sobrescrita
    file_timestamp = date.strftime("%Y%m%d_%H%M%S")

    if format == "parquet":
        out_path = "file.parquet"
        df.to_parquet(out_path, index=False)
        s3_path = f"bronze/{table_name}/{partition_path}/{table_name}_{file_timestamp}.parquet"

    elif format == "csv":
        out_path = "file.csv"
        df.to_csv(out_path, sep=";", index=False, encoding="utf-8")
        s3_path = (
            f"bronze/{table_name}/{partition_path}/{table_name}_{file_timestamp}.csv"
        )

    else:
        raise ValueError("Formato não suportado. Use 'csv' ou 'parquet'.")

    s3_client = boto3.client("s3")

    try:
        s3_client.upload_file(out_path, s3_bucket, s3_path)
        print(f"✅ Arquivo {out_path} enviado para s3://{s3_bucket}/{s3_path}")
    except Exception as e:
        print(f"❌ Erro ao enviar para o S3: {e}")


# Carregando variáveis de ambiente
HOST = os.getenv("MSSQL_HOST")
PORT = os.getenv("MSSQL_PORT")
DATABASE = os.getenv("MSSQL_DATABASE")
USER = os.getenv("MSSQL_USER")
PASSWORD = os.getenv("MSSQL_PASSWORD")

table_names = get_table_names(HOST, PORT, DATABASE, USER, PASSWORD)

for table in table_names:

    # Extraindo dados
    df = get_sql_server_data(
        host=HOST,
        port=PORT,
        database=DATABASE,
        user=USER,
        password=PASSWORD,
        table_name=table,
    )

    # Enviando para o S3
    upload_file_to_s3(df, table_name=table, format="csv")
