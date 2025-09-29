import boto3
import requests
import pandas as pd
import datetime
import pyodbc
from dotenv import load_dotenv
import os
from table_reader import get_table_names


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

    query = f"SELECT * FROM dbo.{table_name}"
    print(f"Extraindo dados da tabela: {table_name}... \n")
    try:
        df = pd.read_sql(query, conn)
    except Exception as e:
        print(f"Erro ao extrair dados da tabela {table_name}: {e}")
        df = pd.DataFrame()  # Retorna um DataFrame vazio em caso de erro
        pass
    conn.close()
    return df


def upload_file_to_s3(
    df,
    s3_bucket="dev-analytics-datamigration-bucket",
    format="csv",
    table_name="empty_table",
):
    """
    Faz o upload de um DataFrame para um bucket S3 com particionamento por ano/mês/dia.
    """
    date = datetime.datetime.now()

    # Partition path based on current date
    partition_path = f"year={date.year}/month={date.month:02d}/day={date.day:02d}"

    # File timestamp for uniqueness
    file_timestamp = date.strftime("%Y%m%d_%H%M%S")

    if format == "parquet":
        try:
            out_path = "src\\data\\file.parquet"
            df.to_parquet(out_path, index=False)
            s3_path = f"bronze/vesto/{table_name}/{partition_path}/{table_name}_{file_timestamp}.parquet"
        except Exception as e:
            print(f"Erro ao salvar Parquet: {e}")
            pass

    elif format == "csv":
        out_path = "src\\data\\file.csv"
        try:
            df.to_csv(out_path, sep=";", index=False, encoding="utf-8")
            s3_path = f"bronze/vesto/{table_name}/{partition_path}/{table_name}_{file_timestamp}.csv"
        except Exception as e:
            print(f"Erro ao salvar CSV: {e}")
            pass

    else:
        raise ValueError("Formato não suportado. Use 'csv' ou 'parquet'.")

    session = boto3.Session(
        aws_access_key_id=os.getenv("aws_access_key_id"),
        aws_secret_access_key=os.getenv("aws_secret_access_key"),
        aws_session_token=os.getenv("aws_session_token"),
        region_name=os.getenv("region_name"),
    )
    s3_client = session.client("s3")

    try:
        s3_client.upload_file(out_path, s3_bucket, s3_path)
        print(f"Arquivo {out_path} enviado para s3://{s3_bucket}/{s3_path}")
    except Exception as e:
        print(f"Erro ao enviar para o S3: {e}")
        pass


if __name__ == "__main__":
    print("Iniciando o processo de ingestão...")

    # Loading environment variables
    load_dotenv()

    HOST = os.getenv("MSSQL_HOST")
    PORT = os.getenv("MSSQL_PORT")
    DATABASE = os.getenv("MSSQL_DATABASE")
    USER = os.getenv("MSSQL_USER")
    PASSWORD = os.getenv("MSSQL_PASSWORD")

    table_names = get_table_names(HOST, PORT, DATABASE, USER, PASSWORD)

    for table in table_names:

        # Data extraction

        df = get_sql_server_data(
            host=HOST,
            port=PORT,
            database=DATABASE,
            user=USER,
            password=PASSWORD,
            table_name=table,
        )
        # S3 upload - CSV or Parquet format
        upload_file_to_s3(df, table_name=table, format="parquet")

    print("Processo de ingestão concluído.")
