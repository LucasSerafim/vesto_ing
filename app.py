import boto3
import requests
import pandas as pd
import datetime
from io import StringIO, BytesIO


def make_kaye_request(url, params=None, headers=None):

    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()  # Raise an error for bad responses
        return response

    except requests.RequestException as e:
        print(f"An error occurred: {e}")
        return None


def fetch_results():
    quotes_list = []

    for i in range(20):  # Fetch 5 pages of quotes
        data = make_kaye_request(f"https://api.kanye.rest").json()
        data["ingestion_datetime"] = datetime.datetime.now().isoformat()
        quotes_list.append(data)

    return pd.DataFrame(quotes_list)


def upload_file_to_s3(df, s3_bucket="s3-orq-t", format="csv"):
    """
    Faz o upload de um arquivo local para um bucket S3.

    Args:
        file_path (str): O caminho completo do arquivo local.
        bucket_name (str): O nome do bucket S3.
        s3_file_name (str): O nome desejado para o arquivo no S3.
    """
    date = datetime.datetime.now()
    if format == "parquet":
        out_path = "file.parquet"
        df.to_parquet(out_path, index=False)
        s3_path = f"landing/{date.strftime("%Y-%m-%d")}/dataframe_test_{date}.parquet"

    elif format == "csv":
        out_path = "file.csv"
        df.to_csv(out_path, index=False, encoding="utf-8")
        s3_path = f"landing/{date.strftime("%Y-%m-%d")}/dataframe_test_{date}.csv"

    s3_bucket = "s3-orq-t"

    s3_client = boto3.client("s3")
    try:
        s3_client.upload_file(out_path, s3_bucket, s3_path)
        print(f"Arquivo {out_path} enviado para s3://{s3_bucket}/{s3_path}")
    except Exception as e:
        print(f"Erro: {e}")


df = fetch_results()
upload_file_to_s3(df)
