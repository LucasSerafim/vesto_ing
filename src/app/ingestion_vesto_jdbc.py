import sys
import datetime
import boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import DataFrame

# ========================
# Função: Casting de colunas
# ========================


def get_column_list_with_cast(spark, jdbcUrl, user, password, table_name):
    """Obtém lista de colunas e converte sql_variant para VARCHAR"""
    query = f"""(
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{table_name}'
        AND TABLE_SCHEMA = 'dbo'
    ) AS cols"""

    cols_df = (
        spark.read.format("jdbc")
        .option("url", jdbcUrl)
        .option("dbtable", query)
        .option("user", user)
        .option("password", password)
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .load()
    )

    columns = []
    for row in cols_df.collect():
        col_name = row["COLUMN_NAME"]
        data_type = row["DATA_TYPE"]

        if data_type == "sql_variant":
            columns.append(f"CAST([{col_name}] AS VARCHAR(MAX)) AS [{col_name}]")
        else:
            columns.append(f"[{col_name}]")

    return ", ".join(columns)


# ========================
# Função: Upload para S3
# ========================
def save_to_s3(df: DataFrame, table_name: str, s3_bucket: str):
    date = datetime.datetime.now()
    partition_path = f"year={date.year}/month={date.month:02d}/day={date.day:02d}"
    file_timestamp = date.strftime("%Y%m%d_%H%M%S")

    s3_path = f"s3://{s3_bucket}/bronze/vesto/{table_name}/{partition_path}/"

    (df.write.mode("overwrite").parquet(s3_path))  # ou "append" se preferir

    print(f"Tabela {table_name} salva em {s3_path}")
    return s3_path


# ========================
# MAIN
# ========================
def main():
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "HOST",
            "PORT",
            "DATABASE",
            "USER",
            "PASSWORD",
            "S3_BUCKET",
            "GLUE_DATABASE",
            "GLUE_ROLE",
        ],
    )

    print(args)

    HOST = args["HOST"]
    PORT = args["PORT"]
    DATABASE = args["DATABASE"]
    USER = args["USER"]
    PASSWORD = args["PASSWORD"]
    s3_bucket = args["S3_BUCKET"]
    glue_database = args["GLUE_DATABASE"]
    glue_role = args["GLUE_ROLE"]

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    # Adicione estas configurações:
    spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY")
    spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "LEGACY")

    jdbcUrl = f"jdbc:sqlserver://{HOST}:{PORT};databaseName={DATABASE}"

    # Pegar lista de tabelas do SQL Server via JDBC

    query = (
        query
    ) = """(SELECT t.name AS name,SUM(p.rows) AS QtdLinhas
           FROM sys.tables t
           JOIN sys.partitions p ON t.object_id = p.object_id
           WHERE p.index_id IN (0,1)
           GROUP BY t.name
           HAVING SUM(p.rows) > 0) AS table_list"""

    tables_df = (
        spark.read.format("jdbc")
        .option("url", jdbcUrl)
        .option("dbtable", query)
        .option("user", USER)
        .option("password", PASSWORD)
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .load()
    )

    table_list = [row["name"] for row in tables_df.collect()]

    print(f"Tabelas encontradas: {table_list}")

    # Loop para cada tabela -> extrair dados -> salvar no S3 -> rodar crawler
    for table in table_list:
        print(f"Extraindo dados da tabela {table}...")

        # Query para converter sql_variant para varchar
        query = f"""(
            SELECT {get_column_list_with_cast(spark, jdbcUrl, USER, PASSWORD, table)}
            FROM dbo.{table}
        ) AS query"""

        df = (
            spark.read.format("jdbc")
            .option("url", jdbcUrl)
            .option("dbtable", query)
            .option("user", USER)
            .option("password", PASSWORD)
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
            .load()
        )

        if df.count() > 0:
            s3_path = save_to_s3(df, table, s3_bucket)

        else:
            print(f"Tabela {table} está vazia, ignorando.")

    print("Ingestão concluída.")


if __name__ == "__main__":
    main()
