# Documentação Técnica - Job AWS Glue para Ingestão SQL Server

## Visão Geral
Segundo a necessidade de armazenar dados históricos da Uniquechic, o projeto busca abastecer um datalake com os dados do sistema Vesto.

O fluxo de dados realiza a orquestração da ingestão de tabelas de um servidor SQL Server, em formato *.parquet* para a camada bronze de um bucket s3.

Todo o fluxo é executado via AWS Glue Jobs, com agendamento semanal realizado nas segundas-feiras. 

O tempo de execução é de cerca de 1 hora e 30 minutos, ou seja, toda segunda-feira, o fluxo cria uma réplica da posição de cada tabela populada do sistema.


Um exemplo prático pode ser encontrado importando uma tabela arbitrária. Vamos utilizar a tabela dbo.ATIVIDADE.
Após a identificação dessa tabela, o script realiza um full scan, armazenando o resultado em um arquivo .parquet.
Se o processo foi executado, por exemplo, no dia 28/09/2025, o arquivo poderá ser encontrado no seguinte caminho:


`s3://dev-analytics-datamigration-bucket/bronze/vesto/ATIVIDADE/year=2025/month=09/day=28/part-*.parquet`



## Arquitetura

```
SQL Server (Vesto) → vesto_ingestion_job (AWS Glue job) →  dev-analytics-datamigration-bucket (S3)
```

### Camadas de Dados

- **Bronze**: Camada de dados brutos no formato Parquet
- **Particionamento**: `year=YYYY/month=MM/day=DD`

## Dependências

### Bibliotecas Python
- `sys`: Manipulação de argumentos de sistema
- `datetime`: Geração de timestamps e partições temporais
- `boto3`: SDK da AWS para interação com serviços
- `awsglue.utils`: Utilitários do Glue (getResolvedOptions)
- `awsglue.context`: Contexto do Glue
- `pyspark`: Framework de processamento distribuído

### Drivers JDBC

O código utiliza o driver mencionado abaixo para realizar a conexão jdbc com o banco de dados.

- `com.microsoft.sqlserver.jdbc.SQLServerDriver`: Driver JDBC do SQL Server

## Parâmetros de Entrada

O job recebe os seguintes parâmetros via `getResolvedOptions`:

| Parâmetro | Descrição | Exemplo |
|-----------|-----------|---------|
| `JOB_NAME` | Nome do job Glue | `vesto_ingestion_job` |
| `HOST` | Hostname do SQL Server | `<endereço_vesto>` |
| `PORT` | Porta do SQL Server | `<porta_vesto>` |
| `DATABASE` | Nome do banco de dados | `uniquechic` |
| `USER` | Usuário de conexão | `<usuário Vesto>` |
| `PASSWORD` | Senha de conexão | `***` |
| `S3_BUCKET` | Bucket S3 de destino | `dev-analytics-datamigration-bucket` |
| `GLUE_DATABASE` | Database no Glue Catalog | `vesto_database` |
| `GLUE_ROLE` | IAM Role para o Crawler | `arn:aws:iam::195478975290:role/AWSGlueServiceRole-Poc-Datalake` |

## Funções Principais

### 1. `get_column_list_with_cast()`

**Propósito**: Obtém metadados das colunas da tabela e realiza casting de tipos incompatíveis.

**Parâmetros**:
- `spark`: Sessão Spark
- `jdbcUrl`: URL de conexão JDBC
- `user`: Usuário do banco
- `password`: Senha do banco
- `table_name`: Nome da tabela

**Comportamento**:
- Consulta `INFORMATION_SCHEMA.COLUMNS` para obter colunas e tipos
- Converte colunas `sql_variant` para `VARCHAR(MAX)` (tipo não suportado pelo Spark)
- Retorna string formatada para uso em query SQL

**Exemplo de retorno**:
```sql
[coluna1], CAST([coluna_variant] AS VARCHAR(MAX)) AS [coluna_variant], [coluna3]
```

### 2. `save_to_s3()`

**Propósito**: Salva DataFrame no S3 em formato Parquet com particionamento temporal.

**Parâmetros**:
- `df`: DataFrame Spark
- `table_name`: Nome da tabela
- `s3_bucket`: Bucket S3 de destino

**Comportamento**:
- Gera partição baseada na data atual (`year=YYYY/month=MM/day=DD`)
- Cria timestamp para rastreabilidade
- Salva dados em modo `overwrite` no path: `s3://{bucket}/bronze/vesto/{table}/{partition}/`
- Retorna o path S3 completo

**Estrutura do Path**:
```
s3://bucket/bronze/vesto/tabela_exemplo/year=2025/month=10/day=03/
```


## Fluxo de Execução (main)

### 1. Inicialização

```python
# Leitura de parâmetros
args = getResolvedOptions(sys.argv, [...])

# Criação de contextos Spark
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
```

### 2. Configurações Spark

Aplicação de configurações para compatibilidade de datetime em Parquet:

```python
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "LEGACY")
```

**Propósito**: Evita problemas de conversão de datas entre diferentes calendários (Juliano/Gregoriano).

### 3. Descoberta de Tabelas

Query SQL para listar tabelas com dados:

```sql
SELECT t.name AS name, SUM(p.rows) AS QtdLinhas
FROM sys.tables t
JOIN sys.partitions p ON t.object_id = p.object_id
WHERE p.index_id IN (0,1)
GROUP BY t.name
HAVING SUM(p.rows) > 0
```

**Critério**: Apenas tabelas com `QtdLinhas > 0`

### 4. Processamento por Tabela

Para cada tabela encontrada:

1. **Extração**:
   - Monta query com casting de `sql_variant`
   - Lê dados via JDBC do SQL Server
   
2. **Validação**:
   - Verifica se `df.count() > 0`
   - Ignora tabelas vazias

3. **Persistência**:
   - Salva DataFrame em S3 (formato Parquet)
   
4. **Catalogação** (comentado):
   - Criaria/executaria crawler para atualizar catálogo

## Tratamento de Tipos de Dados

### Problema: `sql_variant`

O tipo `sql_variant` do SQL Server não possui equivalente direto no Spark/Parquet.

**Solução implementada**:
- Detecção via `INFORMATION_SCHEMA.COLUMNS`
- Conversão para `VARCHAR(MAX)` no momento da extração
- Preservação do nome original da coluna

## Formato de Saída

### Estrutura de Diretórios

```
s3://bucket/
└── bronze/
    └── vesto/
        └── {table_name}/
            └── year={YYYY}/
                └── month={MM}/
                    └── day={DD}/
                        └── part-*.parquet
```

### Formato de Arquivo

- **Tipo**: Apache Parquet
- **Compressão**: Padrão Snappy (configurável)
- **Modo de escrita**: Overwrite (substitui partição diária)

## Considerações de Performance

- **Agendamento**: O fluxo de dados será executado todas as *segundas-feiras* às 9h00 BRT (12:00 GTM)

### Otimizações Implementadas

1. **Leitura JDBC paralela**: Spark distribui leitura entre executors
2. **Formato Parquet**: Compressão eficiente e leitura colunar
3. **Particionamento temporal**: Facilita queries com filtros de data


## Monitoramento

### Logs do Glue

Verificar em CloudWatch Logs:
- `/aws-glue/jobs/output`
- `/aws-glue/jobs/error`

### Métricas Importantes

- Tempo de execução por tabela
- Quantidade de linhas processadas
- Tamanho dos arquivos Parquet gerados
- Taxa de sucesso/falha

## Permissões IAM Necessárias

### Para o Job Glue

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::bucket-name/*",
        "arn:aws:s3:::bucket-name"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "glue:CreateCrawler",
        "glue:UpdateCrawler",
        "glue:StartCrawler",
        "glue:GetCrawler"
      ],
      "Resource": "*"
    }
  ]
}
```


## Troubleshooting

### Erro: "JDBC Driver not found"

**Solução**: Verificar se o JAR do SQL Server JDBC está configurado no job Glue.

### Erro: "sql_variant not supported"

**Solução**: Já tratado pela função `get_column_list_with_cast()`. Verificar se a função está sendo chamada corretamente.

### Erro: "Permission denied" no S3

**Solução**: Verificar IAM Role do job Glue e políticas do bucket S3.




## Referências

- [AWS Glue Documentation](https://docs.aws.amazon.com/glue/)
- [Apache Spark SQL Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [SQL Server JDBC Driver](https://docs.microsoft.com/en-us/sql/connect/jdbc/)
- [Apache Parquet Format](https://parquet.apache.org/documentation/latest/)


## Autor


- Lucas Serafim | lucas-serafim@hotmail.com | https://github.com/LucasSerafim