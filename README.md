# Data Ingestion SQL Server → AWS S3

Este projeto realiza a **ingestão diária de dados do SQL Server para o Amazon S3**, organizando os arquivos em formato **Parquet** ou **CSV**, com **particionamento por ano/mês/dia**.  

---

## Funcionalidades
- Conexão com banco **SQL Server** via ODBC.  
- Descoberta automática das tabelas que possuem registros.  
- Extração dos dados em **pandas DataFrame**.  
- Salvamento em **CSV** ou **Parquet**.  
- Upload para **Amazon S3**, com organização em:  s3://SEU_BUCKET/bronze/vesto/TABELA/year=AAAA/month=MM/day=DD/TABELA_TIMESTAMP.parquet



- Evita sobrescrita de arquivos (usa timestamp no nome).  
- Ideal para rodar 1x por dia (via Airflow, Cron ou Lambda).  

---

##  Estrutura do Projeto

- main.py → Script principal: orquestra a ingestão (ETL)
- table_reader.py → Lista tabelas do SQL Server com registros
- requirements.txt → Dependências do projeto
- .env → Variáveis de ambiente (NÃO versionar no GitHub!)
- src/
  - data/
    - file.csv
    - file.parquet




---

##  Requisitos
- Python 3.9+  
- ODBC Driver 18 for SQL Server  
- Credenciais válidas do **SQL Server** e da **AWS**  

---

##  Instalação
1. Clone este repositório:
   ```bash
   git clone https://github.com/LucasSerafim/vesto_ing.git
   cd sqlserver-to-s3

2. Crie e ative um ambiente virtual:
    python -m venv venv
    source venv/bin/activate   # Linux/Mac
    venv\Scripts\activate      # Windows
3. Instale as dependências:
    pip install -r requirements.txt


## Configuração
    No arquivo .env, substitua os valores das variáveis de acordo com suas informações pessoais de Login e Tokens.

    # SQL Server
    MSSQL_HOST=seu_host_sql
    MSSQL_PORT=1433
    MSSQL_DATABASE=seu_database
    MSSQL_USER=seu_usuario
    MSSQL_PASSWORD=sua_senha

    # AWS
    aws_access_key_id=SEU_ACCESS_KEY
    aws_secret_access_key=SUA_SECRET_KEY
    aws_session_token=SEU_SESSION_TOKEN   # opcional, se usar STS
    region_name=us-east-1


## Como Executar?
    bash:
    python main.py

    Saída esperada:
    Iniciando o processo de ingestão...
    Extraindo dados da tabela: Clientes...
    Arquivo src\data\file.parquet enviado para s3://SEU_BUCKET/bronze/vesto/Clientes/year=2025/month=09/day=29/Clientes_20250929_182045.parquet
    Processo de ingestão concluído.






- Autor: Lucas Serafim
- Executar 1x por semana (sugestão via agendamento).


