# 📊 Data Pipeline - Receita Federal de Empresas

> 🇧🇷 Leia em [Português](#-data-pipeline---receita-federal-de-empresas-1)  
> 🇺🇸 Read in [English](#-data-pipeline---brazilian-federal-revenue-corporate-registry)

---

# 🇧🇷 Data Pipeline - Receita Federal de Empresas

Pipeline de dados para processamento de informações cadastrais de empresas brasileiras da Receita Federal.

## 🏢 Sobre os Dados CNPJ
Os dados utilizados neste projeto são os Dados Abertos do CNPJ disponibilizados pela Receita Federal do Brasil, que contêm informações cadastrais completas sobre empresas e estabelecimentos brasileiros.

### 📋 Principais Características dos Dados
Fonte Oficial:
- 📍 https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj
- 📍 https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/consultas/dados-publicos-cnpj

#### Atualização:

- 🔄 Os dados são atualizados mensalmente

#### Estrutura dos Arquivos:

- 📦 Arquivos no formato ZIP contendo CSV
- 🗂️ Separados por tipo de informação (Empresas, Estabelecimentos, Sócios)
- 📊 Dados em formato delimitado por ponto e vírgula
- 🇧🇷 Codificação Latin-1 (ISO-8859-1)

## 📁 Tipos de Dados Disponíveis
### 1. Dados de Empresas
``` sh
{
    "cnpj": "00000191",                     # CNPJ
    "razao_social": "EMPRESA BRASILEIRA",
    "natureza_juridica": 2051,              # Código da natureza jurídica
    "qualificacao_responsavel": 10,         # Qualificação do responsável
    "capital_social": "1000000,00",         # Capital social da empresa
    "porte_empresa": "05",                  # Porte da empresa (00, 01, 03, 05)
    "ente_federativo": ""                   # Ente federativo responsável
}
```

### 2. Dados de Sócios
``` sh
{
    "cnpj": "00000191",                     # CNPJ da empresa
    "tipo_socio": 1,                        # 1=PJ, 2=PF, 3=Estrangeiro
    "nome_socio": "JOÃO DA SILVA",
    "documento_socio": "***999999**",       # CPF ou CNPJ do sócio
    "codigo_qualificacao": 10,              # Código de qualificação
    "data_entrada_sociedade": "20200101"    # Data de entrada
}
```

### Classificação de Porte:
``` python
PORTE_EMPRESA = {
    "00": "Não informado",
    "01": "Microempresa",
    "03": "Empresa de Pequeno Porte",
    "05": "Demais empresas"
}
```

## 📋 Funcionalidades

- Ingestão de dados dos arquivos ZIP da Receita Federal
- Processamento em camadas (Bronze → Silver → Gold)
- Validação de dados com Pandera
- Monitoramento de qualidade com métricas em PostgreSQL
- Dashboards no Apache Superset
- Orquestração com Apache Airflow


## 🛠️ Tecnologias
- Python 3.13 - Processamento de dados
- Pandas - Manipulação de dados
- Airflow - Orquestração de pipelines
- Pandera - Validação de dados
- PostgreSQL - Armazenamento de dados e métricas
- Superset - Visualização de dados
- Docker - Containerização

## 🏗️ Arquitetura

![alt text](https://github.com/patriciatozi/receita-federal-empresas/blob/main/documentation/src/Arquitetura_Receita_Federal.png)


## 📁 Estrutura do Projeto

``` sh
receita-federal-empresas/
├── dags/
│   └── receita_federal_job.py    # DAG principal do Airflow
├── scripts/
│   ├── data_ingestion/           # Ingestão de dados brutos
│   │   ├── companies.py
│   │   └── partners.py
│   ├── data_processing/          # Processamento das camadas
│   │   ├── companies.py
│   │   └── partners.py
│   ├── data_refinement/          # Camada gold
│   │   └── companies_detail.py
│   ├── data_quality/             # Validações com Pandera  
│   │   ├── bronze_validation.py
│   │   ├── silver_validation.py
│   │   ├── gold_validation.py
│   └──tests                      # Testes Unitários
│       ├── conftest.py
│       ├── test_data_quality.py
│       └── companies_detail.py
├── docker-compose.yml            # Orquestração de containers
├── Dockerfile                    # Imagem customizada
├── requirements.txt              # Dependências Python
└── .env                          # Variáveis de ambiente
```
## 📊 Camadas de Dados

### 🥉 Bronze Layer (Raw)
- Função:
    - Dados brutos ingeridos da Receita Federal
    - Formato original preservado
    - Validações básicas de formato
- Destinos:
    - PostgreSQL: `bronze_companies`, `bronze_partners`
    - Parquet: `/data/bronze/` (backup)

#### Tabelas PostgreSQL
##### 1) `bronze_companies`
Coluna | Tipo | Descrição 
--- | --- | --- 
cnpj | TEXT | Cadastro Nacional da Pessoa Jurídica 
razao_social | TEXT | Nome empresarial
natureza_juridica | INTEGER | Código de natureza jurídica 
qualificacao_responsavel | INTEGER | Qualificação da pessoa responsável pela empresa
capital_social | TEXT | Capital social da empresa
cod_porte | TEXT | Código do porte da empresa
last_update | TEXT | Última atualização mensal dos dados origem

##### 2) `bronze_partners`
Coluna | Tipo | Descrição 
--- | --- | --- 
cnpj | TEXT | Cadastro Nacional da Pessoa Jurídica 
tipo_socio | TEXT | Tipo do sócio da empresa
nome_socio | TEXT | Corresponde ao nome do sócio pessoa física, razão social e/ou nome da empresa 
documento_socio | TEXT | CPF ou CNPJ do sócio, sócios estrangeiros são representados por `***999999**`
codigo_qualificacao_socio | TEXT | Capital social da empresa
last_update | TEXT | Última atualização mensal dos dados origem

### 🥈 Silver Layer (Cleaned)
- Função:
    - Dados limpos e tratados
    - Schema validation com Pandera
    - Padronização de formatos
- Destinos:
    - PostgreSQL: `silver_companies`, `silver_partners`
    - Parquet: `/data/silver/` (backup)

#### Tabelas PostgreSQL
##### 1) `silver_companies`
Coluna | Tipo | Descrição 
--- | --- | --- 
cnpj | TEXT | Cadastro Nacional da Pessoa Jurídica 
razao_social | TEXT | Nome empresarial
natureza_juridica | INTEGER | Código de natureza jurídica 
qualificacao_responsavel | INTEGER | Qualificação da pessoa responsável pela empresa
capital_social | FLOAT | Capital social da empresa
cod_porte | TEXT | Código do porte da empresa
last_update | DATE | Última atualização de processamento dos dados

##### 2) `silver_partners`
Coluna | Tipo | Descrição 
--- | --- | --- 
cnpj | TEXT | Cadastro Nacional da Pessoa Jurídica 
tipo_socio | TEXT | Tipo do sócio da empresa
nome_socio | TEXT | Corresponde ao nome do sócio pessoa física, razão social e/ou nome da empresa 
documento_socio | TEXT | CPF ou CNPJ do sócio, sócios estrangeiros são representados por `***999999**`
codigo_qualificacao_socio | TEXT | Capital social da empresa
flag_socio_estrangeiro | INTEGER | Indicação de que se trata de um sócio estrangeiro (`1`) ou não (`0`)
last_update | DATE | Última atualização de processamento dos dados

### 🥇 Gold Layer (Business)
- Função:
    - Dados enriquecidos para análise
    - Métricas de negócio
    - Agregações e transformações
- Destinos:
    - PostgreSQL: `gold_companies_detail`
    - Parquet: `/data/gold/` (backup)

#### Tabela PostgreSQL
##### `gold_companies_detail`
Coluna | Tipo | Descrição 
--- | --- | --- 
cnpj | TEXT | Cadastro Nacional da Pessoa Jurídica 
qtde_socios | INTEGER | Número de sócios participantes do CNPJ
flag_socio_estrangeiro | BOOLEAN | Indicação de sócio estrangeiro (`True`) ou não estrangeiro (`False`)
doc_alvo | BOOLEAN | `True` quando porte da empresa = 03 & qtde_socios > 1, `False` para os demais casos
last_update | DATE | Última atualização de processamento dos dados

## ✅ Data Quality Checks

### Validações Implementadas
#### Camada Silver:
- ✅ CNPJ deduplicado
- ✅ Código de porte válido (00, 01, 03, 05)
- ✅ Natureza jurídica dentro da faixa esperada (1011 a 9999)
- ✅ Tipo de Sócio válido (1, 2 ou 3)

#### Camada Gold:
- ✅ CNPJ deduplicado com 14 dígitos
- ✅ Flags booleanas consistentes
- ✅ Regras de negócio aplicadas

### Métricas Monitoradas
``` sql
-- Exemplo de métricas coletadas
SELECT * FROM data_quality_metrics 
WHERE table_name = 'bronze_companies' 
ORDER BY last_update DESC 
LIMITE 10;
```

## 📈 Dashboards no Superset

### Métricas de Qualidade de Dados
#### 1) Quantidade de registros (`total_records`)
- Contabiliza o número total de linhas em cada tabela
- Ajuda a identificar problemas de ingestão ou cargas incompletas
#### 2) Integridade (`null_percentage`)
- Percentual de valores nulos nas tabelas
- Permite monitorar a completude dos dados e identificar colunas críticas com alta ausência de informação
- No dashboard, os dados são divididos em categorias: `Valid` (dados preenchidos) e `Mostly Nulls` (dados ausentes)
#### 3) Unicidade (`dq_duplicate_count`)
- Número de registros duplicados em cada tabela
- Avalia a consistência dos dados, garantindo que chaves ou registros únicos não se repitam

### Visualização na Ferramenta
![alt text](https://github.com/patriciatozi/receita-federal-empresas/blob/main/documentation/src/superset_dq_dashboard.png)

## 🚀 Setup do Ambiente
### Pré-requisitos
- Docker 20.10+
- Docker Compose 2.0+
- 8GB RAM recomendado

### 1. Clone o repositório
``` sh
git clone https://github.com/patriciatozi/receita-federal-empresas.git
cd receita-federal-empresas
```
### 2. Configure as variáveis de ambiente
``` sh
cp .env.example .env
# Edite o .env com suas configurações
```
### 3. Execute os containers
``` sh
# Inicie todos os serviços
docker-compose up -d

# Ou construa as imagens e inicie
docker-compose build
docker-compose up -d
```
### 4. Acesse as interfaces
- Airflow: http://localhost:8080
    - Usuário: airflow
    - Senha: airflow
- Superset: http://localhost:8088
    - Usuário: admin
    - Senha: admin

## 🔧 Comandos Úteis

### Docker Compose
``` sh
# Ver status dos containers
docker-compose ps

# Logs do Airflow
docker-compose logs airflow-scheduler

# Executar comando em container específico
docker-compose exec airflow-scheduler airflow dags list

# Parar todos os serviços
docker-compose down

# Parar e remover volumes
docker-compose down -v
```
### Airflow
``` sh
# Listar DAGs
docker-compose exec airflow-scheduler airflow dags list

# Trigger manual da DAG
docker-compose exec airflow-scheduler airflow dags trigger receita_federal_job

# Ver logs de uma task
docker-compose exec airflow-webserver airflow tasks logs receita_federal_job bronze_companies
```
### Desenvolvimento
``` sh
# Instalar dependências localmente
pip install -r requirements.txt

# Executar script individualmente
python scripts/data_ingestion/companies.py

# Testar validações
python scripts/data_quality/silver_validation.py
```

## 📝 Próximos Passos
### Melhorias Futuras
- Implementar alertas de qualidade
- Tornar dinâmica a leitura de dados da origem, não se restringindo a apenas um arquivo de cada natureza (empresas ou sócios) a partir do último diretório atualizado
- Refatoração para o PySpark e implementação em nuvem:
    - Processamento distribuído mais eficaz
    - Prevenção a Assimetria de Dados (data skew)
    - Implementar particionamento de dados

## 📄 Licença
Este projeto está sob a licença MIT. Veja o arquivo LICENSE para detalhes.

---

# 📊 Data Pipeline - Brazilian Federal Revenue (Corporate Registry)

Data pipeline for processing company registration information from the Brazilian Federal Revenue (Receita Federal).

## 🏢 About the CNPJ Data
The data used in this project are the **CNPJ Open Data** made available by the Brazilian Federal Revenue Service.  
They contain complete registration information about Brazilian companies and business establishments.

### 📋 Main Data Characteristics
Official Source:  
- 📍 https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj  
- 📍 https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/consultas/dados-publicos-cnpj

#### Update Frequency:

- 🔄 Monthly

#### File Structure:

- 📦 ZIP files containing CSVs  
- 🗂️ Separated by type (Companies, Establishments, Partners)  
- 📊 Semicolon-delimited format  
- 🇧🇷 Latin-1 (ISO-8859-1) encoding

## 📁 Available Data Types
### 1. Company Data
``` sh
{
    "cnpj": "00000191",
    "razao_social": "EMPRESA BRASILEIRA",
    "natureza_juridica": 2051,
    "qualificacao_responsavel": 10,
    "capital_social": "1000000,00",
    "porte_empresa": "05",
    "ente_federativo": ""
}
```

### 2. Partner Data
``` sh
{
    "cnpj": "00000191",
    "tipo_socio": 1,
    "nome_socio": "JOÃO DA SILVA",
    "documento_socio": "***999999**",
    "codigo_qualificacao": 10,
    "data_entrada_sociedade": "20200101"
}
```

### Company Size Classification
``` python
COMPANY_SIZE = {
    "00": "Not informed",
    "01": "Microenterprise",
    "03": "Small Business",
    "05": "Other companies"
}
```

## 📋 Features

- Data ingestion from Receita Federal ZIP archives
- Layered processing (Bronze → Silver → Gold)
- Data validation with Pandera
- Quality monitoring with PostgreSQL metrics
- Dashboards built with Apache Superset
- Orchestration with Apache Airflow


## 🛠️ Technologies
- Python 3.13 — Data processing
- Pandas — Data manipulation
- Airflow — Pipeline orchestration
- Pandera — Data validation
- PostgreSQL — Storage and metrics
- Superset — Data visualization
- Docker — Containerization

## 🏗️ Architecture

![alt text](https://github.com/patriciatozi/receita-federal-empresas/blob/main/documentation/src/Arquitetura_Receita_Federal.png)


## 📁 Project Structure

``` sh
receita-federal-empresas/
├── dags/
│   └── receita_federal_job.py         # Main Airflow DAG
├── scripts/
│   ├── data_ingestion/                # Raw data ingestion
│   │   ├── companies.py
│   │   └── partners.py
│   ├── data_processing/               # Layer transformations
│   │   ├── companies.py
│   │   └── partners.py
│   ├── data_refinement/               # Gold layer
│   │   └── companies_detail.py
│   ├── data_quality/                  # Pandera validations
│   │   ├── bronze_validation.py
│   │   ├── silver_validation.py
│   │   ├── gold_validation.py
│   └── tests                          # Unit tests
│       ├── conftest.py
│       ├── test_data_quality.py
│       └── companies_detail.py
├── docker-compose.yml                 # Container orchestration
├── Dockerfile                         # Custom image
├── requirements.txt                   # Python dependencies
└── .env                               # Environment variables
```

## 📊 Data Layers

### 🥉 Bronze Layer (Raw)
- Purpose:
    - Raw data ingestion from Receita Federal
    - Original format preserved
    - Basic format validations
- Destinations:
    - PostgreSQL: `bronze_companies`, `bronze_partners`
    - Parquet: `/data/bronze/` (backup)

#### PostgreSQL Tables
##### 1) `bronze_companies`
Column | Type | Description 
--- | --- | --- 
cnpj | TEXT | Cadastro Nacional da Pessoa Jurídica 
razao_social | TEXT | Nome empresarial
natureza_juridica | INTEGER | Código de natureza jurídica 
qualificacao_responsavel | INTEGER | Qualificação da pessoa responsável pela empresa
capital_social | TEXT | Capital social da empresa
cod_porte | TEXT | Código do porte da empresa
last_update | TEXT | Última atualização mensal dos dados origem

##### 2) `bronze_partners`
Column | Type | Description 
--- | --- | --- 
cnpj | TEXT | Cadastro Nacional da Pessoa Jurídica 
tipo_socio | TEXT | Tipo do sócio da empresa
nome_socio | TEXT | Corresponde ao nome do sócio pessoa física, razão social e/ou nome da empresa 
documento_socio | TEXT | CPF ou CNPJ do sócio, sócios estrangeiros são representados por `***999999**`
codigo_qualificacao_socio | TEXT | Capital social da empresa
last_update | TEXT | Última atualização mensal dos dados origem

### 🥈 Silver Layer (Cleaned)
- Purpose:
    - Dados limpos e tratados
    - Schema validation com Pandera
    - Padronização de formatos
- Destinations:
    - PostgreSQL: `silver_companies`, `silver_partners`
    - Parquet: `/data/silver/` (backup)

#### PostgreSQL Tables
##### 1) `silver_companies`
Column | Type | Description 
--- | --- | --- 
cnpj | TEXT | Cadastro Nacional da Pessoa Jurídica 
razao_social | TEXT | Nome empresarial
natureza_juridica | INTEGER | Código de natureza jurídica 
qualificacao_responsavel | INTEGER | Qualificação da pessoa responsável pela empresa
capital_social | FLOAT | Capital social da empresa
cod_porte | TEXT | Código do porte da empresa
last_update | DATE | Última atualização de processamento dos dados

##### 2) `silver_partners`
Column | Type | Description 
--- | --- | --- 
cnpj | TEXT | Cadastro Nacional da Pessoa Jurídica 
tipo_socio | TEXT | Tipo do sócio da empresa
nome_socio | TEXT | Corresponde ao nome do sócio pessoa física, razão social e/ou nome da empresa 
documento_socio | TEXT | CPF ou CNPJ do sócio, sócios estrangeiros são representados por `***999999**`
codigo_qualificacao_socio | TEXT | Capital social da empresa
flag_socio_estrangeiro | INTEGER | Indicação de que se trata de um sócio estrangeiro (`1`) ou não (`0`)
last_update | DATE | Última atualização de processamento dos dados

### 🥇 Gold Layer (Business)
- Purpose:
    - Enriched data to be consumed for data analysis
    - Business metrics
    - Data transformation and aggregation
- Destinations:
    - PostgreSQL: `gold_companies_detail`
    - Parquet: `/data/gold/` (backup)

#### PostgreSQL Tables
##### `gold_companies_detail`
Column | Type | Description 
--- | --- | --- 
cnpj | TEXT | Cadastro Nacional da Pessoa Jurídica 
qtde_socios | INTEGER | Número de sócios participantes do CNPJ
flag_socio_estrangeiro | BOOLEAN | Indicação de sócio estrangeiro (`True`) ou não estrangeiro (`False`)
doc_alvo | BOOLEAN | `True` quando porte da empresa = 03 & qtde_socios > 1, `False` para os demais casos
last_update | DATE | Última atualização de processamento dos dados

## ✅ Data Quality Checks

### Implemented Validations
#### Silver Layer:
- ✅ Deduplicated CNPJ
- ✅ Valid company size code (00, 01, 03, 05)
- ✅ Legal nature within expected range (1011–9999)
- ✅ Valid partner type (1, 2, or 3)

#### Gold Layer:
- ✅ Deduplicated CNPJ with 14 digits
- ✅ Consistent boolean flags
- ✅ Business rules applied

### Monitored Metrics
``` sql
-- Example of collected metrics
SELECT * FROM data_quality_metrics 
WHERE table_name = 'bronze_companies' 
ORDER BY last_update DESC 
LIMIT 10;
```

## 📈 Superset Dashboards

### Data Quality Metrics
#### 1) Record Count (`total_records`)
- Counts the total number of rows in each table
- Helps identify ingestion problems or incomplete loads
#### 2) Completeness (`null_percentage`)
- Percentual de valores nulos nas tabelas
- Percentage of null values in tables
- Enables monitoring of data completeness and identification of critical columns with high missing data rates
- In the dashboard, data are categorized as `Valid` (filled data) or `Mostly Nulls` (missing data)
#### 3) Uniqueness (`dq_duplicate_count`)
- Number of duplicate records in each table
- Evaluates data consistency by ensuring that unique keys or records are not repeated

### Visualization Example
![alt text](https://github.com/patriciatozi/receita-federal-empresas/blob/main/documentation/src/superset_dq_dashboard.png)

## 🚀 Environment Setup
### Prerequisites
- Docker 20.10+
- Docker Compose 2.0+
- 8GB RAM recommended

### 1. Clone the repository
``` sh
git clone https://github.com/patriciatozi/receita-federal-empresas.git
cd receita-federal-empresas
```
### 2. Configure environment variables
``` sh
cp .env.example .env
# Edit the .env file with your configurations
```
### 3. Run the containers
``` sh
# Start all services
docker-compose up -d

# Or build images and then start
docker-compose build
docker-compose up -d
```
### 4. Access the interfaces
- Airflow: http://localhost:8080
    - Username: airflow
    - Password: airflow
- Superset: http://localhost:8088
    - Username: admin
    - Password: admin

## 🔧 Useful Commands

### Docker Compose
``` sh
# Check container status
docker-compose ps

# Airflow logs
docker-compose logs airflow-scheduler

# Execute a command inside a specific container
docker-compose exec airflow-scheduler airflow dags list

# Stop all services
docker-compose down

# Stop and remove volumes
docker-compose down -v
```
### Airflow
``` sh
# List DAGs
docker-compose exec airflow-scheduler airflow dags list

# Trigger DAG manually
docker-compose exec airflow-scheduler airflow dags trigger receita_federal_job

# View task logs
docker-compose exec airflow-webserver airflow tasks logs receita_federal_job bronze_companies
```
### Desenvolvimento
``` sh
# Install dependencies locally
pip install -r requirements.txt

# Run an individual script
python scripts/data_ingestion/companies.py

# Test validations
python scripts/data_quality/silver_validation.py
```

## 📝 Next Steps
### Future Improvements
- Implement data quality alerts
- Make data source reading dynamic, not limited to a single file per type (companies or partners) — always loading the most recent available directory
- Refactor to PySpark and deploy to the cloud for:
    - More efficient distributed processing
    - Prevention of data skew issues
    - Implementation of data partitioning

## 📄 License
This project is licensed under the MIT License. See the LICENSE file for details.
