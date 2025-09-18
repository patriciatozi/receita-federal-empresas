# 📊 Data Pipeline - Receita Federal de Empresas

Pipeline de dados para processamento de informações cadastrais de empresas brasileiras da Receita Federal.

## 🏢 Sobre os Dados CNPJ
Os dados utilizados neste projeto são os Dados Abertos do CNPJ disponibilizados pela Receita Federal do Brasil, que contêm informações cadastrais completas sobre empresas e estabelecimentos brasileiros.

### 📋 Principais Características dos Dados
Fonte Oficial:
- 📍 https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj
- 📍 https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/consultas/dados-publicos-cnpj

#### Atualização:

- 🔄 Os dados são atualizados mensalmente
- ⚠️ Podem sofrer desatualização de até 3 meses em relação à data atual

#### Estrutura dos Arquivos:

- 📦 Arquivos no formato ZIP contendo CSV
- 🗂️ Separados por tipo de informação (Empresas, Estabelecimentos, Sócios)
- 📊 Dados em formato delimitado por ponto e vírgula
- 🇧🇷 Codificação Latin-1 (ISO-8859-1)

## 📁 Tipos de Dados Disponíveis
### 1. Dados de Empresas
``` sh
# Estrutura principal
{
    "cnpj": "00000000000191",           # CNPJ raiz (8 dígitos)
    "razao_social": "EMPRESA BRASILEIRA",
    "natureza_juridica": 2051,          # Código da natureza jurídica
    "qualificacao_responsavel": 10,     # Qualificação do responsável
    "capital_social": 1000000.00,       # Capital social da empresa
    "porte_empresa": "05",              # Porte da empresa (00, 01, 03, 05)
    "ente_federativo": ""               # Ente federativo responsável
}
```

### 2. Dados de Sócios
``` sh
{
    "cnpj": "00000000000191",           # CNPJ da empresa
    "tipo_socio": 1,                    # 1=PJ, 2=PF, 3=Estrangeiro
    "nome_socio": "JOÃO DA SILVA",
    "documento_socio": "00000000000",   # CPF ou CNPJ do sócio
    "codigo_qualificacao": 10,          # Código de qualificação
    "data_entrada_sociedade": "20200101" # Data de entrada
}
```

### 🎯 Dados Relevantes para o Desafio
#### Campos Utilizados no Projeto:
Para empresas:
- `cnpj`: Número do CNPJ (14 dígitos)
- `razao_social`: Nome empresarial
- `natureza_juridica`: Código da natureza jurídica
- `qualificacao_responsavel`: Qualificação do responsável
- `capital_social`: Valor do capital social
- `porte_empresa`: Porte da empresa (00, 01, 03, 05)

Para sócios:
- `cnpj`: CNPJ da empresa
- `tipo_socio`: Tipo de sócio (1, 2, 3)
- `nome_socio`: Nome do sócio
- `documento_socio`: CPF/CNPJ do sócio
- `codigo_qualificacao`: Código de qualificação

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

- ✅ Ingestão de dados dos arquivos ZIP da Receita Federal
- ✅ Processamento em camadas (Bronze → Silver → Gold)
- ✅ Validação de dados com Pandera
- ✅ Monitoramento de qualidade com métricas em PostgreSQL
- ✅ Dashboards no Apache Superset
- ✅ Orquestração com Apache Airflow


## 🛠️ Tecnologias
- Python 3.13 - Processamento de dados
- Pandas - Manipulação de dados
- Airflow - Orquestração de pipelines
- Pandera - Validação de dados
- PostgreSQL - Armazenamento de dados e métricas
- Superset - Visualização de dados
- Docker - Containerização

## 🏗️ Arquitetura

![alt text](https://github.com/patriciatozi/receita-federal-empresas/blob/main/documentation/src/Arquitetura%20-%20Receita%20Federal.png)


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
│   │   ├──gold_validation.py
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
- Dados brutos ingeridos da Receita Federal
- Formato original preservado
- Validações básicas de formato

### 🥈 Silver Layer (Cleaned)
- Dados limpos e tratados
- Schema validation com Pandera
- Padronização de formatos

### 🥇 Gold Layer (Business)
- Dados enriquecidos para análise
- Métricas de negócio
- Agregações e transformações

## ✅ Data Quality Checks

### Validações Implementadas
#### Camada Silver:
- ✅ CNPJ deduplicado
- ✅ Código de porte válido (00, 01, 03, 05)
- ✅ Natureza jurídica dentro da faixa esperada (1011 a 9999)
- ✅ Tipo de Sócio válido (1, 2 ou 3)

#### Camada Gold:
- ✅ CNPJ deduplicado com 14 dígitos
- ✅ Quantidade de sócios > 0
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

TBD

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
- Adicionar mais fontes de dados
- Otimizar performance de queries
- Implementar particionamento de dados
### Customização
- Editar scripts/data_processing/ para novas transformações
- Modificar scripts/data_quality/ para novas validações

## 📄 Licença
Este projeto está sob a licença MIT. Veja o arquivo LICENSE para detalhes.
