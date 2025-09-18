# 📊 Data Pipeline - Receita Federal de Empresas

Pipeline de dados para processamento de informações cadastrais de empresas brasileiras da Receita Federal.

## 🏗️ Arquitetura

![alt text](https://file%2B.vscode-resource.vscode-cdn.net/Users/patriciatozi/Documents/Projetos/GitHub/receita-federal-empresas/documentation/src/Arquitetura%20-%20Receita%20Federal.png?version%3D1758221667306)

## 📋 Funcionalidades

- ✅ Ingestão de dados dos arquivos ZIP da Receita Federal
- ✅ Processamento em camadas (Bronze → Silver → Gold)
- ✅ Validação de dados com Pandera
- ✅ Monitoramento de qualidade com métricas em PostgreSQL
- ✅ Dashboards no Apache Superset
- ✅ Orquestração com Apache Airflow

## 🛠️ Tecnologias
- Python 3.12 - Processamento de dados
- Pandas - Manipulação de dados
- Airflow - Orquestração de pipelines
- Pandera - Validação de dados
- PostgreSQL - Armazenamento de dados e métricas
- Superset - Visualização de dados
- Docker - Containerização

## 📁 Estrutura do Projeto

``` sh
receita-federal-empresas/
├── dags/
│   └── main_pipeline.py          # DAG principal do Airflow
├── scripts/
│   ├── data_ingestion/           # Ingestão de dados brutos
│   │   ├── companies.py
│   │   └── partners.py
│   ├── data_processing/          # Processamento das camadas
│   │   ├── companies.py
│   │   └── partners.py
│   ├── data_refinement/          # Camada gold
│   │   └── companies_detail.py
│   └── data_quality/             # Validações com Pandera
│       ├── bronze_validation.py
│       ├── silver_validation.py
│       └── gold_validation.py
├── docker-compose.yml            # Orquestração de containers
├── Dockerfile                    # Imagem customizada
├── requirements.txt              # Dependências Python
└── .env                          # Variáveis de ambiente
```

## 🚀 Setup do Ambiente
### Pré-requisitos
- Docker 20.10+
- Docker Compose 2.0+
- 8GB RAM recomendado

### 1. Clone o repositório
``` sh
git clone https://github.com/seu-usuario/receita-federal-empresas.git
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
- ✅ CNPJ com 14 dígitos
- ✅ Código de porte válido (00, 01, 03, 05)
- ✅ Natureza jurídica dentro da faixa esperada
- ✅ Capital social não negativo
#### Camada Gold:
- ✅ Quantidade de sócios > 0
- ✅ Flags booleanas consistentes
- ✅ Regras de negócio aplicadas

### Métricas Monitoradas
``` sql
-- Exemplo de métricas coletadas
SELECT * FROM data_quality_metrics 
WHERE table_name = 'companies' 
ORDER BY created_at DESC 
LIMITE 10;
```

## 📈 Dashboards no Superset

TBD

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
docker-compose exec airflow-scheduler airflow dags trigger main_pipeline

# Ver logs de uma task
docker-compose exec airflow-webserver airflow tasks logs main_pipeline bronze_companies
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
-Adicionar testes unitários
- Implementar alertas de qualidade
- Adicionar mais fontes de dados
- Otimizar performance de queries
- Implementar particionamento de dados
### Customização
- Editar scripts/data_processing/ para novas transformações
- Modificar scripts/data_quality/ para novas validações
- Ajustar dags/main_pipeline.py para alterar o fluxo
- Customizar queries no Superset para novos dashboards

## 🤝 Contribuição
1. Fork o projeto
2. Crie uma branch para sua feature (git checkout -b feature/AmazingFeature)
3. Commit suas mudanças (git commit -m 'Add some AmazingFeature')
4. Push para a branch (git push origin feature/AmazingFeature)
5. Abra um Pull Request

## 📄 Licença
Este projeto está sob a licença MIT. Veja o arquivo LICENSE para detalhes.