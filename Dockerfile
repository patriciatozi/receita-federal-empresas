FROM apache/airflow:3.0.1

# Copia requirements e instala dependências principais
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

# Instala dependências de desenvolvimento e testes
RUN pip install --no-cache-dir \
    pytest==8.3.4 \
    pytest-cov==5.0.0 \
    pytest-mock==3.14.0 \
    pytest-xdist==3.5.0 \
    python-dotenv==1.0.0

# Cria diretório para testes
RUN mkdir -p /opt/airflow/tests && \
    chown -R airflow:root /opt/airflow/tests

# Garante permissões corretas
USER root
RUN chown -R airflow:root /opt/airflow
USER airflow