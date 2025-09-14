FROM python:3.11-slim

WORKDIR /app

# Instalar dependências do sistema
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Copiar requirements e instalar dependências
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar TODO o código fonte para /app
COPY . .

# Variáveis de ambiente
ENV PYTHONPATH=/app

CMD ["python", "scripts/run_pipeline.py"]