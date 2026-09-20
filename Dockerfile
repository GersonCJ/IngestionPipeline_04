FROM python:3.12-slim

# Evita falhas de hardlink do uv ao mapear volumes com o host
ENV UV_LINK_MODE=copy
ENV UV_NO_DEV=1

# Instala o Java (necessário para o PySpark) e limpa o cache do apt
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

# Baixa o instalador do uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
ENV PATH="/root/.local/bin/:$PATH"

WORKDIR /app
ENV PYTHONPATH="/app"

RUN mkdir -p data/raw trusted delivered gx

COPY . .

# Sincroniza as dependências do projeto
RUN uv sync