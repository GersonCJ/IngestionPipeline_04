FROM python:3.14-slim

# Instala o Java (necessário para o PySpark) e limpa o cache do apt
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# Define a variável JAVA_HOME para o PySpark encontrar o Java
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Download the latest installer
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Ensure installed binary is on the PATH
ENV PATH="/root/.local/bin/:$PATH"

WORKDIR /app
ENV PYTHONPATH="/app"

# Pontos de montagem das camadas (ver constants/path_strings.py).
# O conteudo vem dos mounts em tempo de execucao; aqui so garantimos que os
# diretorios existam quando a imagem roda sem nenhum volume.
RUN mkdir -p data/raw trusted delivered gx

# Copy the project into the image
COPY . .

# Disable development dependencies
ENV UV_NO_DEV=1

# Sync the project to a project environment
RUN uv --version
RUN uv sync
