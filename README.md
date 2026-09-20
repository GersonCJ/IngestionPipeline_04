# Atv4 — Ingestão e processamento de dados

Este repositório reúne uma stack de ingestão e processamento de dados com Python, Kafka, PySpark e PostgreSQL. A implementação ativa do projeto, conforme os arquivos atuais em `docker-compose.yml` e `kafka/`, é uma pipeline de streaming que produz eventos em Kafka, processa com PySpark e persiste em PostgreSQL e arquivos Parquet.

A pasta `src/` também contém utilitários para leitura, validação e tratamento local de arquivos brutos, mas a arquitetura executada em containers é a de streaming em tempo real.

---

## Visão geral da arquitetura atual

```text
Arquivos brutos em data/raw_free/
          │
          ▼
Python / scripts de ingestão
          │
          ▼
Kafka (producer + consumer)
          │
          ▼
PySpark (processamento de streams)
          │
          ├── escrita em Parquet
          └── persistência em PostgreSQL
```

### Componentes principais

- `kafka/producer.py`: produz mensagens para o Kafka
- `kafka/consumer.py`: consome esses eventos e aplica a lógica de transformação
- `docker-compose.yml`: orquestra PostgreSQL, Zookeeper, Kafka e os serviços Python
- `src/`: módulos de leitura, validação, tratamento e carga de dados locais
- `database/init.sql`: bootstrap inicial do banco
- `data/`: fontes e saídas do pipeline

---

## Estrutura do projeto

```text
.
├── data/
│   ├── raw_free/
│   │   ├── Bancos/
│   │   ├── Complains/
│   │   └── Empregados/
│   ├── trusted_parquet/
│   └── delivered_gold/
├── src/
│   ├── cli.py
│   ├── catalog.py
│   ├── load.py
│   ├── schemas.py
│   ├── sources.py
│   └── treatment.py
├── kafka/
│   ├── producer.py
│   └── consumer.py
├── constants/
│   └── path_strings.py
├── database/
│   └── init.sql
├── profiles/
│   └── profiles.yml
├── docker-compose.yml
├── Dockerfile
├── Dockerfile.consumer
├── pyproject.toml
├── .env
├── EXECUCAO.md
├── export_delivery.py
├── README.md
├── diagrama.txt
└── .gitignore
```

---

## Stack ativa

### 1) Kafka + Zookeeper
O ambiente de containers inclui:

- `zookeeper`
- `kafka`
- `python-producer`
- `pyspark-consumer`
- `postgres-db`

A sequência principal é:

1. producer publica eventos em Kafka
2. consumer lê streams do tópico
3. processamento e enriquecimento são executados em PySpark
4. os dados são gravados em Parquet e/ou persistidos no PostgreSQL

### 2) PostgreSQL
O banco principal é provisionado pelo serviço `postgres-db`.

Configuração padrão:

- host: `postgres-db`
- porta: `5432`
- usuário: `postgres`
- senha: `postgres`
- banco: `atv4`

### 3) Python para tratamento de arquivos locais
Os módulos em `src/` realizam o processamento de dados em disco, com foco em:

- leitura de arquivos por origem
- validação de registros com Pydantic
- quarentena e reconciliação
- escrita em parquet
- carga para PostgreSQL
- integração com catalogação de metadados

Arquivos importantes:

- `src/sources.py`
- `src/schemas.py`
- `src/treatment.py`
- `src/load.py`
- `src/cli.py`
- `src/catalog.py`

---

## Dados e fontes

Os arquivos de origem ficam em:

- `data/raw_free/Bancos/`
- `data/raw_free/Complains/`
- `data/raw_free/Empregados/`

As saídas tratadas ficam em:

- `data/trusted_parquet/`
- `data/delivered_gold/`

---

## Como subir a stack

### Pré-requisitos

- Docker Desktop em execução
- memória suficiente reservada para os containers locais
- acesso ao daemon do Docker

### Inicialização

No diretório raiz do projeto:

```bash
docker compose up -d
```

Para verificar os containers ativos:

```bash
docker compose ps
```

Para acompanhar logs relevantes:

```bash
docker compose logs -f kafka
docker compose logs -f pyspark-consumer
docker compose logs -f python-producer
```

---

## Variáveis de ambiente

O arquivo `.env` contém as configurações do ambiente do projeto, incluindo conexão com PostgreSQL e caminho do repositório para o Docker.

Exemplo:

```env
TARGET_DB_USER=postgres
TARGET_DB_PASS=postgres
TARGET_DB_HOST=postgres
TARGET_DB_PORT=5432
TARGET_DB_NAME=postgres
DB_USER=postgres
DB_PASSWORD=postgres
DB_NAME=postgres
HOST_PROJECT_DIR=/run/desktop/mnt/host/d/ingestao/IngestionPipeline_04
```

> `HOST_PROJECT_DIR` deve apontar para o caminho do repositório como o daemon do Docker o enxerga. Em Docker Desktop no Windows, normalmente esse valor fica em `/run/desktop/mnt/host/...`.

---

## Execução local do pipeline

Além do fluxo de streaming em containers, o projeto mantém uma interface de execução por etapas em `src/cli.py`.

```bash
uv run python -m src.cli transform
uv run python -m src.cli load
uv run python -m src.cli export
```

Esses comandos representam a camada de processamento local em etapas separadas, em vez de um único script monolítico.

---

## Comandos úteis

### Reconstruir imagens

```bash
docker compose --profile build build
```

### Reiniciar a stack inteira

```bash
docker compose down -v --remove-orphans
docker compose up -d
```

### Conectar ao PostgreSQL

```bash
docker exec -it postgres-db psql -U postgres -d atv4
```

### Verificar tópicos do Kafka

```bash
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list
```

---

## Observações importantes

- O contexto de execução ativo no momento é Kafka + PySpark + PostgreSQL.
- A pasta `src/` complementa a pipeline com módulos de leitura, validação e carga de dados locais.
- Há documentação e artefatos legados que mencionam dbt, Great Expectations e Airflow, mas a infraestrutura principal atualmente em funcionamento é a stack definida em `docker-compose.yml`.
- O arquivo `EXECUCAO.md` contém instruções adicionais para execução e troubleshooting do projeto.

---

## Arquivos de referência

- `docker-compose.yml`
- `kafka/producer.py`
- `kafka/consumer.py`
- `src/cli.py`
- `src/load.py`
- `src/treatment.py`
- `database/init.sql`
- `EXECUCAO.md`