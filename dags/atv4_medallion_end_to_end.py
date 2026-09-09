"""Pipeline medalhao Atv4: raw -> trusted -> delivery -> catalogo.

Cada task e um container efemero criado pelo DockerOperator a partir das
imagens `ingestion_atv4` e `dbt_atv4`. O Airflow nao carrega pandas, Great
Expectations nem dbt — ele so orquestra.

Duas particularidades desse desenho, ambas consequencia de o Airflow rodar
dentro de um container:

1. Os `mounts` sao resolvidos pelo daemon do Docker, nao pelo filesystem deste
   container. Por isso os caminhos vem de HOST_PROJECT_DIR, que descreve o
   repositorio como o daemon o enxerga (no Docker Desktop, algo como
   /run/desktop/mnt/host/c/...). Um caminho errado aqui nao gera erro: o
   daemon cria um diretorio vazio e a task "passa" sem processar nada.
2. `mount_tmp_dir=False` e obrigatorio — o diretorio temporario que o operator
   monta por padrao existe so aqui dentro, e o daemon nao o enxerga.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from docker.types import Mount

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG

# ---------------------------------------------------------------- ambiente

HOST_PROJECT_DIR = os.getenv("HOST_PROJECT_DIR", "").rstrip("/")

if not HOST_PROJECT_DIR:
    # Falhar no parse e melhor do que rodar: sem esta variavel os mounts
    # apontariam para caminhos vazios e as tasks passariam sem processar nada.
    # O erro aparece como Import Error na UI do Airflow.
    raise ValueError(
        "HOST_PROJECT_DIR nao definido. Preencha no .env com o caminho deste "
        "repositorio como o daemon do Docker o enxerga — no Docker Desktop, "
        "/run/desktop/mnt/host/c/... — e reinicie os servicos do Airflow."
    )

NETWORK = os.getenv("ATV4_NETWORK", "atv4_private")
DOCKER_URL = os.getenv("DOCKER_HOST", "unix:///var/run/docker.sock")

INGESTION_IMAGE = "ingestion_atv4"
DBT_IMAGE = "dbt_atv4"
DBT_PROJECT_DIR = "/usr/app/dbt_atv4_project"

DB_ENV = {
    "DB_HOST": os.getenv("DB_HOST", "postgres-db"),
    "DB_PORT": os.getenv("DB_PORT", "5432"),
    "DB_USER": os.getenv("DB_USER", "postgres"),
    "DB_PASSWORD": os.getenv("DB_PASSWORD", ""),
    "DB_NAME": os.getenv("DB_NAME", "pipeline_db"),
}

OM_ENV = {
    **DB_ENV,
    "OM_SERVER_URL": os.getenv("OM_SERVER_URL", "http://openmetadata-server:8585/api"),
    "OM_JWT_TOKEN": os.getenv("OM_JWT_TOKEN", ""),
}

# Camadas do medalhao, montadas do repositorio para dentro dos containers.
DATA_MOUNTS = [
    Mount(source=f"{HOST_PROJECT_DIR}/data/raw_free", target="/app/data/raw", type="bind"),
    Mount(source=f"{HOST_PROJECT_DIR}/data/trusted_parquet", target="/app/trusted", type="bind"),
    Mount(source=f"{HOST_PROJECT_DIR}/data/delivered_gold", target="/app/delivered", type="bind"),
    # Data Context do Great Expectations: volume nomeado, servido pelo nginx.
    Mount(source="gx_docs", target="/app/gx", type="volume"),
]

DBT_MOUNTS = [
    # Bind para que o target/ (manifest e docs) apareca no host e o nginx da
    # porta 8181 sirva o resultado.
    Mount(source=f"{HOST_PROJECT_DIR}/elt_dbt_atv4", target=DBT_PROJECT_DIR, type="bind"),
]

COMMON = dict(
    docker_url=DOCKER_URL,
    network_mode=NETWORK,
    auto_remove="force",
    mount_tmp_dir=False,
    do_xcom_push=False,
    tty=False,
)


def ingestion_task(task_id: str, step: str, **kwargs) -> DockerOperator:
    """Uma etapa de `src/cli.py` rodando na imagem do projeto."""
    return DockerOperator(
        task_id=task_id,
        image=INGESTION_IMAGE,
        command=f"uv run python -m src.cli {step}",
        mounts=DATA_MOUNTS,
        environment=kwargs.pop("environment", DB_ENV),
        **COMMON,
        **kwargs,
    )


def dbt_task(task_id: str, command: str) -> DockerOperator:
    """Um comando dbt rodando na imagem dbt-postgres."""
    return DockerOperator(
        task_id=task_id,
        image=DBT_IMAGE,
        # Entrypoint explicito em vez de depender do que a imagem define (ou de
        # limpa-lo com uma string vazia, que tem comportamento ambiguo).
        entrypoint="dbt",
        working_dir=DBT_PROJECT_DIR,
        command=f"{command} --project-dir {DBT_PROJECT_DIR} --profiles-dir /root/.dbt",
        mounts=DBT_MOUNTS,
        environment=DB_ENV,
        **COMMON,
    )


with DAG(
    dag_id="atv4_medallion_end_to_end",
    description="Raw -> Trusted -> Delivery com gates do Great Expectations e catalogo no OpenMetadata",
    # As fontes sao arquivos estaticos em data/raw_free: nao ha dado novo
    # chegando, entao o disparo e manual.
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0},
    tags=["atv4", "medallion", "dbt", "great-expectations", "openmetadata"],
) as dag:

    raw_to_trusted = ingestion_task(
        "raw_to_trusted",
        "transform",
    )

    # Gate duro: se uma suite reprovar, a task falha e nada abaixo executa —
    # dado reprovado nao chega ao Postgres nem a camada Delivery.
    gx_trusted = ingestion_task(
        "gx_trusted",
        "quality-trusted",
    )

    load_postgres = ingestion_task(
        "load_postgres",
        "load",
    )

    dbt_run = dbt_task("dbt_run", "run")

    dbt_test = dbt_task("dbt_test", "test")

    dbt_docs_generate = dbt_task("dbt_docs_generate", "docs generate")

    export_delivery = ingestion_task(
        "export_delivery",
        "export",
    )

    gx_delivery = ingestion_task(
        "gx_delivery",
        "quality-delivery",
    )

    openmetadata_catalog = ingestion_task(
        "openmetadata_catalog",
        "catalog",
        environment=OM_ENV,
        # Unica task que depende de um servico externo pesado, que pode ainda
        # estar subindo logo apos um `docker compose up`.
        retries=2,
        retry_delay=timedelta(seconds=60),
    )

    (
        raw_to_trusted
        >> gx_trusted
        >> load_postgres
        >> dbt_run
        >> dbt_test
        >> dbt_docs_generate
        >> export_delivery
        >> gx_delivery
        >> openmetadata_catalog
    )
