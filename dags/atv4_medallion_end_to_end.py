from __future__ import annotations

import json
import os
from datetime import datetime, timedelta

from docker.types import Mount

from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG

# ---------------------------------------------------------------- ambiente

HOST_PROJECT_DIR = os.getenv("HOST_PROJECT_DIR", "").rstrip("/")

if not HOST_PROJECT_DIR:
    raise ValueError(
        "HOST_PROJECT_DIR nao definido. Preencha no .env com o caminho deste "
        "repositorio como o daemon do Docker o enxerga."
    )

NETWORK = os.getenv("ATV4_NETWORK", "atv4_private")
DOCKER_URL = os.getenv("DOCKER_HOST", "unix:///var/run/docker.sock")

INGESTION_IMAGE = "ingestion_atv4"
DBT_IMAGE = "dbt_atv4"
DBT_PROJECT_DIR = "/usr/app/dbt_atv4_project"

# Configurações de ambiente do Banco de Dados
DB_ENV = {
    "DB_HOST": os.getenv("TARGET_DB_HOST", os.getenv("DB_HOST", "postgres-db")),
    "DB_PORT": os.getenv("TARGET_DB_PORT", os.getenv("DB_PORT", "5432")),
    "DB_USER": os.getenv("TARGET_DB_USER", os.getenv("DB_USER", "postgres")),
    "DB_PASSWORD": os.getenv("TARGET_DB_PASS", os.getenv("DB_PASSWORD", "")),
    "DB_NAME": os.getenv("TARGET_DB_NAME", os.getenv("DB_NAME", "atv4")),
    "PYTHONPATH": "/app",
}

# Configurações de ambiente da AWS
AWS_ENV = {
    **DB_ENV,
    "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", ""),
    "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", ""),
    "AWS_SESSION_TOKEN": os.getenv("AWS_SESSION_TOKEN", ""),
    "AWS_DEFAULT_REGION": os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    "S3_BUCKET_ORIGEM": os.getenv("S3_BUCKET_ORIGEM", ""),
    "S3_BUCKET_DESTINO": os.getenv("S3_BUCKET_DESTINO", "bucket-destino-dados"),
    "SQS_QUEUE_URL": os.getenv("SQS_QUEUE_URL", ""),
}

# Configurações do OpenMetadata
OM_ENV = {
    **DB_ENV,
    "OM_SERVER_URL": os.getenv("OM_SERVER_URL", "http://openmetadata-server:8585/api"),
    "OM_JWT_TOKEN": os.getenv("OM_JWT_TOKEN", ""),
}

DATA_MOUNTS = [
    Mount(source=f"{HOST_PROJECT_DIR}/src", target="/app/src", type="bind"),
    Mount(source=f"{HOST_PROJECT_DIR}/data/raw_free", target="/app/data/raw", type="bind"),
    Mount(source=f"{HOST_PROJECT_DIR}/data/trusted_parquet", target="/app/trusted", type="bind"),
    Mount(source=f"{HOST_PROJECT_DIR}/data/delivered_gold", target="/app/delivered", type="bind"),
    Mount(source="gx_docs", target="/app/gx", type="volume"),
]

DBT_MOUNTS = [
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
        working_dir="/app",
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
        entrypoint="dbt",
        working_dir=DBT_PROJECT_DIR,
        command=f"{command} --project-dir {DBT_PROJECT_DIR} --profiles-dir /root/.dbt",
        mounts=DBT_MOUNTS,
        environment=DB_ENV,
        **COMMON,
    )


with DAG(
    dag_id="atv4_medallion_end_to_end",
    description="Raw -> Trusted -> Delivery com gates do Great Expectations, AWS Lambda e catalogo no OpenMetadata",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0},
    tags=["atv4", "medallion", "dbt", "great-expectations", "openmetadata", "aws"],
) as dag:

    # 1. Lambda Produtora
    lambda_producer = LambdaInvokeFunctionOperator(
        task_id="lambda_producer",
        function_name="lambda-produtora",
        aws_conn_id="aws_default",
        payload=json.dumps({
            "stage": "start",
            "sqs_url": os.getenv("SQS_QUEUE_URL"),
            "bucket_origem": os.getenv("S3_BUCKET_ORIGEM")
        }),
    )

    raw_to_trusted = ingestion_task("raw_to_trusted", "transform")
    gx_trusted = ingestion_task("gx_trusted", "quality-trusted")
    load_postgres = ingestion_task("load_postgres", "load")

    dbt_run = dbt_task("dbt_run", "run")
    dbt_test = dbt_task("dbt_test", "test")
    dbt_docs_generate = dbt_task("dbt_docs_generate", "docs generate")

    # Exportação local para parquet no diretório /app/delivered
    export_delivery = ingestion_task("export_delivery", "export")

    # Upload dos arquivos exportados para o S3 e notificação via SQS
    push_to_aws = DockerOperator(
        task_id="push_to_aws",
        image=INGESTION_IMAGE,
        working_dir="/app",
        command="uv run python -m src.aws_exporter",
        mounts=DATA_MOUNTS,
        environment=AWS_ENV,
        **COMMON,
    )

    gx_delivery = ingestion_task("gx_delivery", "quality-delivery")

    openmetadata_catalog = ingestion_task(
        "openmetadata_catalog",
        "catalog",
        environment=OM_ENV,
        retries=2,
        retry_delay=timedelta(seconds=60),
    )

    # 2. Lambda Consumidora (Lê SQS, enriquece via SQL e salva no S3 de destino)
    lambda_consumer = LambdaInvokeFunctionOperator(
        task_id="lambda_consumer",
        function_name="lambda-consumidora",
        aws_conn_id="aws_default",
        payload=json.dumps({
            "stage": "finish",
            "sqs_url": os.getenv("SQS_QUEUE_URL"),
            "dest_bucket": os.getenv("S3_BUCKET_DESTINO", "bucket-destino-dados")
        }),
    )

    # Fluxo de dependências
    (
        lambda_producer
        >> raw_to_trusted
        >> gx_trusted
        >> load_postgres
        >> dbt_run
        >> dbt_test
        >> dbt_docs_generate
        >> export_delivery
        >> push_to_aws
        >> gx_delivery
        >> openmetadata_catalog
        >> lambda_consumer
    )