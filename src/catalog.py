"""Publica o catalogo do Postgres no OpenMetadata."""

import logging
import os
from pathlib import Path

import yaml
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

CONFIG_PATH = Path(os.getenv("OM_CONFIG_PATH", "/app/om/postgres_catalog.yaml"))


def build_config() -> dict:
    """Le o YAML da ingestao e completa o que vem do ambiente."""
    config = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))

    token = os.getenv("OM_JWT_TOKEN", "").strip()
    if not token:
        raise RuntimeError(
            "OM_JWT_TOKEN nao definido. Pegue o token em "
            "http://localhost:8585 -> Settings -> Bots -> ingestion-bot "
            "e preencha OM_JWT_TOKEN no .env."
        )

    connection = config["source"]["serviceConnection"]["config"]
    connection.pop("password", None)
    
    connection["username"] = os.environ["DB_USER"]
    connection["authType"] = {"password": os.environ["DB_PASSWORD"]}
    connection["hostPort"] = f"{os.environ['DB_HOST']}:{os.environ['DB_PORT']}"
    connection["database"] = os.environ["DB_NAME"]

    server = config["workflowConfig"]["openMetadataServerConfig"]
    server["hostPort"] = os.getenv("OM_SERVER_URL", "http://openmetadata-server:8585/api")
    server["securityConfig"] = {"jwtToken": token}

    return config


def run() -> None:
    """Executa a ingestao e falha se algum passo do workflow reportar erro."""
    from metadata.workflow.metadata import MetadataWorkflow

    config = build_config()

    logger.info(
        "Ingerindo catalogo de %s para %s",
        config["source"]["serviceConnection"]["config"]["hostPort"],
        config["workflowConfig"]["openMetadataServerConfig"]["hostPort"],
    )

    workflow = MetadataWorkflow.create(config)

    try:
        workflow.execute()
        workflow.print_status()
        workflow.raise_from_status()
    finally:
        workflow.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run()