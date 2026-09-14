import logging
import os
from dotenv import load_dotenv

try:
    from metadata.workflow.metadata import MetadataWorkflow
except ImportError:
    from metadata.workflow.ingestion import MetadataWorkflow

try:
    from metadata.generated.schema.entity.services.connections.database.postgresConnection import (
        PostgresConnection,
    )
except ImportError:
    PostgresConnection = None

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def get_postgres_connection_config(
    db_host: str, db_port: str, db_user: str, db_password: str, db_name: str
) -> dict:
    """Monta a configuracao de conexao compativel com a versao do OpenMetadata."""
    conn_config = {
        "type": "Postgres",
        "hostPort": f"{db_host}:{db_port}",
        "username": db_user,
        "database": db_name,
    }

    if PostgresConnection:
        fields = getattr(
            PostgresConnection,
            "model_fields",
            getattr(PostgresConnection, "__fields__", {}),
        )
        if "authType" in fields:
            conn_config["authType"] = {"password": db_password}
        elif "password" in fields:
            conn_config["password"] = db_password
        else:
            conn_config["authType"] = {"password": db_password}
    else:
        conn_config["authType"] = {"password": db_password}

    return conn_config


def run() -> None:
    db_host = os.getenv("TARGET_DB_HOST") or os.getenv("DB_HOST", "postgres-db")
    if db_host in ("postgres", "localhost", "127.0.0.1", "", None):
        db_host = "postgres-db"

    db_port = os.getenv("TARGET_DB_PORT") or os.getenv("DB_PORT", "5432")
    db_user = os.getenv("TARGET_DB_USER") or os.getenv("DB_USER", "postgres")
    db_password = os.getenv("TARGET_DB_PASS") or os.getenv("DB_PASSWORD", "postgres")
    db_name = os.getenv("TARGET_DB_NAME") or os.getenv("DB_NAME", "atv4")

    om_server_url = os.getenv("OM_SERVER_URL", "http://openmetadata-server:8585/api")
    om_jwt_token = os.getenv("OM_JWT_TOKEN", "")

    logger.info("Ingerindo catalogo de %s:%s para %s", db_host, db_port, om_server_url)

    pg_conn_config = get_postgres_connection_config(
        db_host, db_port, db_user, db_password, db_name
    )

    config = {
        "source": {
            "type": "postgres",
            "serviceName": "postgres_service",
            "serviceConnection": {"config": pg_conn_config},
            "sourceConfig": {
                "config": {
                    "type": "DatabaseMetadata",
                    "includeTables": True,
                    "includeViews": True,
                }
            },
        },
        "sink": {
            "type": "metadata-rest",
            "config": {},
        },
        "workflowConfig": {
            "openMetadataServerConfig": {
                "hostPort": om_server_url,
                "authProvider": "openmetadata",
                "securityConfig": {
                    "jwtToken": om_jwt_token,
                },
            }
        },
    }

    workflow = MetadataWorkflow.create(config)
    workflow.execute()
    workflow.print_status()
    workflow.stop()


if __name__ == "__main__":
    run()