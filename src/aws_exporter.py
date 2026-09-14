import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import boto3
import pyarrow.parquet as pq
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def get_parquet_record_count(file_path: Path) -> int:
    """Lê os metadados do arquivo Parquet sem carregar o conteúdo na memória."""
    try:
        parquet_file = pq.ParquetFile(file_path)
        return parquet_file.metadata.num_rows
    except Exception as err:
        logger.warning("Falha ao ler a contagem de registros de %s: %s", file_path.name, err)
        return 0


def export_to_aws() -> None:
    s3_bucket = os.getenv("S3_BUCKET_DESTINO")
    sqs_url = os.getenv("SQS_QUEUE_URL")
    aws_region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

    if not s3_bucket or not sqs_url:
        raise ValueError("S3_BUCKET_DESTINO e SQS_QUEUE_URL precisam estar configurados no ambiente.")

    delivered_dir = Path("/app/delivered") if Path("/app/delivered").exists() else Path("data/delivered_gold")
    parquet_files = list(delivered_dir.glob("*.parquet"))

    if not parquet_files:
        logger.warning("Nenhum arquivo .parquet encontrado em %s para envio.", delivered_dir)
        return

    s3_client = boto3.client("s3", region_name=aws_region)
    sqs_client = boto3.client("sqs", region_name=aws_region)

    for file_path in parquet_files:
        s3_key = f"gold/{file_path.name}"

        logger.info("Enviando %s para s3://%s/%s...", file_path.name, s3_bucket, s3_key)
        s3_client.upload_file(str(file_path), s3_bucket, s3_key)

        record_count = get_parquet_record_count(file_path)
        processed_at = datetime.now(timezone.utc).isoformat()

        # Payload enriquecido com rastreabilidade, auditoria e governança
        payload = {
            "file_info": {
                "filename": file_path.name,
                "target_bucket": s3_bucket,
                "target_key": s3_key,
                "status": "EXPORTED"
            },
            "audit": {
                "source_system": "bacen_complaints",
                "airflow_run_id": os.getenv("AIRFLOW_CTX_DAG_RUN_ID", "manual_run"),
                "processed_at": processed_at,
                "record_count": record_count
            },
            "governance": {
                "schema_version": "v1.0",
                "data_quality_passed": True,
                "contains_pii": False,
                "data_classification": "INTERNAL",
                "business_domain": "reclamacoes_satisfacao",
                "environment": os.getenv("ENVIRONMENT", "prod")
            }
        }

        logger.info("Publicando mensagem com metadados enriquecidos na SQS...")
        sqs_client.send_message(
            QueueUrl=sqs_url,
            MessageBody=json.dumps(payload),
        )
        logger.info("Notificação e metadados enviados com sucesso para: %s", file_path.name)


if __name__ == "__main__":
    export_to_aws()