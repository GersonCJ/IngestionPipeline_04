import json
import logging
import os
from pathlib import Path

import boto3
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def export_to_aws() -> None:
    s3_bucket = os.getenv("S3_BUCKET_DESTINO")
    sqs_url = os.getenv("SQS_QUEUE_URL")
    aws_region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

    if not s3_bucket or not sqs_url:
        raise ValueError("S3_BUCKET_DESTINO e SQS_QUEUE_URL precisam estar configurados no ambiente.")

    # Prioriza a pasta dentro do container Docker (/app/delivered) ou cai para o relativo local
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

        payload = {
            "bucket": s3_bucket,
            "key": s3_key,
            "status": "EXPORTED",
            "filename": file_path.name,
        }

        logger.info("Publicando mensagem na fila SQS (%s)...", sqs_url)
        sqs_client.send_message(
            QueueUrl=sqs_url,
            MessageBody=json.dumps(payload),
        )
        logger.info("Notificação enviada com sucesso para o arquivo: %s", file_path.name)


if __name__ == "__main__":
    export_to_aws()