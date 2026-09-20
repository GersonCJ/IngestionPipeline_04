import json
import os
import time
from pathlib import Path
from kafka import KafkaProducer

# Configurações do ambiente
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'dados-raw-topic')
RAW_DATA_DIR = os.getenv('RAW_DATA_DIR', r'D:\ingestao\IngestionPipeline_04\data\raw_free')
SLEEP_INTERVAL = float(os.getenv('SLEEP_INTERVAL', '0.5'))

def create_kafka_producer():
    """Inicializa o produtor do Kafka com serialização em JSON."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def read_and_parse_file(file_path):
    """Lê arquivos locais mantendo a lógica do script de origem."""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    try:
        data = json.loads(content)
        items = data if isinstance(data, list) else [data]
    except json.JSONDecodeError:
        items = [{'linha': line} for line in content.splitlines() if line.strip()]

    return items

def run_producer():
    producer = create_kafka_producer()
    raw_path = Path(RAW_DATA_DIR)

    if not raw_path.exists():
        print(f"Erro: O diretório '{RAW_DATA_DIR}' não existe.")
        return

    print("Iniciando o produtor Kafka...")
    print(f"Conectado em: {KAFKA_BOOTSTRAP_SERVERS} | Tópico: {KAFKA_TOPIC}")

    for file_path in raw_path.glob('*'):
        if file_path.is_file():
            print(f"Processando arquivo: {file_path.name}")
            items = read_and_parse_file(file_path)

            for item in items:
                producer.send(KAFKA_TOPIC, value=item)
                print(f"Mensagem enviada: {item}")
                time.sleep(SLEEP_INTERVAL)

    producer.flush()
    producer.close()
    print("Todos os arquivos foram processados e enviados ao Kafka!")

if __name__ == '__main__':
    run_producer()