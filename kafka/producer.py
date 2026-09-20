import os
import json
import time
import pandas as pd
from kafka import KafkaProducer

KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
DATA_DIR = os.getenv("DATA_DIR", "./data/raw_free")

def json_serializer(v):
    # Converte tipos complexos/datas para string e trata caracteres especiais
    return json.dumps(v, default=str, ensure_ascii=False).encode('utf-8')

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=json_serializer
    )

    if not os.path.exists(DATA_DIR):
        print(f"ERRO: Diretório '{DATA_DIR}' não foi encontrado no container.")
        return

    # Varre recursivamente todas as pastas e arquivos dentro de raw_free
    for root, _, files in os.walk(DATA_DIR):
        for file in files:
            if file.endswith('.csv'):
                csv_path = os.path.join(root, file)
                
                # Identifica o tipo de dataset pela pasta pai (ex: Complains -> dataset_type: Complains)
                dataset_type = os.path.basename(root)
                if dataset_type == os.path.basename(DATA_DIR):
                    dataset_type = "general"

                print(f"Lendo arquivo: {csv_path} | Tipo: {dataset_type}")
                
                try:
                    df = pd.read_csv(csv_path, sep=';', encoding='latin1')
                    # Substitui valores NaN/nulos do Pandas por None (convertido para null no JSON)
                    df = df.where(pd.notnull(df), None)
                    
                    for _, row in df.iterrows():
                        payload = {
                            "dataset_type": dataset_type,
                            "file_name": file,
                            "raw_data": row.to_dict()
                        }
                        producer.send("raw-events", value=payload)
                        time.sleep(0.01) # Pequeno intervalo para evitar gargalos de envio
                    
                    producer.flush()
                    print(f"Concluído: {file}")
                except Exception as e:
                    print(f"Erro ao processar {csv_path}: {e}")

if __name__ == "__main__":
    main()