import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, MapType


def main():
    # 1. Leitura das variáveis de ambiente
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    db_host = os.getenv("TARGET_DB_HOST", "postgres-db")
    db_port = os.getenv("TARGET_DB_PORT", "5432")
    db_name = os.getenv("TARGET_DB_NAME", "atv4")
    db_user = os.getenv("TARGET_DB_USER", "postgres")
    db_pass = os.getenv("TARGET_DB_PASS", "postgres")

    jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"

    # 2. Inicialização da SparkSession com drivers Kafka e PostgreSQL
    spark = (
        SparkSession.builder.appName("PySparkKafkaToPostgresConsumer")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # 3. Schema compatível com a estrutura enviada pelo producer.py
    payload_schema = StructType([
        StructField("dataset_type", StringType(), True),
        StructField("file_name", StringType(), True),
        StructField("raw_data", MapType(StringType(), StringType()), True),
    ])

    # 4. Leitura do Streaming Kafka (Tópico: raw-events)
    kafka_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_servers)
        .option("subscribe", "raw-events")
        .option("startingOffsets", "earliest")
        .load()
    )

    # 5. Desserialização do JSON e extração dos campos da chave 'raw_data'
    parsed_stream = (
        kafka_stream.selectExpr("CAST(value AS STRING) as json_payload")
        .select(from_json(col("json_payload"), payload_schema).alias("data"))
        .select("data.*")
        .filter(col("dataset_type") == "complaints")
        .select(
            col("raw_data")["CNPJ IF"].alias("cnpj_base"),
            col("raw_data")["Instituição financeira"].alias("instituicao_financeira"),
            col("raw_data")["Quantidade total de reclamações"]
            .cast("integer")
            .alias("soma_reclamacoes"),
            col("raw_data")["Ano"].alias("ano"),
            col("raw_data")["Trimestre"].alias("trimestre"),
        )
        .filter(col("cnpj_base").isNotNull() & (col("cnpj_base") != ""))
        .withColumn("created_at", current_timestamp())
    )

    # 6. Função para gravar cada lote (batch) no PostgreSQL via JDBC
    def write_batch_to_postgres(batch_df, batch_id):
        if batch_df.isEmpty():
            return

        print(f"[CONSUMER] Escrevendo lote {batch_id} no PostgreSQL ({batch_df.count()} registros)...")

        (
            batch_df.write.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", "delivery_atv4.delivery_reclamacoes_agregadas")
            .option("user", db_user)
            .option("password", db_pass)
            .option("driver", "org.postgresql.Driver")
            .mode("append")
            .save()
        )

    # 7. Escrita do Streaming usando foreachBatch
    checkpoint_path = "./data/checkpoints/postgres_complaints"

    query = (
        parsed_stream.writeStream.foreachBatch(write_batch_to_postgres)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="5 seconds")
        .start()
    )

    print("[CONSUMER] Aguardando mensagens do Kafka...")
    query.awaitTermination()


if __name__ == "__main__":
    main()