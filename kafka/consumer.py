import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

def main():
    # 1. Variáveis de ambiente
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    db_host = os.getenv("TARGET_DB_HOST", "postgres-db")
    db_port = os.getenv("TARGET_DB_PORT", "5432")
    db_name = os.getenv("TARGET_DB_NAME", "atv4")
    db_user = os.getenv("TARGET_DB_USER", "postgres")
    db_pass = os.getenv("TARGET_DB_PASS", "postgres")

    # 2. Inicialização da SparkSession (versão compatível 3.5.0 / Scala 2.12)
    spark = (
        SparkSession.builder
        .appName("PySparkStructuredStreamingEnrichment")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("id_transacao", StringType(), True),
        StructField("id_cliente", IntegerType(), True),
        StructField("valor", DoubleType(), True)
    ])

    # 3. Ingestão da Stream de dados do Kafka
    kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("subscribe", "dados-raw-topic") \
        .option("startingOffsets", "earliest") \
        .load()

    parsed_stream = kafka_stream \
        .selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), schema).alias("data")) \
        .select("data.*")

    # 4. Leitura Estática do Banco PostgreSQL
    jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"
    
    clientes_df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "public.clientes") \
        .option("user", db_user) \
        .option("password", db_pass) \
        .option("driver", "org.postgresql.Driver") \
        .load()

    # 5. Enriquecimento via Join
    enriched_stream = parsed_stream.join(
        clientes_df, 
        parsed_stream.id_cliente == clientes_df.id, 
        "left"
    ).select(
        parsed_stream["id_transacao"],
        parsed_stream["id_cliente"],
        parsed_stream["valor"],
        clientes_df["nome"].alias("nome_cliente"),
        clientes_df["email"].alias("email_cliente")
    )

    # 6. Escrita do Streaming
    output_path = "./data/trusted_parquet/kafka_enriched"
    checkpoint_path = "./data/checkpoints/kafka_enriched"

    query = enriched_stream.writeStream \
        .format("parquet") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_path) \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()