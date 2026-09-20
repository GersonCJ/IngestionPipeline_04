import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, sum as _sum, count
from pyspark.sql.types import StructType, StructField, StringType, MapType

# Variáveis de ambiente e banco de dados
db_host = os.getenv("TARGET_DB_HOST", "postgres-db")
db_port = os.getenv("TARGET_DB_PORT", "5432")
db_name = os.getenv("TARGET_DB_NAME", "atv4")
db_user = os.getenv("TARGET_DB_USER", "postgres")
db_pass = os.getenv("TARGET_DB_PASS", "postgres")
JDBC_URL = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"
KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("KafkaProducerStream")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0")
        .getOrCreate()
    )

# ------------------------------------------------------------------------------
# STREAM 1: RAW (Kafka) -> TRUSTED (Parquet)
# ------------------------------------------------------------------------------
def start_raw_to_trusted_stream(spark: SparkSession):
    raw_schema = StructType([
        StructField("dataset_type", StringType(), True),
        StructField("file_name", StringType(), True),
        StructField("raw_data", MapType(StringType(), StringType()), True),
    ])

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVER)
        .option("subscribe", "raw-events")
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed_df = (
        raw_stream.selectExpr("CAST(value AS STRING) as json_payload")
        .select(from_json(col("json_payload"), raw_schema).alias("data"))
        .select("data.*")
    )

    trusted_complaints = (
        parsed_df.filter(col("dataset_type") == "complaints")
        .select(
            col("raw_data")["Ano"].alias("ano"),
            col("raw_data")["Trimestre"].alias("trimestre"),
            col("raw_data")["CNPJ IF"].alias("cnpj_base"),
            col("raw_data")["Instituição financeira"].alias("instituicao_financeira"),
            col("raw_data")["Quantidade total de reclamações"]
            .cast("int")
            .alias("qtd_recl_total"),
        )
        .filter(col("cnpj_base").isNotNull() & (col("cnpj_base") != ""))
    )

    return (
        trusted_complaints.writeStream.format("parquet")
        .option("path", "./data/trusted/complaints/")
        .option("checkpointLocation", "./checkpoints/trusted_complaints/")
        .outputMode("append")
        .start()
    )


# ------------------------------------------------------------------------------
# STREAM 2: TRUSTED (Parquet) -> DELIVERY (Postgres + Parquet Gold)
# ------------------------------------------------------------------------------
def start_trusted_to_delivery_stream(spark: SparkSession):
    trusted_schema = StructType([
        StructField("ano", StringType(), True),
        StructField("trimestre", StringType(), True),
        StructField("cnpj_base", StringType(), True),
        StructField("instituicao_financeira", StringType(), True),
        StructField("qtd_recl_total", StringType(), True),
    ])

    trusted_stream = spark.readStream.schema(trusted_schema).parquet(
        "./data/trusted/complaints/"
    )

    delivery_df = (
        trusted_stream.withColumn("created_at", current_timestamp())
        .withWatermark("created_at", "10 minutes")
        .groupBy("cnpj_base", "instituicao_financeira")
        .agg(
            count("qtd_recl_total").alias("total_ocorrencias"),
            _sum(col("qtd_recl_total").cast("int")).alias("soma_reclamacoes"),
        )
    )

    def write_delivery_sinks(batch_df, batch_id):
        if batch_df.isEmpty():
            return

        # Corrigido: uso das variáveis db_user e db_pass
        (
            batch_df.write.format("jdbc")
            .option("url", JDBC_URL)
            .option("dbtable", "delivery_atv4.delivery_reclamacoes_agregadas")
            .option("user", db_user)
            .option("password", db_pass)
            .option("driver", "org.postgresql.Driver")
            .mode("append")
            .save()
        )

        (
            batch_df.write.mode("append").parquet(
                "./data/delivery/reclamacoes_agregadas/"
            )
        )

    return (
        delivery_df.writeStream.foreachBatch(write_delivery_sinks)
        .option("checkpointLocation", "./checkpoints/delivery_complaints/")
        .outputMode("update")
        .start()
    )


if __name__ == "__main__":
    spark = get_spark()

    print("[SPARK] Iniciando Stream 1: RAW -> TRUSTED")
    query_trusted = start_raw_to_trusted_stream(spark)

    print("[SPARK] Iniciando Stream 2: TRUSTED -> DELIVERY")
    query_delivery = start_trusted_to_delivery_stream(spark)

    spark.streams.awaitAnyTermination()