"""
================================================================
 kafka_consumer.py — Kafka → Iceberg (Nessie + MinIO)
================================================================
"""
import os
import argparse
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, from_json, current_timestamp, lit


# ============================================================
# SCHEMA — one per Kafka topic
# ============================================================
SCHEMA_TRANSACTIONS = StructType([
    StructField("txn_id", StringType()),
    StructField("account_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("txn_type", StringType()),
    StructField("amount", StringType()),
    StructField("currency", StringType()),
    StructField("txn_date", StringType()),
    StructField("value_date", StringType()),
    StructField("branch_code", StringType()),
    StructField("channel", StringType()),
    StructField("narration", StringType()),
    StructField("balance_after", StringType()),
    StructField("status", StringType()),
    StructField("ref_number", StringType()),
])

# Topic → (schema, iceberg_table)
TOPIC_CONFIG = {
    "finacle-transactions": (SCHEMA_TRANSACTIONS, "lakehouse.bronze.finacle_transactions"),
}


# ============================================================
# KAFKA OPTIONS
# ============================================================
def get_kafka_options():
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "my-cluster-kafka-bootstrap:9092")
    protocol = os.environ.get("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
    mechanism = os.environ.get("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512")
    username = os.environ.get("KAFKA_USERNAME", "app-user")
    password = os.environ.get("KAFKA_PASSWORD", "")

    jaas_config = (
        f'org.apache.kafka.common.security.scram.ScramLoginModule required '
        f'username="{username}" password="{password}";'
    )

    return {
        "kafka.bootstrap.servers": bootstrap,
        "kafka.security.protocol": protocol,
        "kafka.sasl.mechanism": mechanism,
        "kafka.sasl.jaas.config": jaas_config,
        "startingOffsets": "earliest",
        "failOnDataLoss": "false",
    }


# ============================================================
# SPARK SESSION
# ============================================================
def create_spark_session():
    minio_endpoint = os.environ.get("S3_ENDPOINT",
                                    "http://minio-api.lakehouse-data.svc.cluster.local:9000")
    minio_access_key = os.environ.get("S3_ACCESS_KEY", "minioadmin")
    minio_secret_key = os.environ.get("S3_SECRET_KEY", "MyStr0ngP@ssw0rd123")
    warehouse_path = os.environ.get("ICEBERG_WAREHOUSE", "s3a://lakehouse-warehouse/warehouse")
    nessie_uri = os.environ.get("NESSIE_URI", "http://nessie.lakehouse-catalog.svc:19120/api/v2")
    nessie_ref = os.environ.get("NESSIE_REF", "main")

    print(f"Spark config:")
    print(f"  Nessie:    {nessie_uri}")
    print(f"  MinIO:     {minio_endpoint}")
    print(f"  Warehouse: {warehouse_path}")

    spark = (SparkSession.builder
        .appName("KafkaToIcebergConsumer")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse",
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.catalog-impl",
                "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.lakehouse.uri", nessie_uri)
        .config("spark.sql.catalog.lakehouse.ref", nessie_ref)
        .config("spark.sql.catalog.lakehouse.warehouse", warehouse_path)
        .config("spark.sql.catalog.lakehouse.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.lakehouse.s3.endpoint", minio_endpoint)
        .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
        .config("spark.sql.catalog.lakehouse.s3.access-key-id", minio_access_key)
        .config("spark.sql.catalog.lakehouse.s3.secret-access-key", minio_secret_key)
        .config("spark.sql.catalog.lakehouse.s3.region", "us-east-1")
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.defaultCatalog", "lakehouse")
        .getOrCreate()
    )

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")

    return spark


# ============================================================
# BATCH PROCESSING
# ============================================================
def process_batch(spark, kafka_options, topic, schema, iceberg_table) -> int:
    print(f"\nProcessing: {topic} → {iceberg_table}")

    df_raw = (
        spark.read
        .format("kafka")
        .options(**kafka_options)
        .option("subscribe", topic)
        .load()
    )

    msg_count = df_raw.count()
    if msg_count == 0:
        print(f"No messages in '{topic}'")
        return 0

    print(f"Found {msg_count} messages")

    df_parsed = (
        df_raw
        .selectExpr("CAST(key AS STRING) as kafka_key",
                    "CAST(value AS STRING) as json_value")
        .select(
            col("kafka_key"),
            from_json(col("json_value"), schema).alias("data")
        )
        .select("data.*")
        .withColumn("_ingestion_ts", current_timestamp())
        .withColumn("_source", lit("kafka"))
    )

    row_count = df_parsed.count()
    print(f"Parsed {row_count} rows")

    # Check if table exists
    try:
        table_exists = spark.catalog.tableExists(iceberg_table)
        print(f"Table exists: {table_exists}")
    except Exception as e:
        print(f"Existence check failed: {e}")
        table_exists = False

    if not table_exists:
        print(f"Creating new table: {iceberg_table}")
        (
            df_parsed.writeTo(iceberg_table)
            .tableProperty("format-version", "2")
            .tableProperty("write.format.default", "parquet")
            .tableProperty("write.parquet.compression-codec", "snappy")
            .create()
        )
        print(f"✓ Table created and registered in Nessie")
    else:
        print(f"Appending to existing table: {iceberg_table}")
        df_parsed.writeTo(iceberg_table).append()
        print(f"✓ Appended to existing Nessie-registered table")

    # Verify
    try:
        verify_count = spark.table(iceberg_table).count()
        print(f"✓ Nessie verified — table has {verify_count} total rows")
    except Exception as e:
        print(f"✗ Nessie verification FAILED: {e}")

    return row_count


# ============================================================
# STREAMING PROCESSING
# ============================================================
def process_streaming(spark, kafka_options, topic, schema, iceberg_table, checkpoint_dir):
    print(f"\n--- Streaming: {topic} → {iceberg_table} ---")

    df_stream = (spark.readStream
        .format("kafka")
        .options(**kafka_options)
        .option("subscribe", topic)
        .load()
    )

    df_parsed = (df_stream
        .selectExpr("CAST(key AS STRING) as kafka_key",
                    "CAST(value AS STRING) as json_value")
        .select(
            col("kafka_key"),
            from_json(col("json_value"), schema).alias("data")
        )
        .select("data.*")
        .withColumn("_ingestion_ts", current_timestamp())
        .withColumn("_source", lit("kafka"))
    )

    checkpoint = os.path.join(checkpoint_dir, topic)

    query = (df_parsed.writeStream
        .format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .option("fanout-enabled", "true")
        .toTable(iceberg_table)
    )
    return query


# ============================================================
# MAIN
# ============================================================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["batch", "streaming"], default="batch")
    parser.add_argument("--topic", default=None)
    parser.add_argument("--checkpoint-dir", default="/tmp/kafka-iceberg-checkpoints")
    args = parser.parse_args()

    print("=" * 60)
    print(f"Kafka → Iceberg Consumer — {args.mode.upper()} Mode")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print("=" * 60)

    spark = create_spark_session()
    kafka_options = get_kafka_options()

    if args.mode == "batch":
        total_rows = 0
        for topic, (schema, table) in TOPIC_CONFIG.items():
            if args.topic and topic != args.topic:
                continue
            count = process_batch(spark, kafka_options, topic, schema, table)
            total_rows += count

        print(f"\n{'=' * 60}")
        print(f"BATCH COMPLETE — {total_rows} total rows")
        print("=" * 60)

        print("\n--- Verification ---")
        for topic, (schema, table) in TOPIC_CONFIG.items():
            if args.topic and topic != args.topic:
                continue
            try:
                count = spark.table(table).count()
                print(f"  {table}: {count} rows")
            except Exception as e:
                print(f"  {table}: {e}")

        spark.stop()

    elif args.mode == "streaming":
        queries = []
        for topic, (schema, table) in TOPIC_CONFIG.items():
            if args.topic and topic != args.topic:
                continue
            q = process_streaming(spark, kafka_options, topic, schema, table, args.checkpoint_dir)
            queries.append(q)

        print(f"\n{'=' * 60}")
        print(f"STREAMING — {len(queries)} queries running")
        print("=" * 60)

        for q in queries:
            q.awaitTermination()


if __name__ == "__main__":
    main()