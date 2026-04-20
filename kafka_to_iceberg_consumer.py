"""
Kafka to Iceberg Consumer — Banking Data Pipeline (Step 2)
Reads JSON messages from Kafka topics using Spark Structured Streaming
and writes them into Iceberg Bronze tables on MinIO.

Kafka Topics → Iceberg Bronze Layer:
  finacle-transactions  →  lakehouse.bronze.finacle_transactions
  finacle-customers     →  lakehouse.bronze.finacle_customers
  aml-alerts            →  lakehouse.bronze.aml_alerts
  cibil-bureau          →  lakehouse.bronze.cibil_bureau
  npa-report            →  lakehouse.bronze.npa_report

Usage:
  spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1,\\
    org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 \\
    kafka_to_iceberg_consumer.py

  # Batch mode (process all available and stop):
  spark-submit ... kafka_to_iceberg_consumer.py --mode batch

  # Streaming mode (continuous):
  spark-submit ... kafka_to_iceberg_consumer.py --mode streaming
"""
import os
import sys
import argparse
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, LongType
)
from pyspark.sql.functions import (
    col, from_json, current_timestamp, lit, to_timestamp
)


# ============================================================
# SCHEMA DEFINITIONS — explicit schemas for each topic
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

# SCHEMA_CUSTOMERS = StructType([
#     StructField("customer_id", StringType()),
#     StructField("name", StringType()),
#     StructField("pan", StringType()),
#     StructField("aadhaar_masked", StringType()),
#     StructField("mobile", StringType()),
#     StructField("email", StringType()),
#     StructField("dob", StringType()),
#     StructField("gender", StringType()),
#     StructField("kyc_status", StringType()),
#     StructField("kyc_date", StringType()),
#     StructField("risk_category", StringType()),
#     StructField("branch_code", StringType()),
#     StructField("account_type", StringType()),
#     StructField("account_number", StringType()),
#     StructField("account_open_date", StringType()),
#     StructField("occupation", StringType()),
#     StructField("annual_income", StringType()),
#     StructField("address_city", StringType()),
#     StructField("address_state", StringType()),
#     StructField("nominee_name", StringType()),
# ])

# SCHEMA_AML_ALERTS = StructType([
#     StructField("alert_id", StringType()),
#     StructField("customer_id", StringType()),
#     StructField("customer_name", StringType()),
#     StructField("alert_date", StringType()),
#     StructField("alert_type", StringType()),
#     StructField("risk_score", StringType()),
#     StructField("rule_triggered", StringType()),
#     StructField("transaction_ids", StringType()),
#     StructField("total_amount", StringType()),
#     StructField("currency", StringType()),
#     StructField("description", StringType()),
#     StructField("status", StringType()),
#     StructField("assigned_to", StringType()),
#     StructField("priority", StringType()),
#     StructField("due_date", StringType()),
#     StructField("resolution", StringType()),
#     StructField("resolution_date", StringType()),
#     StructField("sar_filed", StringType()),
#     StructField("sar_reference", StringType()),
# ])

# SCHEMA_CIBIL = StructType([
#     StructField("report_id", StringType()),
#     StructField("customer_id", StringType()),
#     StructField("customer_name", StringType()),
#     StructField("pan", StringType()),
#     StructField("cibil_score", StringType()),
#     StructField("score_date", StringType()),
#     StructField("total_accounts", StringType()),
#     StructField("active_accounts", StringType()),
#     StructField("closed_accounts", StringType()),
#     StructField("overdue_accounts", StringType()),
#     StructField("total_outstanding", StringType()),
#     StructField("secured_outstanding", StringType()),
#     StructField("unsecured_outstanding", StringType()),
#     StructField("credit_utilization_pct", StringType()),
#     StructField("enquiry_count_6m", StringType()),
#     StructField("enquiry_count_12m", StringType()),
#     StructField("dpd_30_count", StringType()),
#     StructField("dpd_60_count", StringType()),
#     StructField("dpd_90_count", StringType()),
#     StructField("written_off_amount", StringType()),
#     StructField("suit_filed_count", StringType()),
#     StructField("wilful_defaulter", StringType()),
#     StructField("report_pull_date", StringType()),
#     StructField("report_source", StringType()),
# ])

# SCHEMA_NPA = StructType([
#     StructField("report_date", StringType()),
#     StructField("loan_id", StringType()),
#     StructField("customer_id", StringType()),
#     StructField("customer_name", StringType()),
#     StructField("loan_type", StringType()),
#     StructField("branch_code", StringType()),
#     StructField("sanctioned_amount", StringType()),
#     StructField("outstanding_principal", StringType()),
#     StructField("outstanding_interest", StringType()),
#     StructField("total_outstanding", StringType()),
#     StructField("dpd", StringType()),
#     StructField("asset_classification", StringType()),
#     StructField("npa_status", StringType()),
#     StructField("npa_date", StringType()),
#     StructField("provision_required_pct", StringType()),
#     StructField("provision_amount", StringType()),
#     StructField("collateral_type", StringType()),
#     StructField("collateral_value", StringType()),
#     StructField("net_npa_exposure", StringType()),
#     StructField("recovery_action", StringType()),
#     StructField("recovery_amount", StringType()),
#     StructField("last_review_date", StringType()),
#     StructField("next_review_date", StringType()),
#     StructField("relationship_manager", StringType()),
#     StructField("remarks", StringType()),
# ])

# Topic → (schema, iceberg_table, key_column)
TOPIC_CONFIG = {
    "finacle-transactions": (SCHEMA_TRANSACTIONS, "lakehouse.bronze.finacle_transactions", "txn_id")
    # "finacle-customers":    (SCHEMA_CUSTOMERS,    "lakehouse.bronze.finacle_customers",    "customer_id"),
    # "aml-alerts":           (SCHEMA_AML_ALERTS,   "lakehouse.bronze.aml_alerts",           "alert_id")
}


def get_kafka_options():
    """Build Kafka source options from environment."""
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "my-cluster-kafka-bootstrap:9092")
    protocol = os.environ.get("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
    mechanism = os.environ.get("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512")
    username = os.environ.get("KAFKA_USERNAME", "app-user")
    password = os.environ.get("KAFKA_PASSWORD", "bwDqbYGrgC2AKOMuthoUu7Ckkj8tNjtB")

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


        # .config("spark.sql.catalog.lakehouse.catalog-impl",
        #         "org.apache.iceberg.nessie.NessieCatalog") \
        # .config("spark.sql.catalog.lakehouse.uri", nessie_uri) \
        # .config("spark.sql.catalog.lakehouse.ref", nessie_ref) \

def create_spark_session():
    """Create SparkSession with Iceberg + MinIO configuration."""
    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio-api.lakehouse-data.svc.cluster.local:9000")
    minio_access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key = os.environ.get("MINIO_SECRET_KEY", "MyStr0ngP@ssw0rd123")
    warehouse_path = os.environ.get("ICEBERG_WAREHOUSE", "s3a://lakehouse-warehouse/warehouse")
    # nessie_uri = os.environ.get("NESSIE_URI", "http://nessie:19120/api/v2")
    # nessie_ref = os.environ.get("NESSIE_REF", "main")

    spark = SparkSession.builder \
        .appName("KafkaToIcebergConsumer") \
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                "org.projectnessie.spark.extensions.NessieSparkSessionExtensions") \
        .config("spark.sql.catalog.lakehouse",
                "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.lakehouse.warehouse", warehouse_path) \
        .config("spark.sql.catalog.lakehouse.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.lakehouse.s3.endpoint", minio_endpoint) \
        .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true") \
        .config("spark.sql.catalog.lakehouse.s3.access-key-id", minio_access_key) \
        .config("spark.sql.catalog.lakehouse.s3.secret-access-key", minio_secret_key) \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.defaultCatalog", "lakehouse") \
        .getOrCreate()

    # Create schemas if they don't exist
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")

    return spark


def process_batch(spark, kafka_options, topic, schema, iceberg_table, key_column):
    """Read all available messages from a Kafka topic and write to Iceberg (batch mode)."""
    print(f"\n--- Processing: {topic} → {iceberg_table} ---")

    # Read from Kafka
    df_raw = spark.read \
        .format("kafka") \
        .options(**kafka_options) \
        .option("subscribe", topic) \
        .load()

    if df_raw.count() == 0:
        print(f"  No messages found in topic '{topic}'")
        return 0

    # Parse JSON value
    df_parsed = df_raw \
        .selectExpr("CAST(key AS STRING) as kafka_key", "CAST(value AS STRING) as json_value") \
        .select(
            col("kafka_key"),
            from_json(col("json_value"), schema).alias("data")
        ) \
        .select("data.*") \
        .withColumn("_ingestion_ts", current_timestamp()) \
        .withColumn("_source", lit("kafka"))

    row_count = df_parsed.count()
    print(f"  Parsed {row_count} messages")

    # Write to Iceberg Bronze table
    df_parsed.writeTo(iceberg_table) \
        .tableProperty("format-version", "2") \
        .createOrReplace()

    print(f"  Written to {iceberg_table}: {row_count} rows")
    return row_count


def process_streaming(spark, kafka_options, topic, schema, iceberg_table, key_column, checkpoint_dir):
    """Start a streaming query that continuously reads from Kafka and writes to Iceberg."""
    print(f"\n--- Streaming: {topic} → {iceberg_table} ---")

    df_stream = spark.readStream \
        .format("kafka") \
        .options(**kafka_options) \
        .option("subscribe", topic) \
        .load()

    df_parsed = df_stream \
        .selectExpr("CAST(key AS STRING) as kafka_key", "CAST(value AS STRING) as json_value") \
        .select(
            col("kafka_key"),
            from_json(col("json_value"), schema).alias("data")
        ) \
        .select("data.*") \
        .withColumn("_ingestion_ts", current_timestamp()) \
        .withColumn("_source", lit("kafka"))

    checkpoint = os.path.join(checkpoint_dir, topic)

    query = df_parsed.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint) \
        .option("fanout-enabled", "true") \
        .toTable(iceberg_table)

    return query


def main():
    parser = argparse.ArgumentParser(description="Kafka to Iceberg Consumer — Banking Data Pipeline")
    parser.add_argument("--mode", choices=["batch", "streaming"], default="batch",
                        help="batch = process all available then stop; streaming = continuous")
    parser.add_argument("--topic", default=None,
                        help="Only process a specific topic (e.g., finacle-transactions)")
    parser.add_argument("--checkpoint-dir", default="/tmp/kafka-iceberg-checkpoints",
                        help="Checkpoint directory for streaming mode")
    args = parser.parse_args()

    print("=" * 60)
    print(f"Kafka → Iceberg Consumer — {args.mode.upper()} Mode")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print("=" * 60)

    spark = create_spark_session()
    kafka_options = get_kafka_options()

    if args.mode == "batch":
        total_rows = 0
        for topic, (schema, table, key_col) in TOPIC_CONFIG.items():
            if args.topic and topic != args.topic:
                continue
            count = process_batch(spark, kafka_options, topic, schema, table, key_col)
            total_rows += count

        print(f"\n{'=' * 60}")
        print(f"BATCH COMPLETE — {total_rows} total rows written to Iceberg Bronze")
        print("=" * 60)

        # Verify row counts
        print("\n--- Verification ---")
        for topic, (schema, table, key_col) in TOPIC_CONFIG.items():
            if args.topic and topic != args.topic:
                continue
            try:
                count = spark.table(table).count()
                print(f"  {table}: {count} rows")
            except Exception:
                print(f"  {table}: table not yet created")

        spark.stop()

    elif args.mode == "streaming":
        queries = []
        for topic, (schema, table, key_col) in TOPIC_CONFIG.items():
            if args.topic and topic != args.topic:
                continue
            q = process_streaming(spark, kafka_options, topic, schema, table, key_col, args.checkpoint_dir)
            queries.append(q)
            print(f"  Started streaming query for {topic}")

        print(f"\n{'=' * 60}")
        print(f"STREAMING — {len(queries)} queries running. Press Ctrl+C to stop.")
        print("=" * 60)

        # Wait for all queries
        for q in queries:
            q.awaitTermination()


if __name__ == "__main__":
    main()
