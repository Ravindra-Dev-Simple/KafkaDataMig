"""
Iceberg Silver/Gold Layer Transformations — Banking Data Lakehouse (Step 3)
Reads from Bronze Iceberg tables (populated by kafka_to_iceberg_consumer.py)
and transforms into Silver (cleansed) and Gold (aggregated) layers.

Pipeline: CSV → Kafka → Iceberg Bronze → Silver → Gold
                                          ^^^^^^^^^^^^^^^^
                                          (this script)

Usage:
  spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1 \\
    ingest_test.py

  # Skip Gold layer:
  spark-submit ... ingest_test.py --skip-gold
"""
import os
import sys
import argparse
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, current_timestamp, date_format, hour,
    when, lit, count, sum, avg, max, min, countDistinct,
    concat, substring, substring_index, round as spark_round
)


def create_spark_session():
    """Create SparkSession with Iceberg + MinIO configuration."""
    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio.lakehouse-data.svc.cluster.local:9000")
    minio_access_key = os.environ.get("MINIO_ACCESS_KEY", "admin")
    minio_secret_key = os.environ.get("MINIO_SECRET_KEY", "")
    warehouse_path = os.environ.get("ICEBERG_WAREHOUSE", "s3a://lakehouse/warehouse")

    spark = SparkSession.builder \
        .appName("BankingLakehouseSilverGold") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.lakehouse.type", "hadoop") \
        .config("spark.sql.catalog.lakehouse.warehouse", warehouse_path) \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.defaultCatalog", "lakehouse") \
        .getOrCreate()

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")

    return spark


def check_bronze_tables(spark):
    """Verify Bronze tables exist and have data."""
    required = [
        "lakehouse.bronze.finacle_transactions",
        "lakehouse.bronze.finacle_customers",
        "lakehouse.bronze.npa_report",
    ]
    for table in required:
        try:
            count = spark.table(table).count()
            if count == 0:
                print(f"WARNING: {table} exists but is empty")
            else:
                print(f"  {table}: {count} rows")
        except Exception as e:
            print(f"ERROR: {table} not found — run kafka_to_iceberg_consumer.py first")
            print(f"  {e}")
            return False
    return True


# ============================================================
# SILVER LAYER — Cleansed, typed, deduplicated
# ============================================================

def build_silver_transactions(spark):
    """Bronze → Silver transactions: cast types, add derived columns, dedup."""
    print("\n--- Silver: Transactions ---")
    df = spark.table("lakehouse.bronze.finacle_transactions") \
        .withColumn("txn_date", to_timestamp("txn_date")) \
        .withColumn("amount", col("amount").cast("decimal(18,2)")) \
        .withColumn("balance_after", col("balance_after").cast("decimal(18,2)")) \
        .withColumn("ingestion_ts", current_timestamp()) \
        .withColumn("txn_month", date_format("txn_date", "yyyy-MM")) \
        .withColumn("txn_hour", hour("txn_date")) \
        .withColumn("is_high_value", when(col("amount") >= 200000, True).otherwise(False)) \
        .withColumn("is_flagged", col("status") == "FLAGGED") \
        .filter(col("txn_id").isNotNull()) \
        .dropDuplicates(["txn_id"])

    df.writeTo("lakehouse.silver.finacle_transactions") \
        .tableProperty("format-version", "2") \
        .partitionedBy("txn_month") \
        .createOrReplace()

    row_count = df.count()
    print(f"  Written: {row_count} rows (partitioned by txn_month)")
    return df


def build_silver_customers(spark):
    """Bronze → Silver customers: mask PII fields."""
    print("\n--- Silver: Customers (PII masked) ---")
    df = spark.table("lakehouse.bronze.finacle_customers") \
        .withColumn("pan_masked", concat(lit("XXXXX"), substring("pan", 6, 5))) \
        .withColumn("mobile_masked", concat(lit("XXXXXX"), substring("mobile", 7, 4))) \
        .withColumn("email_masked",
            concat(lit("***@"), substring_index("email", "@", -1))) \
        .withColumn("annual_income", col("annual_income").cast("decimal(18,2)")) \
        .withColumn("ingestion_ts", current_timestamp()) \
        .drop("pan", "mobile", "email") \
        .withColumnRenamed("pan_masked", "pan") \
        .withColumnRenamed("mobile_masked", "mobile") \
        .withColumnRenamed("email_masked", "email")

    df.writeTo("lakehouse.silver.finacle_customers") \
        .tableProperty("format-version", "2") \
        .createOrReplace()

    row_count = df.count()
    print(f"  Written: {row_count} rows")
    return df


def build_silver_loans(spark):
    """Bronze → Silver NPA/loans: cast amounts, compute LTV ratio."""
    print("\n--- Silver: Loans/NPA ---")
    df = spark.table("lakehouse.bronze.npa_report") \
        .withColumn("sanctioned_amount", col("sanctioned_amount").cast("decimal(18,2)")) \
        .withColumn("outstanding_principal", col("outstanding_principal").cast("decimal(18,2)")) \
        .withColumn("outstanding_interest", col("outstanding_interest").cast("decimal(18,2)")) \
        .withColumn("total_outstanding", col("total_outstanding").cast("decimal(18,2)")) \
        .withColumn("collateral_value", col("collateral_value").cast("decimal(18,2)")) \
        .withColumn("provision_amount", col("provision_amount").cast("decimal(18,2)")) \
        .withColumn("dpd", col("dpd").cast("int")) \
        .withColumn("interest_rate", col("provision_required_pct").cast("decimal(5,2)")) \
        .withColumn("ltv_ratio",
            when(col("collateral_value") > 0,
                 spark_round(col("outstanding_principal") / col("collateral_value") * 100, 2))
            .otherwise(lit(None))) \
        .withColumn("ingestion_ts", current_timestamp())

    df.writeTo("lakehouse.silver.lms_loans") \
        .tableProperty("format-version", "2") \
        .createOrReplace()

    row_count = df.count()
    print(f"  Written: {row_count} rows")
    return df


def build_silver_cibil(spark):
    """Bronze → Silver CIBIL: cast score and amounts."""
    print("\n--- Silver: CIBIL Bureau ---")
    try:
        df = spark.table("lakehouse.bronze.cibil_bureau") \
            .withColumn("cibil_score", col("cibil_score").cast("int")) \
            .withColumn("total_outstanding", col("total_outstanding").cast("decimal(18,2)")) \
            .withColumn("secured_outstanding", col("secured_outstanding").cast("decimal(18,2)")) \
            .withColumn("unsecured_outstanding", col("unsecured_outstanding").cast("decimal(18,2)")) \
            .withColumn("credit_utilization_pct", col("credit_utilization_pct").cast("int")) \
            .withColumn("written_off_amount", col("written_off_amount").cast("decimal(18,2)")) \
            .withColumn("ingestion_ts", current_timestamp())

        df.writeTo("lakehouse.silver.cibil_bureau") \
            .tableProperty("format-version", "2") \
            .createOrReplace()

        row_count = df.count()
        print(f"  Written: {row_count} rows")
        return df
    except Exception as e:
        print(f"  SKIP: cibil_bureau bronze table not found ({e})")
        return None


def build_silver_aml(spark):
    """Bronze → Silver AML alerts: cast scores and amounts."""
    print("\n--- Silver: AML Alerts ---")
    try:
        df = spark.table("lakehouse.bronze.aml_alerts") \
            .withColumn("risk_score", col("risk_score").cast("int")) \
            .withColumn("total_amount", col("total_amount").cast("decimal(18,2)")) \
            .withColumn("ingestion_ts", current_timestamp())

        df.writeTo("lakehouse.silver.aml_alerts") \
            .tableProperty("format-version", "2") \
            .createOrReplace()

        row_count = df.count()
        print(f"  Written: {row_count} rows")
        return df
    except Exception as e:
        print(f"  SKIP: aml_alerts bronze table not found ({e})")
        return None


# ============================================================
# GOLD LAYER — Aggregated, business-ready
# ============================================================

def build_gold_branch_summary(df_txn_silver):
    """Gold: Branch-level transaction summary."""
    print("\n--- Gold: Branch Transaction Summary ---")
    df = df_txn_silver \
        .groupBy("branch_code", "txn_type", "channel", "txn_month") \
        .agg(
            count("*").alias("txn_count"),
            sum("amount").alias("total_amount"),
            avg("amount").alias("avg_amount"),
            max("amount").alias("max_amount"),
            countDistinct("customer_id").alias("unique_customers"),
            sum(when(col("is_flagged"), 1).otherwise(0)).alias("flagged_count")
        )

    df.writeTo("lakehouse.gold.branch_txn_summary") \
        .tableProperty("format-version", "2") \
        .createOrReplace()
    print(f"  Written: {df.count()} rows")
    df.show(5, truncate=False)


def build_gold_npa_summary(df_loan_silver):
    """Gold: NPA classification summary (RBI format)."""
    print("\n--- Gold: NPA Classification Summary ---")
    df = df_loan_silver \
        .groupBy("npa_status", "loan_type", "branch_code") \
        .agg(
            count("*").alias("account_count"),
            sum("outstanding_principal").alias("total_outstanding"),
            sum("sanctioned_amount").alias("total_sanctioned"),
            avg("interest_rate").alias("avg_interest_rate"),
            avg("dpd").alias("avg_dpd")
        )

    df.writeTo("lakehouse.gold.npa_classification_summary") \
        .tableProperty("format-version", "2") \
        .createOrReplace()
    print(f"  Written: {df.count()} rows")
    df.show(5, truncate=False)


def build_gold_customer_360(spark, df_cust_silver, df_txn_silver, df_loan_silver):
    """Gold: Customer 360 view joining transactions + loans."""
    print("\n--- Gold: Customer 360 ---")
    df = df_cust_silver.alias("c") \
        .join(
            df_txn_silver.groupBy("customer_id").agg(
                count("*").alias("total_txns"),
                sum("amount").alias("total_txn_amount"),
                max("txn_date").alias("last_txn_date"),
                sum(when(col("is_flagged"), 1).otherwise(0)).alias("flagged_txns")
            ).alias("t"),
            col("c.customer_id") == col("t.customer_id"),
            "left"
        ) \
        .join(
            df_loan_silver.groupBy("customer_id").agg(
                count("*").alias("total_loans"),
                sum("outstanding_principal").alias("total_loan_outstanding"),
                max("dpd").alias("max_dpd"),
                max("npa_status").alias("worst_npa_status")
            ).alias("l"),
            col("c.customer_id") == col("l.customer_id"),
            "left"
        ) \
        .select(
            col("c.customer_id"),
            col("c.name"),
            col("c.risk_category"),
            col("c.kyc_status"),
            col("c.branch_code"),
            col("t.total_txns"),
            col("t.total_txn_amount"),
            col("t.last_txn_date"),
            col("t.flagged_txns"),
            col("l.total_loans"),
            col("l.total_loan_outstanding"),
            col("l.max_dpd"),
            col("l.worst_npa_status")
        )

    df.writeTo("lakehouse.gold.customer_360") \
        .tableProperty("format-version", "2") \
        .createOrReplace()
    print(f"  Written: {df.count()} rows")
    df.show(5, truncate=False)


# ============================================================
# VERIFICATION
# ============================================================

def verify_layers(spark):
    """Print row counts and Iceberg metadata."""
    print("\n" + "=" * 60)
    print("VERIFICATION — Layer Row Counts")
    print("=" * 60)
    for layer in ["bronze", "silver", "gold"]:
        try:
            tables = spark.sql(f"SHOW TABLES IN lakehouse.{layer}").collect()
            for t in tables:
                try:
                    c = spark.table(f"lakehouse.{layer}.{t.tableName}").count()
                    print(f"  {layer}.{t.tableName}: {c} rows")
                except Exception:
                    print(f"  {layer}.{t.tableName}: error reading")
        except Exception:
            print(f"  {layer}: namespace not found")

    # Iceberg snapshot history for Silver transactions
    print("\n--- Snapshots (Silver Transactions) ---")
    try:
        spark.sql("SELECT snapshot_id, committed_at, operation FROM lakehouse.silver.finacle_transactions.snapshots").show(truncate=False)
    except Exception:
        print("  No snapshots available yet")


def main():
    parser = argparse.ArgumentParser(description="Silver/Gold Layer Transformations")
    parser.add_argument("--skip-gold", action="store_true", help="Only build Silver layer")
    parser.add_argument("--verify-only", action="store_true", help="Only run verification")
    args = parser.parse_args()

    print("=" * 60)
    print("Banking Lakehouse — Silver/Gold Transformations")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print("=" * 60)

    spark = create_spark_session()

    if args.verify_only:
        verify_layers(spark)
        spark.stop()
        return

    # Check Bronze tables exist
    print("\n--- Checking Bronze Layer ---")
    if not check_bronze_tables(spark):
        print("\nERROR: Bronze layer is incomplete.")
        print("Run the pipeline first:")
        print("  1. python csv_to_kafka_producer.py")
        print("  2. spark-submit kafka_to_iceberg_consumer.py")
        spark.stop()
        sys.exit(1)

    # Build Silver Layer
    print("\n" + "=" * 60)
    print("PHASE 1: SILVER LAYER — Cleanse & Transform")
    print("=" * 60)

    df_txn_silver = build_silver_transactions(spark)
    df_cust_silver = build_silver_customers(spark)
    df_loan_silver = build_silver_loans(spark)
    build_silver_cibil(spark)
    build_silver_aml(spark)

    # Build Gold Layer
    if not args.skip_gold:
        print("\n" + "=" * 60)
        print("PHASE 2: GOLD LAYER — Business Aggregations")
        print("=" * 60)

        build_gold_branch_summary(df_txn_silver)
        build_gold_npa_summary(df_loan_silver)
        build_gold_customer_360(spark, df_cust_silver, df_txn_silver, df_loan_silver)

    # Verify
    verify_layers(spark)

    print("\n" + "=" * 60)
    print("ALL LAYERS COMPLETE")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
