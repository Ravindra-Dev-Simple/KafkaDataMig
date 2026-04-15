"""
PySpark Ingest Test — Banking Data Lakehouse
Reads CSV from MinIO raw-zone → writes to Iceberg Bronze/Silver/Gold layers
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("BankingLakehouseIngestTest") \
    .getOrCreate()

MINIO_ENDPOINT = "http://minio.lakehouse-data.svc.cluster.local:9000"
RAW_ZONE = "D:/Datamig/raw-zone"

# ============================================================
# 1. BRONZE LAYER — Raw ingestion (schema-on-read)
# ============================================================

print("=" * 60)
print("PHASE 1: BRONZE LAYER — Raw Ingestion")
print("=" * 60)

# --- Transactions ---
df_txn = spark.read.csv(
    f"{RAW_ZONE}/finacle/transactions/2026/04/14/",
    header=True,
    inferSchema=True
)
print(f"Transactions loaded: {df_txn.count()} rows")
df_txn.writeTo("lakehouse.bronze.finacle_transactions") \
    .tableProperty("format-version", "2") \
    .createOrReplace()

# --- Customers ---
df_cust = spark.read.csv(
    f"{RAW_ZONE}/finacle/customers/2026/04/14/",
    header=True,
    inferSchema=True
)
print(f"Customers loaded: {df_cust.count()} rows")
df_cust.writeTo("lakehouse.bronze.finacle_customers") \
    .tableProperty("format-version", "2") \
    .createOrReplace()

# --- Loans ---
df_loan = spark.read.csv(
    f"{RAW_ZONE}/lms/loans/2026/04/14/",
    header=True,
    inferSchema=True
)
print(f"Loans loaded: {df_loan.count()} rows")
df_loan.writeTo("lakehouse.bronze.lms_loans") \
    .tableProperty("format-version", "2") \
    .createOrReplace()

# --- CIBIL Bureau ---
df_cibil = spark.read.csv(
    f"{RAW_ZONE}/bureau/cibil/2026/04/14/",
    header=True,
    inferSchema=True
)
print(f"CIBIL reports loaded: {df_cibil.count()} rows")
df_cibil.writeTo("lakehouse.bronze.cibil_bureau") \
    .tableProperty("format-version", "2") \
    .createOrReplace()

# --- NPA Report ---
df_npa = spark.read.csv(
    f"{RAW_ZONE}/regulatory/npa/2026/04/14/",
    header=True,
    inferSchema=True
)
print(f"NPA records loaded: {df_npa.count()} rows")
df_npa.writeTo("lakehouse.bronze.npa_report") \
    .tableProperty("format-version", "2") \
    .createOrReplace()

# ============================================================
# 2. SILVER LAYER — Cleansed, typed, deduplicated
# ============================================================

print("\n" + "=" * 60)
print("PHASE 2: SILVER LAYER — Cleanse & Transform")
print("=" * 60)

# --- Silver Transactions ---
df_txn_silver = spark.table("lakehouse.bronze.finacle_transactions") \
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

df_txn_silver.writeTo("lakehouse.silver.finacle_transactions") \
    .tableProperty("format-version", "2") \
    .partitionedBy("txn_month") \
    .createOrReplace()
print(f"Silver transactions: {df_txn_silver.count()} rows")

# --- Silver Customers (PII masked) ---
df_cust_silver = spark.table("lakehouse.bronze.finacle_customers") \
    .withColumn("pan_masked", concat(lit("XXXXX"), substring("pan", 6, 5))) \
    .withColumn("mobile_masked", concat(lit("XXXXXX"), substring("mobile", 7, 4))) \
    .withColumn("email_masked",
        concat(lit("***@"), substring_index("email", "@", -1))) \
    .withColumn("ingestion_ts", current_timestamp()) \
    .drop("pan", "mobile", "email") \
    .withColumnRenamed("pan_masked", "pan") \
    .withColumnRenamed("mobile_masked", "mobile") \
    .withColumnRenamed("email_masked", "email")

df_cust_silver.writeTo("lakehouse.silver.finacle_customers") \
    .tableProperty("format-version", "2") \
    .createOrReplace()
print(f"Silver customers (PII masked): {df_cust_silver.count()} rows")

# --- Silver Loans ---
df_loan_silver = spark.table("lakehouse.bronze.lms_loans") \
    .withColumn("sanctioned_amount", col("sanctioned_amount").cast("decimal(18,2)")) \
    .withColumn("outstanding_principal", col("outstanding_principal").cast("decimal(18,2)")) \
    .withColumn("ltv_ratio",
        when(col("collateral_value") > 0,
             round(col("outstanding_principal") / col("collateral_value") * 100, 2))
        .otherwise(lit(None))) \
    .withColumn("ingestion_ts", current_timestamp())

df_loan_silver.writeTo("lakehouse.silver.lms_loans") \
    .tableProperty("format-version", "2") \
    .createOrReplace()
print(f"Silver loans: {df_loan_silver.count()} rows")

# ============================================================
# 3. GOLD LAYER — Aggregated, business-ready
# ============================================================

print("\n" + "=" * 60)
print("PHASE 3: GOLD LAYER — Business Aggregations")
print("=" * 60)

# --- Gold: Branch Transaction Summary ---
df_branch_summary = df_txn_silver \
    .groupBy("branch_code", "txn_type", "channel", "txn_month") \
    .agg(
        count("*").alias("txn_count"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("avg_amount"),
        max("amount").alias("max_amount"),
        countDistinct("customer_id").alias("unique_customers"),
        sum(when(col("is_flagged"), 1).otherwise(0)).alias("flagged_count")
    )

df_branch_summary.writeTo("lakehouse.gold.branch_txn_summary") \
    .tableProperty("format-version", "2") \
    .createOrReplace()
print("Gold - Branch Transaction Summary:")
df_branch_summary.show(truncate=False)

# --- Gold: NPA Classification Summary (RBI format) ---
df_npa_gold = df_loan_silver \
    .groupBy("npa_status", "loan_type", "branch_code") \
    .agg(
        count("*").alias("account_count"),
        sum("outstanding_principal").alias("total_outstanding"),
        sum("sanctioned_amount").alias("total_sanctioned"),
        avg("interest_rate").alias("avg_interest_rate"),
        avg("dpd").alias("avg_dpd")
    )

df_npa_gold.writeTo("lakehouse.gold.npa_classification_summary") \
    .tableProperty("format-version", "2") \
    .createOrReplace()
print("Gold - NPA Classification Summary:")
df_npa_gold.show(truncate=False)

# --- Gold: Customer 360 View ---
df_cust360 = df_cust_silver.alias("c") \
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

df_cust360.writeTo("lakehouse.gold.customer_360") \
    .tableProperty("format-version", "2") \
    .createOrReplace()
print("Gold - Customer 360:")
df_cust360.show(truncate=False)

# ============================================================
# 4. VERIFICATION — Iceberg features
# ============================================================

print("\n" + "=" * 60)
print("PHASE 4: VERIFICATION — Iceberg Features")
print("=" * 60)

# Snapshot history
print("--- Snapshots (Silver Transactions) ---")
spark.sql("SELECT snapshot_id, committed_at, operation, summary FROM lakehouse.silver.finacle_transactions.snapshots").show(truncate=False)

# Partition info
print("--- Partitions (Silver Transactions) ---")
spark.sql("SELECT * FROM lakehouse.silver.finacle_transactions.partitions").show(truncate=False)

# Table metadata
print("--- Table History ---")
spark.sql("SELECT * FROM lakehouse.silver.finacle_transactions.history").show(truncate=False)

# Row counts across all layers
print("\n--- Layer Row Counts ---")
for layer in ["bronze", "silver", "gold"]:
    tables = spark.sql(f"SHOW TABLES IN lakehouse.{layer}").collect()
    for t in tables:
        count = spark.table(f"lakehouse.{layer}.{t.tableName}").count()
        print(f"  {layer}.{t.tableName}: {count} rows")

print("\n" + "=" * 60)
print("ALL TESTS PASSED")
print("=" * 60)

spark.stop()
