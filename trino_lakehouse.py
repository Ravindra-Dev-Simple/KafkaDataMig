"""
================================================================
Trino Bronze Query Client — IN-CLUSTER VERSION
================================================================
Runs INSIDE OpenShift and talks to Trino over the internal
service DNS (no Route, no hosts file needed).

External laptop version uses:
    http://trino-ui-lakehouse-catalog.apps.simplelogicnew.localhost.com:80

This in-cluster version uses:
    http://trino.lakehouse-catalog.svc:8080

Config is read from environment variables so the same image can
be reused with different values in different environments.
================================================================
"""

import argparse
import logging
import os
import sys
from contextlib import contextmanager

import pandas as pd
from tabulate import tabulate
from trino.dbapi import connect
from trino.exceptions import TrinoQueryError, TrinoUserError

# ----------------------------------------------------------------
# Config from environment (with in-cluster defaults)
# ----------------------------------------------------------------
TRINO_HOST        = os.environ.get("TRINO_HOST", "jdbc:trino://trino.lakehouse-catalog.svc:8080")
TRINO_PORT        = int(os.environ.get("TRINO_PORT", "8080"))
TRINO_USER        = os.environ.get("TRINO_USER", "admin")
TRINO_CATALOG     = os.environ.get("TRINO_CATALOG", "iceberg")
TRINO_SCHEMA      = os.environ.get("TRINO_SCHEMA", "bronze")
TRINO_HTTP_SCHEME = os.environ.get("TRINO_HTTP_SCHEME", "http")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("bronze")


# ----------------------------------------------------------------
# Connection helpers
# ----------------------------------------------------------------
@contextmanager
def trino_connection():
    log.info("Trino: %s://%s:%s (catalog=%s, schema=%s)",
             TRINO_HTTP_SCHEME, TRINO_HOST, TRINO_PORT, TRINO_CATALOG, TRINO_SCHEMA)
    conn = connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA,
        http_scheme=TRINO_HTTP_SCHEME,
    )
    try:
        yield conn
    finally:
        conn.close()


def run_query(conn, sql: str):
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description] if cur.description else []
    return pd.DataFrame(rows, columns=cols)


def show(df, title: str, max_rows: int = 20):
    print(f"\n{'=' * 70}\n>>> {title}\n{'=' * 70}")
    if df is None or df.empty:
        print("(no rows)")
        return
    print(tabulate(df.head(max_rows), headers="keys", tablefmt="psql", showindex=False))
    if len(df) > max_rows:
        print(f"... ({len(df) - max_rows} more rows)")


def run_and_show(conn, sql: str, title: str, max_rows: int = 20):
    try:
        df = run_query(conn, sql)
        show(df, title, max_rows=max_rows)
        return df
    except (TrinoUserError, TrinoQueryError) as e:
        msg = e.message if hasattr(e, "message") else str(e)
        log.error("Query failed [%s]: %s", title, msg)
        return None
    except Exception as e:
        log.error("Unexpected error [%s]: %s", title, e)
        return None


# ================================================================
# SECTIONS
# ================================================================
def section_counts(conn):
    print("\n" + "#" * 70)
    print("# Bronze — Discovery & row counts")
    print("#" * 70)

    run_and_show(conn, "SHOW TABLES FROM iceberg.bronze", "Tables in Bronze")

    run_and_show(conn, """
        SELECT 'finacle_transactions' AS tbl, COUNT(*) AS rows FROM iceberg.bronze.finacle_transactions
        UNION ALL SELECT 'finacle_customers', COUNT(*) FROM iceberg.bronze.finacle_customers
        UNION ALL SELECT 'aml_alerts',        COUNT(*) FROM iceberg.bronze.aml_alerts
        UNION ALL SELECT 'cibil_bureau',      COUNT(*) FROM iceberg.bronze.cibil_bureau
        UNION ALL SELECT 'npa_report',        COUNT(*) FROM iceberg.bronze.npa_report
        ORDER BY tbl
    """, "Row counts per Bronze table")

    run_and_show(conn, """
        SELECT
            kafka_topic,
            COUNT(*) AS messages,
            MIN(kafka_timestamp) AS oldest_msg,
            MAX(kafka_timestamp) AS newest_msg,
            MAX(_ingestion_ts)   AS last_ingested
        FROM iceberg.bronze.finacle_transactions
        GROUP BY kafka_topic
    """, "Kafka ingestion freshness (finacle_transactions)")


def section_txn(conn):
    print("\n" + "#" * 70)
    print("# Bronze — Transaction analytics")
    print("#" * 70)

    run_and_show(conn, """
        SELECT
            UPPER(TRIM(channel)) AS channel,
            COUNT(*) AS txn_count,
            ROUND(SUM(CAST(amount AS DECIMAL(18,2))), 2) AS total_amount,
            ROUND(AVG(CAST(amount AS DECIMAL(18,2))), 2) AS avg_amount,
            COUNT(DISTINCT customer_id) AS unique_customers
        FROM iceberg.bronze.finacle_transactions
        WHERE TRY_CAST(amount AS DECIMAL(18,2)) IS NOT NULL
        GROUP BY UPPER(TRIM(channel))
        ORDER BY total_amount DESC
    """, "Channel distribution")

    run_and_show(conn, """
        SELECT
            branch_code,
            UPPER(TRIM(txn_type)) AS txn_type,
            COUNT(*) AS txn_count,
            ROUND(SUM(CAST(amount AS DECIMAL(18,2))), 2) AS total_amount,
            ROUND(MAX(CAST(amount AS DECIMAL(18,2))), 2) AS max_amount
        FROM iceberg.bronze.finacle_transactions
        WHERE TRY_CAST(amount AS DECIMAL(18,2)) IS NOT NULL
        GROUP BY branch_code, UPPER(TRIM(txn_type))
        ORDER BY total_amount DESC
        LIMIT 15
    """, "Branch + type summary (top 15)")

    run_and_show(conn, """
        SELECT
            HOUR(CAST(txn_date AS TIMESTAMP)) AS txn_hour,
            COUNT(*) AS txn_count,
            ROUND(SUM(CAST(amount AS DECIMAL(18,2))), 2) AS total_amount
        FROM iceberg.bronze.finacle_transactions
        WHERE TRY_CAST(txn_date AS TIMESTAMP) IS NOT NULL
          AND TRY_CAST(amount   AS DECIMAL(18,2)) IS NOT NULL
        GROUP BY HOUR(CAST(txn_date AS TIMESTAMP))
        ORDER BY txn_hour
    """, "Hourly transaction pattern")

    run_and_show(conn, """
        SELECT
            COUNT(*) AS total_rows,
            SUM(CASE WHEN TRY_CAST(amount   AS DECIMAL(18,2)) IS NULL THEN 1 ELSE 0 END) AS bad_amount,
            SUM(CASE WHEN TRY_CAST(txn_date AS TIMESTAMP)     IS NULL THEN 1 ELSE 0 END) AS bad_txn_date,
            SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS missing_customer
        FROM iceberg.bronze.finacle_transactions
    """, "Data quality — unparseable values")


def section_risk(conn):
    print("\n" + "#" * 70)
    print("# Bronze — Risk views (AML / NPA / CIBIL)")
    print("#" * 70)

    run_and_show(conn, """
        SELECT
            t.txn_id,
            t.customer_id,
            c.name,
            CAST(t.amount AS DECIMAL(18,2)) AS amount,
            t.channel,
            t.narration,
            t.txn_date
        FROM iceberg.bronze.finacle_transactions t
        LEFT JOIN iceberg.bronze.finacle_customers c ON t.customer_id = c.customer_id
        WHERE UPPER(TRIM(t.channel)) = 'CASH'
          AND TRY_CAST(t.amount AS DECIMAL(18,2)) >= 1000000
        ORDER BY TRY_CAST(t.amount AS DECIMAL(18,2)) DESC
        LIMIT 20
    """, "High-value cash transactions (>= INR 10L)")

    run_and_show(conn, """
        SELECT
            alert_id, customer_id, customer_name, alert_type,
            CAST(risk_score AS INTEGER) AS risk_score,
            CAST(total_amount AS DECIMAL(18,2)) AS total_amount,
            status, priority, alert_date
        FROM iceberg.bronze.aml_alerts
        WHERE UPPER(TRIM(status)) = 'OPEN'
        ORDER BY TRY_CAST(risk_score AS INTEGER) DESC
        LIMIT 20
    """, "Open AML alerts (by risk score)")

    run_and_show(conn, """
        SELECT
            UPPER(TRIM(npa_status)) AS npa_status,
            COUNT(*) AS accounts,
            ROUND(SUM(CAST(outstanding_principal AS DECIMAL(18,2))), 2) AS total_outstanding,
            ROUND(AVG(CAST(dpd AS INTEGER)), 0) AS avg_dpd
        FROM iceberg.bronze.npa_report
        WHERE TRY_CAST(outstanding_principal AS DECIMAL(18,2)) IS NOT NULL
        GROUP BY UPPER(TRIM(npa_status))
        ORDER BY total_outstanding DESC
    """, "NPA classification (from Bronze)")

    run_and_show(conn, """
        SELECT
            customer_id, customer_name,
            CAST(cibil_score AS INTEGER) AS cibil_score,
            CAST(total_outstanding AS DECIMAL(18,2)) AS total_outstanding,
            CAST(dpd_90_count AS INTEGER) AS dpd_90_count,
            wilful_defaulter, report_pull_date
        FROM iceberg.bronze.cibil_bureau
        WHERE TRY_CAST(cibil_score AS INTEGER) < 600
        ORDER BY TRY_CAST(cibil_score AS INTEGER) ASC
        LIMIT 20
    """, "Low CIBIL customers (score < 600)")


# ================================================================
# MAIN
# ================================================================
SECTIONS = {
    "counts": section_counts,
    "txn":    section_txn,
    "risk":   section_risk,
}


def main() -> int:
    parser = argparse.ArgumentParser(description="Trino Bronze queries (in-cluster)")
    parser.add_argument("--section", choices=list(SECTIONS), default=None)
    args = parser.parse_args()

    try:
        with trino_connection() as conn:
            df = run_query(conn, "SHOW SCHEMAS FROM iceberg")
            schemas = df["Schema"].tolist() if df is not None else []
            if "bronze" not in schemas:
                log.error("Bronze schema not found. Run the Kafka consumer first.")
                log.error("Available schemas: %s", schemas)
                return 1
            log.info("Bronze schema found — proceeding.")

            if args.section:
                SECTIONS[args.section](conn)
            else:
                for name, fn in SECTIONS.items():
                    log.info("Running section: %s", name)
                    try:
                        fn(conn)
                    except Exception as e:
                        log.error("Section %s crashed: %s", name, e)

        log.info("Done.")
        return 0

    except Exception as e:
        log.exception("Fatal: %s", e)
        return 2


if __name__ == "__main__":
    sys.exit(main())