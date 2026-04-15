"""
CSV to Kafka Producer — Banking Data Pipeline (Step 1)
Reads CSV files from disk and publishes each row as a JSON message to Kafka topics.

CSV → Kafka Topics:
  finacle_transactions.csv  →  finacle-transactions
  finacle_customers.csv     →  finacle-customers
  aml_alerts.csv            →  aml-alerts
  cibil_bureau.csv          →  cibil-bureau
  npa_report.csv            →  npa-report

Usage:
  python csv_to_kafka_producer.py --bootstrap-server kafka:9092 --csv-dir D:/Datamig
  python csv_to_kafka_producer.py --dry-run   # print to stdout instead of Kafka
"""
import csv
import json
import os
import sys
import time
import argparse
from datetime import datetime


# -- Topic mapping: CSV filename (without extension) → Kafka topic --
CSV_TOPIC_MAP = {
    "finacle_transactions": "finacle-transactions",
    "finacle_customers": "finacle-customers",
    "aml_alerts": "aml-alerts",
    "cibil_bureau": "cibil-bureau",
    "npa_report": "npa-report",
}

# -- Key column for each topic (used as Kafka message key) --
KEY_COLUMNS = {
    "finacle-transactions": "txn_id",
    "finacle-customers": "customer_id",
    "aml-alerts": "alert_id",
    "cibil-bureau": "report_id",
    "npa-report": "loan_id",
}


def get_kafka_config():
    """Load Kafka connection config from environment variables."""
    return {
        "bootstrap_servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "my-cluster-kafka-bootstrap:9092"),
        "security_protocol": os.environ.get("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT"),
        "sasl_mechanism": os.environ.get("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512"),
        "sasl_plain_username": os.environ.get("KAFKA_USERNAME", "app-user"),
        "sasl_plain_password": os.environ.get("KAFKA_PASSWORD", ""),
    }


def create_producer(kafka_config):
    """Create and return a KafkaProducer with SASL auth."""
    from kafka import KafkaProducer

    return KafkaProducer(
        bootstrap_servers=kafka_config["bootstrap_servers"],
        security_protocol=kafka_config["security_protocol"],
        sasl_mechanism=kafka_config["sasl_mechanism"],
        sasl_plain_username=kafka_config["sasl_plain_username"],
        sasl_plain_password=kafka_config["sasl_plain_password"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
        batch_size=32768,
        linger_ms=10,
        compression_type="gzip",
        request_timeout_ms=60000,
    )


def read_csv_file(filepath):
    """Read a CSV file and yield each row as a dict."""
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield dict(row)


def publish_csv_to_kafka(producer, csv_path, topic, key_column, dry_run=False):
    """Read a CSV file and publish each row to a Kafka topic."""
    if not os.path.exists(csv_path):
        print(f"  SKIP: {csv_path} not found")
        return 0

    count = 0
    for row in read_csv_file(csv_path):
        key = row.get(key_column, f"row_{count}")

        if dry_run:
            if count < 3:
                print(f"  [DRY-RUN] topic={topic} key={key}")
                print(f"    {json.dumps(row, indent=2)[:200]}...")
        else:
            producer.send(topic, key=key, value=row)

        count += 1

    return count


def main():
    parser = argparse.ArgumentParser(description="CSV to Kafka Producer — Banking Data Pipeline")
    parser.add_argument("--bootstrap-server", default=None,
                        help="Kafka bootstrap server (overrides KAFKA_BOOTSTRAP_SERVERS env var)")
    parser.add_argument("--csv-dir", default="D:/Datamig",
                        help="Directory containing CSV files")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print sample output instead of sending to Kafka")
    parser.add_argument("--topic", default=None,
                        help="Only publish a specific topic (e.g., finacle-transactions)")
    args = parser.parse_args()

    kafka_config = get_kafka_config()
    if args.bootstrap_server:
        kafka_config["bootstrap_servers"] = args.bootstrap_server

    print("=" * 60)
    print("CSV → Kafka Producer — Banking Data Pipeline")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print(f"CSV Dir:   {args.csv_dir}")
    print(f"Kafka:     {kafka_config['bootstrap_servers']}")
    print(f"Dry Run:   {args.dry_run}")
    print("=" * 60)

    producer = None
    if not args.dry_run:
        if not kafka_config["sasl_plain_password"]:
            print("ERROR: KAFKA_PASSWORD environment variable not set.")
            print("  Set it with: export KAFKA_PASSWORD='your-password'")
            sys.exit(1)
        producer = create_producer(kafka_config)

    total_messages = 0
    start_time = time.time()

    for csv_name, topic in CSV_TOPIC_MAP.items():
        if args.topic and topic != args.topic:
            continue

        csv_path = os.path.join(args.csv_dir, f"{csv_name}.csv")
        key_col = KEY_COLUMNS.get(topic, "id")

        print(f"\n--- {csv_name}.csv → {topic} ---")
        count = publish_csv_to_kafka(producer, csv_path, topic, key_col, args.dry_run)
        total_messages += count
        print(f"  Published: {count} messages")

    if producer and not args.dry_run:
        producer.flush()
        producer.close()

    elapsed = time.time() - start_time

    print(f"\n{'=' * 60}")
    print(f"DONE — {total_messages} total messages in {elapsed:.2f}s")
    if elapsed > 0 and total_messages > 0:
        print(f"Throughput: {total_messages / elapsed:.0f} msg/s")
    print("=" * 60)


if __name__ == "__main__":
    main()
