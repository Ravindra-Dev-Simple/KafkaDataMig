"""
Kafka Bulk Producer — Generate realistic banking transaction data for throughput testing
Generates 10,000+ Finacle-style transactions and publishes to Kafka topics.

Usage (inside kafka-test-client pod or any pod with kafka-python):
  pip install kafka-python
  python kafka_bulk_producer.py --bootstrap-server kafka.simplelogic.localhost.com:9092 --count 10000
"""
import json
import random
import argparse
import time
from datetime import datetime, timedelta
import os

# -- Configuration --
BRANCHES = ["BR001", "BR002", "BR003", "BR004", "BR005"]
CHANNELS = ["NEFT", "RTGS", "UPI", "IMPS", "ATM", "POS", "CASH", "INTERNET_BANKING"]
CURRENCIES = ["INR"]
STATUSES = ["SUCCESS", "SUCCESS", "SUCCESS", "SUCCESS", "SUCCESS",
            "SUCCESS", "SUCCESS", "SUCCESS", "FLAGGED", "FAILED"]  # 80% success, 10% flagged, 10% failed
TXN_TYPES = ["CREDIT", "DEBIT"]
CUSTOMER_IDS = [f"CUST{str(i).zfill(6)}" for i in range(1, 501)]
ACCOUNT_IDS = [f"ACCT{str(i).zfill(6)}" for i in range(1, 501)]

NARRATIONS_CREDIT = [
    "SALARY {month} 2026 - {company}",
    "NEFT CREDIT FROM {name}",
    "RENTAL INCOME",
    "FD MATURITY CREDIT",
    "MUTUAL FUND REDEMPTION",
    "BUSINESS INCOME",
    "GST REFUND",
    "INSURANCE CLAIM SETTLEMENT",
    "PENSION CREDIT",
    "DIVIDEND INCOME",
]

NARRATIONS_DEBIT = [
    "EMI PAYMENT - {loan_type}",
    "RENT PAYMENT",
    "ELECTRICITY BILL - {provider}",
    "UPI TRANSFER TO {name}",
    "ATM CASH WITHDRAWAL",
    "INSURANCE PREMIUM - {company}",
    "CREDIT CARD BILL PAYMENT",
    "SIP INVESTMENT",
    "GST PAYMENT",
    "SUPPLIER PAYMENT",
    "SCHOOL FEE",
    "MEDICAL BILL",
    "GROCERY PURCHASE",
    "FUEL STATION",
]

COMPANIES = ["INFOSYS", "TCS", "WIPRO", "HCL", "COGNIZANT", "ACCENTURE",
             "MINDTREE", "TECH MAHINDRA", "MPHASIS", "L&T INFOTECH"]
NAMES = ["Rajesh", "Priya", "Arun", "Kavitha", "Suresh", "Deepa",
         "Venkatesh", "Lakshmi", "Karthik", "Anjali", "Mohammed", "Pooja"]
PROVIDERS = ["BESCOM", "TANGEDCO", "TSSPDCL", "CESC", "BSES", "MSEDCL"]
LOAN_TYPES = ["HOME LOAN", "PERSONAL LOAN", "CAR LOAN", "BUSINESS LOAN"]
MONTHS = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN",
          "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]


def generate_amount(channel, txn_type):
    """Generate realistic transaction amounts based on channel"""
    ranges = {
        "ATM": (500, 25000),
        "UPI": (10, 100000),
        "POS": (100, 50000),
        "IMPS": (100, 200000),
        "NEFT": (1000, 1000000),
        "RTGS": (200000, 10000000),
        "CASH": (1000, 500000),
        "INTERNET_BANKING": (100, 500000),
    }
    low, high = ranges.get(channel, (100, 100000))
    amount = round(random.uniform(low, high), 2)
    # Occasional high-value suspicious transaction
    if random.random() < 0.02:
        amount = round(random.uniform(900000, 5000000), 2)
    return amount


def generate_narration(txn_type):
    """Generate realistic narration text"""
    if txn_type == "CREDIT":
        template = random.choice(NARRATIONS_CREDIT)
    else:
        template = random.choice(NARRATIONS_DEBIT)

    return template.format(
        month=random.choice(MONTHS),
        company=random.choice(COMPANIES),
        name=random.choice(NAMES),
        provider=random.choice(PROVIDERS),
        loan_type=random.choice(LOAN_TYPES),
    )


def generate_transaction(seq_num, base_date):
    """Generate a single transaction record"""
    txn_type = random.choice(TXN_TYPES)
    channel = random.choice(CHANNELS)
    customer_idx = random.randint(0, len(CUSTOMER_IDS) - 1)

    # Time offset: spread across the day
    offset_seconds = random.randint(0, 86400)
    txn_time = base_date + timedelta(seconds=offset_seconds)

    amount = generate_amount(channel, txn_type)
    balance = round(random.uniform(10000, 5000000), 2)

    status = random.choice(STATUSES)
    # High-value cash transactions more likely to be flagged
    if channel == "CASH" and amount > 500000:
        status = "FLAGGED"

    return {
        "txn_id": f"TXN{base_date.strftime('%Y%m%d')}{str(seq_num).zfill(6)}",
        "account_id": ACCOUNT_IDS[customer_idx],
        "customer_id": CUSTOMER_IDS[customer_idx],
        "txn_type": txn_type,
        "amount": amount,
        "currency": "INR",
        "txn_date": txn_time.strftime("%Y-%m-%dT%H:%M:%S"),
        "value_date": txn_time.strftime("%Y-%m-%d"),
        "branch_code": random.choice(BRANCHES),
        "channel": channel,
        "narration": generate_narration(txn_type),
        "balance_after": balance,
        "status": status,
        "ref_number": f"{channel}{base_date.strftime('%Y%m%d')}{str(seq_num).zfill(6)}",
    }


def generate_aml_alert(txn):
    """Generate AML alert for suspicious transactions"""
    alert_types = [
        ("STRUCTURING", "RULE_CTR_SPLIT", "Multiple transactions structured to avoid CTR threshold"),
        ("HIGH_VALUE", "RULE_HIGH_VALUE", f"High value {txn['channel']} transaction of INR {txn['amount']}"),
        ("VELOCITY", "RULE_HIGH_VELOCITY", "Unusual transaction velocity detected on account"),
        ("UNUSUAL_PATTERN", "RULE_PATTERN_BREAK", "Transaction pattern deviates from customer profile"),
    ]
    alert_type, rule, desc = random.choice(alert_types)

    return {
        "alert_id": f"AML{txn['txn_date'][:10].replace('-', '')}{random.randint(10000, 99999)}",
        "customer_id": txn["customer_id"],
        "alert_date": txn["txn_date"],
        "alert_type": alert_type,
        "risk_score": random.randint(60, 99),
        "rule_triggered": rule,
        "transaction_ids": txn["txn_id"],
        "total_amount": txn["amount"],
        "currency": "INR",
        "description": desc,
        "status": random.choice(["OPEN", "UNDER_REVIEW"]),
        "priority": random.choice(["CRITICAL", "HIGH", "MEDIUM"]),
    }


def main():
    parser = argparse.ArgumentParser(description="Banking Kafka Bulk Producer")
    parser.add_argument("--bootstrap-server", default="my-cluster-kafka-bootstrap:9092")
    parser.add_argument("--count", type=int, default=10000, help="Number of transactions to generate")
    parser.add_argument("--days", type=int, default=30, help="Spread transactions across N days")
    parser.add_argument("--dry-run", action="store_true", help="Print to stdout instead of Kafka")
    args = parser.parse_args()

    base_date = datetime(2026, 3, 15)
    transactions = []
    aml_alerts = []

    print(f"Generating {args.count} transactions across {args.days} days...")
    for i in range(1, args.count + 1):
        day_offset = random.randint(0, args.days - 1)
        txn_date = base_date + timedelta(days=day_offset)
        txn = generate_transaction(i, txn_date)
        transactions.append(txn)

        # Generate AML alert for flagged transactions
        if txn["status"] == "FLAGGED":
            aml_alerts.append(generate_aml_alert(txn))

    print(f"Generated {len(transactions)} transactions, {len(aml_alerts)} AML alerts")

    if args.dry_run:
        for txn in transactions[:5]:
            print(json.dumps(txn, indent=2))
        print(f"... and {len(transactions) - 5} more")
        return

    # Publish to Kafka
    try:
        from kafka import KafkaProducer

        producer = KafkaProducer(
            bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "my-cluster-kafka-bootstrap:9092"),
            security_protocol=os.environ.get("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT"),
            sasl_mechanism=os.environ.get("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512"),
            sasl_plain_username=os.environ.get("KAFKA_USERNAME", "app-user"),
            sasl_plain_password=os.environ.get("KAFKA_PASSWORD", ""),

            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),

            acks="all",
            retries=3,
            batch_size=32768,
            linger_ms=10,
            api_version_auto_timeout_ms=30000,
            request_timeout_ms=60000,

            # 🔥 bonus improvement
            compression_type="gzip"
)

        start = time.time()

        # Publish transactions
        for txn in transactions:
            producer.send(
                "finacle-transactions",
                key=txn["txn_id"],
                value=txn,
            )

        # Publish AML alerts
        for alert in aml_alerts:
            producer.send(
                "aml-alerts",
                key=alert["alert_id"],
                value=alert,
            )

        producer.flush()
        elapsed = time.time() - start

        print(f"\nPublished to Kafka:")
        print(f"  Transactions: {len(transactions)} messages to 'finacle-transactions'")
        print(f"  AML Alerts:   {len(aml_alerts)} messages to 'aml-alerts'")
        print(f"  Time:         {elapsed:.2f}s")
        print(f"  Throughput:   {len(transactions) / elapsed:.0f} msg/s")

    except Exception as e:
        print(f"ERROR: {e}. Falling back to writing JSON files...")
        import tempfile
        tmpdir = tempfile.gettempdir()
        with open(os.path.join(tmpdir, "transactions.json"), "w") as f:
            for txn in transactions:
                f.write(json.dumps(txn) + "\n")
        with open(os.path.join(tmpdir, "aml_alerts.json"), "w") as f:
            for alert in aml_alerts:
                f.write(json.dumps(alert) + "\n")
        print(f"Written to {tmpdir}\\transactions.json and {tmpdir}\\aml_alerts.json")


if __name__ == "__main__":
    main()
