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
import sys
import os
from datetime import datetime, timedelta

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
        "customer_name": random.choice(NAMES),
        "alert_date": txn["txn_date"],
        "alert_type": alert_type,
        "risk_score": random.randint(60, 99),
        "rule_triggered": rule,
        "transaction_ids": txn["txn_id"],
        "total_amount": txn["amount"],
        "currency": "INR",
        "description": desc,
        "status": random.choice(["OPEN", "UNDER_REVIEW"]),
        "assigned_to": random.choice(["Officer_A", "Officer_B", "Officer_C"]),
        "priority": random.choice(["CRITICAL", "HIGH", "MEDIUM"]),
        "due_date": (datetime.now() + timedelta(days=random.randint(7, 30))).strftime("%Y-%m-%d"),
        "resolution": "",
        "resolution_date": "",
        "sar_filed": random.choice(["Yes", "No", "No"]),
        "sar_reference": "",
    }


CITIES = ["Mumbai", "Delhi", "Bangalore", "Chennai", "Hyderabad", "Pune", "Kolkata", "Ahmedabad"]
STATES = ["Maharashtra", "Delhi", "Karnataka", "Tamil Nadu", "Telangana", "Maharashtra", "West Bengal", "Gujarat"]
OCCUPATIONS = ["Salaried", "Self-Employed", "Business", "Professional", "Retired", "Student"]
ACCOUNT_TYPES = ["SAVINGS", "CURRENT", "FD", "RD", "NRE"]
KYC_STATUSES = ["VERIFIED", "VERIFIED", "VERIFIED", "PENDING", "EXPIRED"]
RISK_CATEGORIES = ["LOW", "LOW", "LOW", "MEDIUM", "MEDIUM", "HIGH"]
GENDERS = ["Male", "Female"]


def generate_customer(cust_id):
    """Generate a fake customer record"""
    name = random.choice(NAMES) + " " + random.choice(["Kumar", "Singh", "Sharma", "Patel", "Reddy", "Nair", "Das", "Joshi"])
    city_idx = random.randint(0, len(CITIES) - 1)
    open_date = datetime(2020, 1, 1) + timedelta(days=random.randint(0, 2000))
    kyc_date = open_date + timedelta(days=random.randint(30, 365))
    dob = datetime(1960, 1, 1) + timedelta(days=random.randint(0, 15000))

    return {
        "customer_id": cust_id,
        "name": name,
        "pan": f"{''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=5))}{random.randint(1000, 9999)}{''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=1))}",
        "aadhaar_masked": f"XXXX-XXXX-{random.randint(1000, 9999)}",
        "mobile": f"9{random.randint(100000000, 999999999)}",
        "email": f"{name.split()[0].lower()}{random.randint(1, 999)}@example.com",
        "dob": dob.strftime("%Y-%m-%d"),
        "gender": random.choice(GENDERS),
        "kyc_status": random.choice(KYC_STATUSES),
        "kyc_date": kyc_date.strftime("%Y-%m-%d"),
        "risk_category": random.choice(RISK_CATEGORIES),
        "branch_code": random.choice(BRANCHES),
        "account_type": random.choice(ACCOUNT_TYPES),
        "account_number": f"ACCT{str(random.randint(100000, 999999))}",
        "account_open_date": open_date.strftime("%Y-%m-%d"),
        "occupation": random.choice(OCCUPATIONS),
        "annual_income": str(round(random.uniform(200000, 5000000), 2)),
        "address_city": CITIES[city_idx],
        "address_state": STATES[city_idx],
        "nominee_name": random.choice(NAMES) + " " + random.choice(["Kumar", "Singh", "Sharma"]),
    }


LOAN_TYPE_LIST = ["HOME_LOAN", "PERSONAL_LOAN", "CAR_LOAN", "BUSINESS_LOAN", "EDUCATION_LOAN"]
NPA_STATUSES = ["STANDARD", "STANDARD", "STANDARD", "SMA-0", "SMA-1", "SMA-2", "SUBSTANDARD", "DOUBTFUL", "LOSS"]
ASSET_CLASSIFICATIONS = ["STANDARD", "STANDARD", "NPA", "NPA"]
COLLATERAL_TYPES = ["PROPERTY", "GOLD", "FD", "SHARES", "NONE"]
RECOVERY_ACTIONS = ["NONE", "NOTICE_SENT", "SARFAESI", "DRT", "LOK_ADALAT", "OTS"]


def generate_npa_record(cust_id, seq):
    """Generate a fake NPA/loan record"""
    sanctioned = round(random.uniform(100000, 10000000), 2)
    outstanding_principal = round(sanctioned * random.uniform(0.3, 0.95), 2)
    outstanding_interest = round(outstanding_principal * random.uniform(0.01, 0.15), 2)
    total_outstanding = round(outstanding_principal + outstanding_interest, 2)
    collateral_value = round(sanctioned * random.uniform(0.5, 1.5), 2)
    dpd = random.choice([0, 0, 0, 0, 15, 30, 60, 90, 120, 180, 365])
    npa_status = "STANDARD" if dpd < 90 else random.choice(["SUBSTANDARD", "DOUBTFUL", "LOSS"])
    npa_date = "" if dpd < 90 else (datetime.now() - timedelta(days=dpd)).strftime("%Y-%m-%d")
    provision_pct = 0.4 if dpd < 90 else random.choice([15.0, 25.0, 40.0, 100.0])
    provision_amt = round(total_outstanding * provision_pct / 100, 2)
    recovery_amt = round(total_outstanding * random.uniform(0, 0.3), 2) if dpd >= 90 else 0

    return {
        "report_date": datetime.now().strftime("%Y-%m-%d"),
        "loan_id": f"LN{str(seq).zfill(6)}",
        "customer_id": cust_id,
        "customer_name": random.choice(NAMES),
        "loan_type": random.choice(LOAN_TYPE_LIST),
        "branch_code": random.choice(BRANCHES),
        "sanctioned_amount": str(sanctioned),
        "outstanding_principal": str(outstanding_principal),
        "outstanding_interest": str(outstanding_interest),
        "total_outstanding": str(total_outstanding),
        "dpd": str(dpd),
        "asset_classification": "STANDARD" if dpd < 90 else "NPA",
        "npa_status": npa_status,
        "npa_date": npa_date,
        "provision_required_pct": str(provision_pct),
        "provision_amount": str(provision_amt),
        "collateral_type": random.choice(COLLATERAL_TYPES),
        "collateral_value": str(collateral_value),
        "net_npa_exposure": str(round(total_outstanding - collateral_value, 2)),
        "recovery_action": "NONE" if dpd < 90 else random.choice(RECOVERY_ACTIONS),
        "recovery_amount": str(recovery_amt),
        "last_review_date": (datetime.now() - timedelta(days=random.randint(1, 90))).strftime("%Y-%m-%d"),
        "next_review_date": (datetime.now() + timedelta(days=random.randint(30, 180))).strftime("%Y-%m-%d"),
        "relationship_manager": f"RM_{random.randint(1, 20):03d}",
        "remarks": random.choice(["Regular repayment", "Under monitoring", "Recovery in progress", "Legal action initiated", ""]),
    }


def generate_cibil_record(cust_id, seq):
    """Generate a fake CIBIL bureau record"""
    total_accounts = random.randint(1, 15)
    active = random.randint(1, total_accounts)
    closed = total_accounts - active
    overdue = random.randint(0, min(3, active))
    score = random.randint(300, 900)
    total_out = round(random.uniform(50000, 5000000), 2)
    secured = round(total_out * random.uniform(0.4, 0.8), 2)
    unsecured = round(total_out - secured, 2)

    return {
        "report_id": f"CBR{str(seq).zfill(6)}",
        "customer_id": cust_id,
        "customer_name": random.choice(NAMES),
        "pan": f"{''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=5))}{random.randint(1000, 9999)}{''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=1))}",
        "cibil_score": str(score),
        "score_date": (datetime.now() - timedelta(days=random.randint(1, 90))).strftime("%Y-%m-%d"),
        "total_accounts": str(total_accounts),
        "active_accounts": str(active),
        "closed_accounts": str(closed),
        "overdue_accounts": str(overdue),
        "total_outstanding": str(total_out),
        "secured_outstanding": str(secured),
        "unsecured_outstanding": str(unsecured),
        "credit_utilization_pct": str(random.randint(10, 95)),
        "enquiry_count_6m": str(random.randint(0, 8)),
        "enquiry_count_12m": str(random.randint(0, 15)),
        "dpd_30_count": str(random.randint(0, 5)),
        "dpd_60_count": str(random.randint(0, 3)),
        "dpd_90_count": str(random.randint(0, 2)),
        "written_off_amount": str(round(random.uniform(0, 100000), 2) if random.random() < 0.1 else 0),
        "suit_filed_count": str(random.randint(0, 2) if random.random() < 0.05 else 0),
        "wilful_defaulter": random.choice(["No", "No", "No", "No", "Yes"]),
        "report_pull_date": datetime.now().strftime("%Y-%m-%d"),
        "report_source": random.choice(["CIBIL", "EQUIFAX", "EXPERIAN", "CRIF"]),
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
    customers = []
    npa_records = []
    cibil_records = []

    # --- Generate fake data for ALL 5 topics ---
    num_customers = min(args.count // 20, 500)  # ~5% of txn count
    num_loans = min(args.count // 10, 200)

    print(f"Generating fake data...")
    print(f"  Transactions: {args.count} across {args.days} days")
    print(f"  Customers:    {num_customers}")
    print(f"  Loans/NPA:    {num_loans}")

    # 1. Customers
    used_cust_ids = CUSTOMER_IDS[:num_customers]
    for cid in used_cust_ids:
        customers.append(generate_customer(cid))

    # 2. Transactions + AML alerts
    for i in range(1, args.count + 1):
        day_offset = random.randint(0, args.days - 1)
        txn_date = base_date + timedelta(days=day_offset)
        txn = generate_transaction(i, txn_date)
        transactions.append(txn)
        if txn["status"] == "FLAGGED":
            aml_alerts.append(generate_aml_alert(txn))

    # 3. NPA / Loan records
    for i in range(1, num_loans + 1):
        cid = random.choice(used_cust_ids)
        npa_records.append(generate_npa_record(cid, i))

    # 4. CIBIL bureau records (one per customer)
    for i, cid in enumerate(used_cust_ids, 1):
        cibil_records.append(generate_cibil_record(cid, i))

    print(f"  AML Alerts:   {len(aml_alerts)}")
    print(f"  CIBIL:        {len(cibil_records)}")

    if args.dry_run:
        print("\n--- Sample Transaction ---")
        print(json.dumps(transactions[0], indent=2))
        print("\n--- Sample Customer ---")
        print(json.dumps(customers[0], indent=2))
        print("\n--- Sample NPA ---")
        print(json.dumps(npa_records[0], indent=2))
        print("\n--- Sample CIBIL ---")
        print(json.dumps(cibil_records[0], indent=2))
        if aml_alerts:
            print("\n--- Sample AML Alert ---")
            print(json.dumps(aml_alerts[0], indent=2))
        return

    # --- Publish to Kafka ---
    kafka_password = os.environ.get("KAFKA_PASSWORD", "")
    if not kafka_password:
        print("ERROR: Set KAFKA_PASSWORD environment variable first")
        print("  export KAFKA_PASSWORD='your-password'")
        sys.exit(1)

    try:
        from kafka import KafkaProducer

        producer = KafkaProducer(
            bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "my-cluster-kafka-bootstrap:9092"),
            security_protocol=os.environ.get("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT"),
            sasl_mechanism=os.environ.get("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512"),
            sasl_plain_username=os.environ.get("KAFKA_USERNAME", "app-user"),
            sasl_plain_password=kafka_password,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
            acks="all",
            retries=3,
            batch_size=32768,
            linger_ms=10,
            request_timeout_ms=60000,
            compression_type="gzip",
        )

        start = time.time()

        # Publish all 5 topics
        for txn in transactions:
            producer.send("finacle-transactions", key=txn["txn_id"], value=txn)

        for cust in customers:
            producer.send("finacle-customers", key=cust["customer_id"], value=cust)

        for alert in aml_alerts:
            producer.send("aml-alerts", key=alert["alert_id"], value=alert)

        for npa in npa_records:
            producer.send("npa-report", key=npa["loan_id"], value=npa)

        for cibil in cibil_records:
            producer.send("cibil-bureau", key=cibil["report_id"], value=cibil)

        producer.flush()
        elapsed = time.time() - start

        total_msgs = len(transactions) + len(customers) + len(aml_alerts) + len(npa_records) + len(cibil_records)
        print(f"\nPublished to Kafka ({elapsed:.2f}s, {total_msgs / elapsed:.0f} msg/s):")
        print(f"  finacle-transactions: {len(transactions)} messages")
        print(f"  finacle-customers:    {len(customers)} messages")
        print(f"  aml-alerts:           {len(aml_alerts)} messages")
        print(f"  npa-report:           {len(npa_records)} messages")
        print(f"  cibil-bureau:         {len(cibil_records)} messages")
        print(f"  TOTAL:                {total_msgs} messages")

    except Exception as e:
        print(f"ERROR: {e}. Falling back to writing JSON files...")
        import tempfile
        tmpdir = tempfile.gettempdir()
        all_data = {
            "transactions": transactions,
            "customers": customers,
            "aml_alerts": aml_alerts,
            "npa_records": npa_records,
            "cibil_records": cibil_records,
        }
        for name, records in all_data.items():
            filepath = os.path.join(tmpdir, f"{name}.json")
            with open(filepath, "w") as f:
                for rec in records:
                    f.write(json.dumps(rec) + "\n")
            print(f"  Written {len(records)} records to {filepath}")


if __name__ == "__main__":
    main()
