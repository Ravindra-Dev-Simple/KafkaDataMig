from kafka import KafkaProducer
import json

# ===== CONFIG =====
BOOTSTRAP_SERVER = "my-cluster-kafka-bootstrap:9092"
USERNAME = "app-user"
PASSWORD = "bwDqbYGrgC2AKOMuthoUu7Ckkj8tNjtB"
TOPIC = "finacle-transactions"

# ===== CREATE PRODUCER =====
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVER,
    security_protocol="SASL_PLAINTEXT",
    sasl_mechanism="SCRAM-SHA-512",
    sasl_plain_username=USERNAME,
    sasl_plain_password=PASSWORD,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# ===== SAMPLE DATA =====
data = {
    "txn_id": "TXN1001",
    "customer_id": "CUST001",
    "amount": 5000,
    "channel": "UPI",
    "status": "SUCCESS"
}

# ===== SEND TO KAFKA =====
producer.send(TOPIC, value=data)

# make sure it's delivered
producer.flush()

print("✅ Data pushed to Kafka successfully")