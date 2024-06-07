import time
import json
import uuid
import random
from kafka import KafkaProducer
from datetime import datetime

TOPIC_NAME = "cus-transaction"
CERTS_PATH = "./certs"

producer = KafkaProducer(
    bootstrap_servers=f"kafka-transfer-olubillion.g.aivencloud.com:14283",
    security_protocol="SSL",
    ssl_cafile=f"{CERTS_PATH}/ca.pem",
    ssl_certfile=f"{CERTS_PATH}/service.cert",
    ssl_keyfile=f"{CERTS_PATH}/service.key",
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

transaction_reasons = ["donations", "family support", "bill payment", "personal", "business"]
processed_methods = ["ach", "wire", "interac"]
country = ["Canada", "USA"]
recipient_names = ["Michael", "Jane", "Annabelle", "Bayo", "Chuks", "Dave", "Tola", "Frank", "Grace", "Miwa"]

def produce_messages():
    for i in range(1000):
        transaction = {
            "transaction_id": str(uuid.uuid4()),
            "customer_id": f"customer_{i + 1}",
            "amount": round(1000 * (i + 1), 2),  # example amount
            "transaction_time": datetime.utcnow().isoformat(),
            "transaction_reason": random.choice(transaction_reasons),
            "processed": random.choice(processed_methods),
            "recipient_name": random.choice(recipient_names),
            "country": random.choice(country)
        }
        producer.send(TOPIC_NAME, transaction)
        print(f"Message sent: {transaction}")
        time.sleep(1)

    producer.close()
