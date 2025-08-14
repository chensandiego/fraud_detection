# producer.py
import json
import time
import random
import uuid
from confluent_kafka import Producer

KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC = "transactions"

p = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

def create_tx(user_id=None):
    if user_id is None:
        user_id = f"user_{random.randint(1, 50)}"
    tx = {
        "tx_id": str(uuid.uuid4()),
        "user_id": user_id,
        "amount": round(random.expovariate(1/50), 2),
        "currency": "USD",
        "timestamp": int(time.time()),
        "location": random.choice(["US", "TW", "CN", "JP", "DE"]),
        "merchant": random.choice(["amazon", "walmart", "local_shop", "travelco"]),
    }
    return tx

if __name__ == '__main__':
    for i in range(1000):
        # occasionally spike for testing
        if random.random() < 0.01:
            tx = create_tx("user_5")
            tx["amount"] *= 50
        else:
            tx = create_tx()
        p.produce(TOPIC, key=tx["user_id"], value=json.dumps(tx))
        p.poll(0)
        time.sleep(0.02)
    p.flush()