# producer.py
import json
import time
import random
import uuid
from confluent_kafka import Producer

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "transactions"

p = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})


def create_tx(user_id=None):
    if user_id is None:
        user_id = f"user_{random.randint(1, 20)}"
    tx = {
        "tx_id": str(uuid.uuid4()),
        "user_id": user_id,
        "amount": round(random.expovariate(1 / 50), 2),  # skewed amounts
        "currency": "USD",
        "timestamp": int(time.time()),
        "location": random.choice(["US", "TW", "CN", "JP", "DE"]),
        "merchant": random.choice(["amazon", "walmart", "local_shop", "travelco"]),
        # add more fields as needed
    }
    return tx


def delivery_report(err, msg):
    if err:
        print("Delivery failed:", err)
    else:
        print(f"Produced {msg.key()} -> {msg.topic()} [{msg.partition()}]")


if __name__ == "__main__":
    # replay some normal users and occasional suspicious large amounts
    for i in range(200):
        # occasionally spike amounts for user_5 to test alerts
        if random.random() < 0.02:
            tx = create_tx("user_5")
            tx["amount"] *= 50
        else:
            tx = create_tx()
        p.produce(
            TOPIC, key=tx["user_id"], value=json.dumps(tx), callback=delivery_report
        )
        p.poll(0)  # serve delivery callbacks
        time.sleep(0.05)
    p.flush()
