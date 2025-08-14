# scoring_consumer.py
import json
import time
import joblib
from confluent_kafka import Consumer, Producer
import redis

KAFKA_BOOTSTRAP = "localhost:9092"
IN_TOPIC = "transactions"
OUT_TOPIC = "alerts"
GROUP_ID = "fraud_scoring_group"

# Redis for lightweight per-user feature store (could be replaced with Postgres / feature-store)
redis_client = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

# Try load the model; if missing, use simple rule-based fallback
try:
    model = joblib.load("model.joblib")
    print("Loaded model.joblib")
except Exception as e:
    print("No model found, using rule-based fallback.", e)
    model = None

c_conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest",
}
consumer = Consumer(c_conf)
consumer.subscribe([IN_TOPIC])

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

def update_user_features(tx):
    uid = tx["user_id"]
    now = int(time.time())
    # keep a rolling window in Redis using a sorted set: key=user:{uid}:txs  score=timestamp value=amount:txid
    key = f"user:{uid}:txs"
    member = f"{tx['tx_id']}|{tx['amount']}"
    redis_client.zadd(key, {member: now})
    # expire old entries > 3600 seconds
    cutoff = now - 3600
    redis_client.zremrangebyscore(key, 0, cutoff)
    # compute aggregates
    members = redis_client.zrange(key, 0, -1)
    tx_count = len(members)
    if tx_count == 0:
        avg_amount = 0
    else:
        total = sum(float(m.split("|")[1]) for m in members)
        avg_amount = total / tx_count
    # location risk rudiment: foreign locations flagged
    loc_risk = 1 if tx.get("location") not in ("US",) else 0
    # store quick access aggregates
    redis_client.hset(f"user:{uid}:agg", mapping={
        "tx_count_1h": tx_count,
        "avg_amount_1h": round(avg_amount, 2),
        "last_ts": now
    })
    return {"tx_count_1h": tx_count, "avg_amount_1h": avg_amount, "loc_risk": loc_risk}

def score_transaction(tx, feats):
    # features vector: [amount, tx_count_1h, avg_amount_1h, loc_risk]
    vec = [[float(tx["amount"]), feats["tx_count_1h"], feats["avg_amount_1h"], feats["loc_risk"]]]
    if model is not None:
        prob = model.predict_proba(vec)[0][1]
        return float(prob)
    # fallback rule-based: high amount & high deviation
    if float(tx["amount"]) > 1000 or (feats["tx_count_1h"] >= 3 and tx["amount"] > feats["avg_amount_1h"] * 5):
        return 0.9
    return 0.01

def produce_alert(tx, score, reason="score_threshold"):
    alert = {
        "alert_id": f"alert-{tx['tx_id']}",
        "tx": tx,
        "score": score,
        "reason": reason,
        "detected_at": int(time.time())
    }
    producer.produce(OUT_TOPIC, key=tx["user_id"], value=json.dumps(alert))
    producer.flush()
    print("Produced alert:", alert["alert_id"])

THRESHOLD = 0.7

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error:", msg.error())
            continue
        tx = json.loads(msg.value())
        # make sure schema sanity
        if "tx_id" not in tx:
            print("bad message", tx)
            continue
        feats = update_user_features(tx)
        score = score_transaction(tx, feats)
        print(f"Scored tx {tx['tx_id']} user={tx['user_id']} amount={tx['amount']} score={score:.3f}")
        if score >= THRESHOLD:
            produce_alert(tx, score)
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
