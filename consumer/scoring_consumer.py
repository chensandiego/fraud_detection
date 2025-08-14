import json
import time
import joblib
from confluent_kafka import Consumer, Producer
import redis

KAFKA_BOOTSTRAP = "kafka:9092"
IN_TOPIC = "transactions"
OUT_TOPIC = "alerts"
GROUP_ID = "fraud_scoring_group"

redis_client = redis.Redis(host="redis", port=6379, db=0, decode_responses=True)

try:
    model = joblib.load("model.joblib")
    print("Loaded model.joblib")
except Exception as e:
    print("No model found", e)
    model = None

c_conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest",
}
consumer = Consumer(c_conf)
consumer.subscribe([IN_TOPIC])
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

THRESHOLD = 0.7


def update_user_features(tx):
    uid = tx["user_id"]
    now = int(time.time())
    key = f"user:{uid}:txs"
    member = f"{tx['tx_id']}|{tx['amount']}"
    redis_client.zadd(key, {member: now})
    cutoff = now - 3600
    redis_client.zremrangebyscore(key, 0, cutoff)
    members = redis_client.zrange(key, 0, -1)
    tx_count = len(members)
    total = sum(float(m.split("|")[1]) for m in members) if tx_count else 0
    avg_amount = total / tx_count if tx_count else 0
    loc_risk = 1 if tx.get("location") not in ("US",) else 0
    redis_client.hset(f"user:{uid}:agg", mapping={
        "tx_count_1h": tx_count,
        "avg_amount_1h": round(avg_amount,2),
        "last_ts": now
    })
    return {"tx_count_1h": tx_count, "avg_amount_1h": avg_amount, "loc_risk": loc_risk}


def score_transaction(tx, feats):
    vec = [[float(tx["amount"]), feats["tx_count_1h"], feats["avg_amount_1h"], feats["loc_risk"]]]
    if model is not None:
        prob = model.predict_proba(vec)[0][1]
        return float(prob)
    # fallback
    if float(tx["amount"]) > 1000 or (feats["tx_count_1h"] >= 3 and tx["amount"] > feats["avg_amount_1h"] * 5):
        return 0.9
    return 0.01


def persist_alert(alert):
    # store alert in Redis
    aid = alert['alert_id']
    ts = alert['detected_at']
    redis_client.hset(f"alert:{aid}", mapping=alert)
    redis_client.zadd("alerts:recent", {aid: ts})


def produce_alert(tx, score, reason="score_threshold"):
    alert = {
        "alert_id": f"alert-{tx['tx_id']}",
        "tx": tx,
        "score": score,
        "reason": reason,
        "detected_at": int(time.time()),
        "fp": 0
    }
    producer.produce(OUT_TOPIC, key=tx["user_id"], value=json.dumps(alert))
    producer.flush()
    persist_alert(alert)
    print("Produced alert:", alert['alert_id'])

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error:", msg.error())
            continue
        tx = json.loads(msg.value())
        if 'tx_id' not in tx:
            continue
        feats = update_user_features(tx)
        score = score_transaction(tx, feats)
        print(f"Scored {tx['tx_id']} user={tx['user_id']} amount={tx['amount']} score={score:.3f}")
        if score >= THRESHOLD:
            produce_alert(tx, score)
except KeyboardInterrupt:
    pass
finally:
    consumer.close()