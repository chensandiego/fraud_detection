from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import redis
import time

app = FastAPI()
redis_client = redis.Redis(host="redis", port=6379, db=0, decode_responses=True)

class AlertMark(BaseModel):
    false_positive: bool

@app.get("/alerts")
def get_recent_alerts(limit: int = 50):
    # fetch most recent alert ids
    ids = redis_client.zrevrange("alerts:recent", 0, limit - 1)
    alerts = []
    for aid in ids:
        data = redis_client.hgetall(f"alert:{aid}")
        if data:
            # convert some values
            data['score'] = float(data.get('score', 0))
            data['detected_at'] = int(data.get('detected_at', 0))
            data['fp'] = int(data.get('fp', 0))
            alerts.append(data)
    return {"count": len(alerts), "alerts": alerts}

@app.post("/alerts/{alert_id}/mark")
def mark_false_positive(alert_id: str, body: AlertMark):
    key = f"alert:{alert_id}"
    if not redis_client.exists(key):
        raise HTTPException(status_code=404, detail="alert not found")
    if body.false_positive:
        redis_client.hset(key, mapping={"fp": 1, "marked_ts": int(time.time())})
    else:
        redis_client.hset(key, mapping={"fp": 0, "marked_ts": int(time.time())})
    return {"status": "ok", "alert_id": alert_id, "false_positive": body.false_positive}