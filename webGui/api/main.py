import os, time
from typing import Any, Dict, List
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
import redis.asyncio as redis


REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
r = redis.from_url(REDIS_URL, decode_responses=True)

app = FastAPI(title="Redis Dashboard API")


# --- EXACT MAPPINGS ---
# 1) docker exec -it redis redis-cli hgetall taxi:{id}:state
@app.get("/taxi/{taxi_id}")
async def get_taxi_state(taxi_id: str):
    key = f"taxi:{taxi_id}:state"
    h = await r.hgetall(key)  # exact HGETALL
    if not h:
        raise HTTPException(404, f"No hash at {key}")
    # optional: parse a few fields for nicer UI
    out: Dict[str, Any] = {k: v for k, v in h.items()}
    ts = out.get("ts")
    try:
        tsf = (
            int(ts) / 1000.0
            if ts and len(ts) > 10
            else float(ts)
            if ts
            else time.time()
        )
    except:
        tsf = time.time()
    out["_ts_readable"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(tsf))
    return JSONResponse(out)


# 2) docker exec -it redis redis-cli lrange alerts 0 -1
@app.get("/alerts")
async def get_alerts(start: int = Query(0), stop: int = Query(-1)):
    key = "alerts"
    items: List[str] = await r.lrange(key, start, stop)  # exact LRANGE
    # optional: structured parse for UI convenience
    parsed = []
    for s in items:
        parts = s.split(",")
        if len(parts) == 4:
            typ, taxi_id, ts_ms, value = parts
            try:
                ts = int(ts_ms) / 1000.0
            except:
                ts = time.time()
            parsed.append(
                {
                    "raw": s,
                    "type": typ,
                    "taxi_id": taxi_id,
                    "timestamp": ts,
                    "time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts)),
                    "value": value,
                }
            )
        else:
            parsed.append({"raw": s})
    return JSONResponse({"items": parsed, "count": len(items)})
