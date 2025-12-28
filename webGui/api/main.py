import os, time
from typing import Any, Dict, List
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
import redis.asyncio as redis

# Redis configuration
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
r = redis.from_url(REDIS_URL, decode_responses=True)

app = FastAPI(title="Redis Dashboard API")

# --- TAXI STATE ENDPOINT ---
@app.get("/taxi/{taxi_id}")
async def get_taxi_state(taxi_id: str):
    # 1. ID cleanup: Remove 'taxi:' if included to avoid duplicates
    clean_id = taxi_id.replace("taxi:", "")
    
    # 2. Build the correct key (e.g.: taxi:100:state)
    key = f"taxi:{clean_id}:state"
    
    # 3. Query Redis
    h = await r.hgetall(key)
    
    if not h:
        # Debug log for docker logs
        print(f"DEBUG: Key '{key}' not found in Redis") 
        raise HTTPException(404, f"No hash at {key}")

    # 4. Response formatting
    out: Dict[str, Any] = {k: v for k, v in h.items()}
    ts = out.get("ts")
    try:
        # Try to detect if it's milliseconds or seconds
        ts_val = float(ts) if ts else time.time()
        tsf = ts_val / 1000.0 if ts_val > 1e11 else ts_val
    except:
        tsf = time.time()
        
    out["_ts_readable"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(tsf))
    return JSONResponse(out)

# --- ALERTS ENDPOINT ---
@app.get("/alerts")
async def get_alerts(start: int = Query(0), stop: int = Query(-1)):
    key = "alerts"
    # Read the alerts list (LIFO - Last In First Out normally)
    items: List[str] = await r.lrange(key, start, stop)
    
    parsed = []
    for s in items:
        # Expected format: "type,taxi_id,ts,value"
        parts = s.split(",")
        if len(parts) >= 4:
            typ = parts[0]
            taxi_id = parts[1]
            ts_raw = parts[2]
            value = parts[3]
            
            try:
                ts_val = float(ts_raw)
                ts = ts_val / 1000.0 if ts_val > 1e11 else ts_val
            except:
                ts = time.time()
                
            parsed.append({
                "raw": s,
                "type": typ,
                "taxi_id": taxi_id,
                "timestamp": ts,
                "time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts)),
                "value": value,
            })
        else:
            parsed.append({"raw": s})
            
    return JSONResponse({"items": parsed, "count": len(items)})


# --- WEBSOCKET ALERTS ENDPOINT ---
@app.websocket("/ws/alerts")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    print("DEBUG: WS Client connected", flush=True)
    pubsub = r.pubsub()
    await pubsub.subscribe("alerts_channel")
    print("DEBUG: Subscribed to alerts_channel", flush=True)
    try:
        async for message in pubsub.listen():
            if message["type"] == "message":
                print(f"DEBUG: Sending to WS: {message['data']}", flush=True)
                await websocket.send_text(message["data"])
    except Exception as e:
        print(f"DEBUG: WS Error: {e}", flush=True)
    finally:
        await pubsub.unsubscribe("alerts_channel")
        print("DEBUG: WS Client disconnected", flush=True)
