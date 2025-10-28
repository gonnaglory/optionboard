from typing import Any, Dict, List, Optional
from fastapi import FastAPI, HTTPException, Request, Response
import ujson, logging
import asyncio
from contextlib import asynccontextmanager
from backend.http_client import MOEXClient, get_redis
from backend.config import settings


from fastapi.middleware.cors import CORSMiddleware

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Singletons on app.state
    app.state.moex_client = MOEXClient(base_url="https://iss.moex.com")
    app.state.redis = get_redis()
    sem = asyncio.Semaphore(1)
    
    # Warmup on startup
    try:
        await app.state.moex_client.get_options()

        raw_assets = await app.state.redis.get(f"{settings.REDIS_PREFIX}UNDERLYINGASSETS")
        assets: List[str] = ujson.loads(raw_assets) if raw_assets else []

        async def warm_one(asset: str):
            async with sem:
                try:
                    await app.state.moex_client.load_candles(asset)
                except Exception as e:
                    logging.error(f"Error in get_options: {e}")
                    pass
                finally:
                    await app.state.moex_client.add_params(asset)
        await asyncio.gather(*(warm_one(a) for a in assets))
        
        yield
    finally:
        await app.state.moex_client.close()
        # If your redis client needs closing, do it here (depends on get_redis impl)

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root(request: Request):
    """Root endpoint: показывает доступные базовые активы."""
    r = request.app.state.redis

    try:
        # If you want to ensure the cache is fresh, call this conditionally or remove it:
        # await request.app.state.moex_client.get_options()

        raw = await r.get(f"{settings.REDIS_PREFIX}UNDERLYINGASSETS")
        assets = ujson.loads(raw) if raw else []
        return {"message": "OptionBoard API is running", "available_options": assets}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/favicon.ico")
async def favicon():
    return Response(status_code=204)

@app.get("/{asset}")
async def get_option(asset: str, request: Request):
    r = request.app.state.redis
    calc_key = f"{settings.REDIS_PREFIX}asset:{asset}:calc"
    raw_key  = f"{settings.REDIS_PREFIX}asset:{asset}:raw"

    payload = await r.get(calc_key)
    if not payload:
        payload = await r.get(raw_key)
    if not payload:
        raise HTTPException(status_code=404, detail=f"No data in Redis for asset: {asset}")
    return ujson.loads(payload if isinstance(payload, (str, bytes)) else ujson.dumps(payload))