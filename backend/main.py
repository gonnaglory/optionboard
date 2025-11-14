import logging
import asyncio
from contextlib import asynccontextmanager
from typing import Dict, List, Any
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from backend.http_client import MOEXClient, get_redis, close_redis
from backend.dbworker import close_connection_pool, getdb_candles_fast

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan менеджер для управления жизненным циклом приложения
    """
    # Инициализация клиентов
    app.state.moex_client = MOEXClient(base_url="https://iss.moex.com")
    app.state.redis = await get_redis()

    try:
        await app.state.moex_client.get_options()

        assets = await app.state.redis.smembers("UNDERLYINGASSETS")
        if not assets:
            logger.warning("No assets found in Redis")
            assets = []
        else:
            assets = sorted(list(assets))
            logger.info("Processing %d assets on startup: %s", len(assets), assets)
            
        sem = asyncio.Semaphore(5)
        # Ограниченная параллельная загрузка свечей
        async def load_asset_candles(asset: str):

            async with sem:
                try:
                    await app.state.moex_client.load_candles(asset)
                except Exception as e:
                    logger.error("Error loading candles for %s: %s", asset, e)
                    return False

        # Ограниченное добавление параметров
        async def add_asset_params(asset: str):
            async with sem:
                try:
                    await app.state.moex_client.add_params(asset)
                except Exception as e:
                    logger.error("Error adding params for %s: %s", asset, e)
                    return 0

        # Загрузка свечей
        candles_tasks = [load_asset_candles(asset) for asset in assets]
        await asyncio.gather(*candles_tasks, return_exceptions=True)
        
        # Добавление параметров
        params_tasks = [add_asset_params(asset) for asset in assets]
        await asyncio.gather(*params_tasks, return_exceptions=True)

    except Exception as e:
        logger.error("Error during application startup: %s", e)
        # Не прерываем запуск приложения из-за ошибок инициализации

    # Передача управления FastAPI
    yield
    
    try:
        await app.state.moex_client.close()
        await close_redis()
        await close_connection_pool()
        logger.info("All connections closed")
    except Exception as e:
        logger.error("Error during shutdown: %s", e)

app = FastAPI(
    title="OptionBoard API",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["192.168.0.5"],  # В продакшене заменить на конкретные домены
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
    max_age=3600,
)

# Middleware для логирования запросов
@app.middleware("http")
async def log_requests(request: Request, call_next):
    """
    Middleware для логирования входящих запросов
    """
    start_time = asyncio.get_event_loop().time()
    
    response = await call_next(request)
    
    process_time = asyncio.get_event_loop().time() - start_time
    logger.info(
        "%s %s completed in %.3fs with status %d",
        request.method,
        request.url.path,
        process_time,
        response.status_code
    )
    
    return response

@app.get("/", response_model=List[str])
async def root(request: Request) -> List[str]:
    """
    Корневой endpoint со списком доступных активов
    """
    try:
        red = request.app.state.redis
        assets = await red.smembers("UNDERLYINGASSETS")
        assets_list = sorted(list(assets)) if assets else []
        
        return assets_list
    
    except Exception as e:
        logger.error("Error in root endpoint: %s", e)
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/favicon.ico")
async def favicon() -> Response:
    """
    Endpoint для favicon (возвращает 204 No Content)
    """
    return Response(status_code=204)

@app.get("/{asset}", response_model=Any)
async def get_asset(asset: str, request: Request) -> Dict[str, Any]:
    """
    Endpoint для получения опционов по конкретному активу
    
    Args:
        asset: Базовый актив
        refresh: Принудительное обновление данных
    """
    # Валидация asset
    if not asset or len(asset) > 10:
        raise HTTPException(status_code=400, detail="Invalid asset format")
    
    try:
        red = request.app.state.redis
        
        # Получение дат экспирации
        expirations = await red.smembers(f"idx:{asset}:expirations")
        if not expirations:
            logger.warning("No expirations found for asset %s", asset)
    
        # Пакетное получение опционов для всех дат экспирации
        options = {}
        
        for expiry in expirations:
            try:
                # Получаем SECID для данной даты экспирации
                secids = await red.smembers(f"idx:{asset}:{expiry}")

                # Пакетное чтение данных опционов
                keys = [f"{asset}:{secid}" for secid in secids]
                pipe = red.pipeline(transaction=False)
                
                for key in keys:
                    pipe.json().get(key, "$")
                
                rows = await pipe.execute()
                options.update({expiry: [row[0] for row in rows]})
                
            except Exception as e:
                logger.error("Error processing expiry %s for asset %s: %s", expiry, asset, e)

        return options

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error in get_asset_options for %s: %s", asset, e)
        raise HTTPException(status_code=500, detail=f"Failed to retrieve options for {asset}")

@app.get("/candles/{asset}", response_model=List[Dict])
async def get_candles_endpoint(asset: str, request: Request) -> Dict[str, Any]:
    """
    Endpoint для получения свечей по инструменту
    
    Args:
        asset: Идентификатор инструмента
    """    
    try:
        # Определяем базовый актив
        try:
            int(asset[-1])
            asset = asset[:2]
        except (ValueError, IndexError):
            pass
        
        candles_data = await getdb_candles_fast(asset)
        
        if not candles_data:
            logger.warning("No candles found for %s", asset)
            return
        
        return candles_data
        
    except Exception as e:
        logger.error("Error getting candles for %s: %s", asset, e)
        raise HTTPException(status_code=500, detail=f"Failed to retrieve candles for {asset}")

@app.get("/health", response_model=Dict[str, Any])
async def health_check(request: Request) -> Dict[str, Any]:
    """
    Endpoint для проверки здоровья приложения
    """
    try:
        # Проверяем соединение с Redis
        red = request.app.state.redis
        await red.ping()
        
        # Проверяем наличие активов
        assets = await red.smembers("assets:underlying")
        
        return {
            "status": "healthy",
            "redis": "connected",
            "assets_count": len(assets) if assets else 0,
            "cache_size": len(request.app.state.cache)
        }
    except Exception as e:
        logger.error("Health check failed: %s", e)
        raise HTTPException(status_code=503, detail="Service unavailable")

# Обработчики ошибок
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """
    Обработчик HTTP исключений
    """
    logger.warning("HTTPException: %s %s - %d: %s", 
                  request.method, request.url.path, exc.status_code, exc.detail)
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": exc.detail}
    )

@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """
    Обработчик общих исключений
    """
    logger.error("Unhandled exception in %s %s: %s", 
                request.method, request.url.path, exc, exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"error": "Internal server error"}
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "backend.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )