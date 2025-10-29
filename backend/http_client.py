import ujson, aiofiles, logging, asyncio
from aiohttp import ClientSession, TCPConnector
import redis.asyncio as redis
from pathlib import Path
from typing import Any, List
from datetime import datetime, timedelta, timezone

from backend.dbworker import get_last_candle_date, save_candles
from backend.services import candles, actual_futures, hist_vol
from backend.config import settings
from backend.vectorized_calculations import process_asset_options

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=settings.LOG_LEVEL, format=settings.LOG_FORMAT)

_redis = None

def get_redis():
    global _redis
    if _redis is None:
        _redis = redis.from_url(settings.REDIS_URL, encoding="utf-8", decode_responses=True)
    return _redis

class HTTPClient:
    def __init__(self, base_url: str):
        self._session = ClientSession(
            base_url=base_url,
            connector=TCPConnector(ssl=False)
        )

    async def close(self):
        await self._session.close()


class MOEXClient(HTTPClient):
    async def get_options(self) -> List[str]:
        url = (
            "/iss/engines/futures/markets/options/securities.json"
            "?iss.meta=off&securities.columns="
            "SECID,SHORTNAME,PREVSETTLEPRICE,DECIMALS,MINSTEP,LASTTRADEDATE,"
            "PREVOPENPOSITION,PREVPRICE,OPTIONTYPE,STRIKE,CENTRALSTRIKE,"
            "UNDERLYINGASSET,UNDERLYINGSETTLEPRICE"
        )

        try:
            async with self._session.get(url) as response:
                if response.status != 200:
                    raise RuntimeError(f"MOEX API returned {response.status}")
                raw_data = await response.json()

            columns = raw_data["securities"]["columns"]
            data_rows = raw_data["securities"]["data"]

            # Индексы нужных колонок
            col_indices = {name: idx for idx, name in enumerate(columns)}

            # Получаем список уникальных базовых активов
            assets = sorted({row[col_indices["UNDERLYINGASSET"]] for row in data_rows})

            # Сохраняем список доступных активов
            r = get_redis()
            await r.set(f"{settings.REDIS_PREFIX}UNDERLYINGASSETS", ujson.dumps(assets))

            # Разбиваем данные по активам
            for asset in assets:
                filtered = []
                for row in data_rows:
                    if row[col_indices["UNDERLYINGASSET"]] != asset:
                        continue

                    option = {
                        key: row[col_indices[key]]
                        for key in col_indices
                    }
                    filtered.append(option)
                    
                # Сохраняем сырые данные без расчетных параметров
                await r.set(f"{settings.REDIS_PREFIX}asset:{asset}:raw", ujson.dumps(filtered))
                await r.delete(f"{settings.REDIS_PREFIX}asset:{asset}:calc") # очищаем вычисленные данные, чтобы не отдавать устаревшее
            
            return assets
            
        except Exception as e:
            logger.error(f"Error in get_options: {e}")
            raise
        
    async def add_params(self, asset: str):
        """Добавляет расчетные параметры для всех опционов актива сразу"""
        try:
            r = get_redis()
            raw_key = f"{settings.REDIS_PREFIX}asset:{asset}:raw"
            calc_key = f"{settings.REDIS_PREFIX}asset:{asset}:calc"

            # читаем «сырой» массив опционов из Redis
            raw = await r.get(raw_key)
            if not raw:
                logging.warning("No raw options in Redis for %s", asset)
                return
            options_data = ujson.loads(raw)

            hist = await hist_vol(asset)  # использует БД свечей, как и раньше :contentReference[oaicite:3]{index=3}
            if hist is None:
                logging.warning("No historical volatility for %s", asset)
                return

            updated_options = await process_asset_options(options_data, hist)  # векторные расчёты :contentReference[oaicite:4]{index=4}

            # сохраняем уже «обогащённые» данные в Redis
            await r.set(calc_key, ujson.dumps(updated_options))
            logger.info(f"Added params to {asset} - processed {len(updated_options)} options")
                        
        except Exception as e:
            logger.error(f"Error adding params to {asset}: {e}")
      
    async def load_candles(self, underlying: str, start_date: datetime = None, end_date: datetime = None):
        """Основной цикл загрузки свечей с учётом уже сохранённых данных"""
        # Определяем движок и рынок
        try:
            int(underlying[-1])
            underlying = underlying[:2]
            engine = "futures"
            market = "forts"
        except ValueError:
            engine = "stock"
            market = "shares"

        # Определяем стартовую дату
        last_date = await get_last_candle_date(underlying)
        if last_date is None:
            start_date = (datetime.now() - timedelta(weeks=26))
        else:
            start_date = last_date

        end_date = datetime.now()

        logger.info("Fetching candles for %s from %s to %s",
                    underlying, start_date.date(), end_date.date())

        # Формируем список дат
        dates = []
        current_date = start_date
        while current_date <= end_date:
            dates.append(current_date)
            current_date += timedelta(days=1)
 
        # Параллельная загрузка (ограничиваем количество одновременно выполняемых задач)
        sem = asyncio.Semaphore(5)

        async def sem_task(date):
            async with sem:
                if market == 'forts':
                    act_fut = await actual_futures(underlying, date)
                    raw_candles = await candles(self._session, engine, market, act_fut, date)
                else:
                    raw_candles = await candles(self._session, engine, market, underlying, date)
                if raw_candles:
                    await save_candles(underlying, raw_candles)
                    
        await asyncio.gather(*(sem_task(d) for d in dates))