import logging, asyncio
from aiohttp import ClientSession, TCPConnector, ClientTimeout
import redis.asyncio as redis
from collections import defaultdict
from datetime import datetime, timedelta
from typing import List, Dict, Set, Any

from backend.dbworker import get_last_candle_date, save_candles
from backend.services import candles, actual_futures, hist_vol
from backend.config import settings
from backend.vectorized_calculations import process_asset_options

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=settings.LOG_LEVEL, format=settings.LOG_FORMAT)

# Глобальные объекты с ленивой инициализацией
_redis_client = None
_redis_lock = asyncio.Lock()

async def get_redis():
    """Ленивая инициализация Redis клиента с пулом соединений"""
    global _redis_client
    async with _redis_lock:
        if _redis_client is None:
            _redis_client = redis.Redis(
                host=getattr(settings, 'REDIS_HOST', 'localhost'),
                port=getattr(settings, 'REDIS_PORT', 6379),
                db=getattr(settings, 'REDIS_DB', 0),
                password=getattr(settings, 'REDIS_PASSWORD', None),
                encoding="utf-8",
                decode_responses=True,
                max_connections=getattr(settings, 'REDIS_MAX_CONNECTIONS', 50),
                socket_connect_timeout=5,
                socket_timeout=10,
                retry_on_timeout=True
            )
    return _redis_client

async def close_redis():
    """Закрытие Redis соединения"""
    global _redis_client
    if _redis_client:
        await _redis_client.close()
        _redis_client = None

BASE_FIELDS = {
    "SECID", "SHORTNAME", "PREVSETTLEPRICE", "DECIMALS", "MINSTEP", "LASTTRADEDATE",
    "PREVOPENPOSITION", "PREVPRICE", "OPTIONTYPE", "STRIKE", "CENTRALSTRIKE",
    "UNDERLYINGASSET", "UNDERLYINGSETTLEPRICE"
}

class HTTPClient:
    """Базовый HTTP клиент с пулом соединений"""
    
    def __init__(self, base_url: str):
        self._session = ClientSession(
            base_url=base_url,
            connector=TCPConnector(
                ssl=False,
                limit=100,
                limit_per_host=20,
                ttl_dns_cache=300,
                keepalive_timeout=30
            ),
            timeout=ClientTimeout(30)
        )

    async def close(self):
        """Закрытие сессии"""
        if not self._session.closed:
            await self._session.close()


class MOEXClient(HTTPClient):
    """Клиент для работы с MOEX API"""
    
    async def get_options(self) -> List[Dict[str, Any]]:
        """
        Получение списка опционов с сохранением в Redis
        
        Returns:
            Список опционов
        """
        url = (
            "/iss/engines/futures/markets/options/securities.json"
            "?iss.meta=off&securities.columns="
            "SECID,SHORTNAME,PREVSETTLEPRICE,DECIMALS,MINSTEP,LASTTRADEDATE,"
            "PREVOPENPOSITION,PREVPRICE,OPTIONTYPE,STRIKE,CENTRALSTRIKE,"
            "UNDERLYINGASSET,UNDERLYINGSETTLEPRICE"
        )

        async with self._session.get(url) as response:
            if response.status != 200:
                raise RuntimeError(f"MOEX API returned {response.status}")
            raw = await response.json()

        data_rows = raw.get("securities", {}).get("data", [])
        if not data_rows:
            logger.warning("No options data received from MOEX")
            return []

        red = await get_redis()
        
        # Подготовка данных для Redis
        assets: Set[str] = set()
        docs: List[tuple[str, dict]] = []
        per_asset_all: Dict[str, Set[str]] = defaultdict(set)
        per_asset_expiries: Dict[str, Set[str]] = defaultdict(set)
        per_asset_expiry_options: Dict[tuple[str, str], Set[str]] = defaultdict(set)

        for row in data_rows:
            if len(row) < 13:
                continue
                
            try:
                secid = str(row[0] or "").strip()
                asset = str(row[11] or "").strip()
                expiry = str(row[5] or "").strip()
                
                if not secid or not asset:
                    continue

                # Создаем документ опциона
                doc = {
                    "SECID": secid,
                    "SHORTNAME": str(row[1] or ""),
                    "PREVSETTLEPRICE": float(row[2] or 0) if row[2] else 0.0,
                    "DECIMALS": int(row[3] or 0),
                    "MINSTEP": float(row[4] or 0),
                    "LASTTRADEDATE": expiry,
                    "PREVOPENPOSITION": int(row[6] or 0),
                    "PREVPRICE": float(row[7] or 0),
                    "OPTIONTYPE": str(row[8] or ""),
                    "STRIKE": float(row[9] or 0),
                    "CENTRALSTRIKE": float(row[10] or 0),
                    "UNDERLYINGASSET": asset,
                    "UNDERLYINGSETTLEPRICE": float(row[12] or 0),
                }
                
                key = f"{asset}:{secid}"
                docs.append((key, doc))
                assets.add(asset)
                
                # Обновляем индексы
                per_asset_all[asset].add(secid)
                if expiry:
                    per_asset_expiries[asset].add(expiry)
                    per_asset_expiry_options[(asset, expiry)].add(secid)
                    
            except (ValueError, TypeError, IndexError) as e:
                logger.warning("Invalid option data skipped: %s, error: %s", row, e)
                continue

        # Пакетное сохранение в Redis
        try:
            pipe = red.pipeline(transaction=False)
            
            # Сохраняем активы
            if assets:
                await red.sadd("UNDERLYINGASSETS", *assets)
            
            # Сохраняем документы опционов
            for key, doc in docs:
                pipe.json().set(key, "$", doc)
                pipe.expire(key, 3600)  # TTL 1 час
            
            # Сохраняем индексы
            for asset, secids in per_asset_all.items():
                idx_all = f"idx:{asset}"
                if secids:
                    pipe.sadd(idx_all, *secids)
                    pipe.expire(idx_all, 3600)
            
            for asset, expiries in per_asset_expiries.items():
                idx_exp = f"idx:{asset}:expirations"
                if expiries:
                    pipe.sadd(idx_exp, *expiries)
                    pipe.expire(idx_exp, 3600)
            
            for (asset, expiry), secids in per_asset_expiry_options.items():
                idx_one = f"idx:{asset}:{expiry}"
                if secids:
                    pipe.sadd(idx_one, *secids)
                    pipe.expire(idx_one, 3600)

            # Выполняем все команды
            await pipe.execute()
            
        except Exception as e:
            logger.error("Failed to save options to Redis: %s", e)
            raise

    async def add_params(self, asset: str) -> int:
        """
        Добавление расчетных параметров к опционам актива
        
        Args:
            asset: Базовый актив
            
        Returns:
            Количество обновленных опционов
        """
        red = await get_redis()
        updated_count = 0

        try:
            # Получаем список SECID для актива
            secids = await red.smembers(f"idx:{asset}")
            if not secids:
                logger.warning("No options found for asset %s", asset)
                return 0

            # Подготавливаем ключи для массового чтения
            keys = [f"{asset}:{secid}" for secid in secids]

            # Массовое чтение документов
            pipe = red.pipeline(transaction=False)
            for key in keys:
                pipe.json().get(key, "$")
            
            results = await pipe.execute()

            # Обрабатываем результаты
            options = []
            valid_keys = []
            
            for key, result in zip(keys, results):
                if not result:
                    continue
                    
                doc = result[0] if isinstance(result, list) and result else result
                if isinstance(doc, dict):
                    doc["_key"] = key  # Сохраняем ключ для обновления
                    options.append(doc)
                    valid_keys.append(key)

            if not options:
                logger.warning("No valid option records for asset %s", asset)
                return 0

            # Получаем историческую волатильность
            hv = await hist_vol(asset)
            if hv is None:
                logger.warning("No historical volatility for %s, using default", asset)

            # Обогащаем расчетными параметрами
            enriched_options = await process_asset_options(options, hv)

            # Массовое обновление в Redis
            update_pipe = red.pipeline(transaction=False)
            for option in enriched_options:
                key = option.get("_key")
                if not key:
                    continue
                    
                # Убираем служебное поле
                option.pop("_key", None)
                
                update_pipe.json().set(key, "$", option)
                update_pipe.expire(key, getattr(settings, 'REDIS_DATA_TTL', 3600))
                updated_count += 1

            if updated_count > 0:
                await update_pipe.execute()
                logger.info("Updated %d options for %s", updated_count, asset)
            else:
                logger.warning("No options updated for %s", asset)

        except Exception as e:
            logger.exception("Error in add_params for %s: %s", asset, e)
            # Не прерываем выполнение для других активов

    async def load_candles(self, underlying: str) -> bool:
        """
        Загрузка свечей для базового актива
        
        Args:
            underlying: Базовый актив
            days_back: Количество дней для загрузки (по умолчанию из настроек)
            
        Returns:
            True если успешно, False при ошибке
        """

        # Определяем движок и рынок
        try:
            int(underlying[-1])
            base_underlying = underlying[:2]
            engine = "futures"
            market = "forts"
            is_futures = True
        except (ValueError, IndexError):
            base_underlying = underlying
            engine = "stock"
            market = "shares"
            is_futures = False

        # Определяем диапазон дат
        end_date = datetime.now()
        last_date = await get_last_candle_date(base_underlying)
        
        if last_date:
            start_date = last_date + timedelta(minutes=1)
        else:
            start_date = end_date.replace(hour=7, minute=0, second=0, microsecond=0) - timedelta(weeks=8)

        # Если start_date позже end_date, нет новых данных
        if start_date >= end_date:
            logger.debug("No new data needed for %s", base_underlying)
            return True

        # Формируем список дат для загрузки
        dates = []
        dates.append(start_date)
        current_date = start_date.replace(hour=6, minute=59)
        
        while current_date.date() < end_date.date():
            current_date += timedelta(days=1)
            # Пропускаем выходные
            if current_date.weekday() < 5:  # 0-4 = понедельник-пятница
                dates.append(current_date)

        if not dates:
            logger.info("No trading days to load for %s", base_underlying)
            return True
        
        logger.info("Fetching candles for %s from %s to %s", 
            base_underlying, start_date, end_date.date())

        # Ограничиваем параллелизм
        sem = asyncio.Semaphore(10)

        async def load_candles_for_date(date: datetime) -> bool:
            """Загрузка свечей для конкретной даты"""
            async with sem:
                try:
                    if is_futures:
                        # Для фьючерсов получаем актуальный контракт
                        actual_future = await actual_futures(base_underlying, date)
                        raw_candles = await candles(self._session, engine, market, actual_future, date)
                    else:
                        raw_candles = await candles(self._session, engine, market, base_underlying, date)
                    
                    if raw_candles:
                        await save_candles(base_underlying, raw_candles)
                        return True
                    else:
                        logger.warning("No candles for %s on %s", base_underlying, date.date())
                        return False
                        
                except Exception as e:
                    logger.error("Error loading candles for %s on %s: %s", 
                                base_underlying, date.date(), e)
                    return False

        # Параллельная загрузка с ограничением
        tasks = [load_candles_for_date(date) for date in dates]
        
        await asyncio.gather(*tasks, return_exceptions=True)