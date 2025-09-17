import ujson, aiofiles, logging, asyncio
from aiohttp import ClientSession, TCPConnector
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

async def save_json(data: Any, file_path: Path) -> None:
    """Асинхронно сохраняет JSON, только если содержимое изменилось."""
    serialized = ujson.dumps(data, indent=2, ensure_ascii=False)

    # Если файл существует и данные совпадают — не пишем
    if file_path.exists():
        async with aiofiles.open(file_path, "r", encoding="utf-8") as f:
            existing = await f.read()
        if existing == serialized:
            return

    file_path.parent.mkdir(parents=True, exist_ok=True)
    async with aiofiles.open(file_path, "w", encoding="utf-8") as f:
        await f.write(serialized)


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
            await save_json(assets, settings.DATA_FOLDER / "UNDERLYINGASSETS.json")

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
                await save_json(filtered, settings.DATA_FOLDER / f"{asset}.json")
            
            return assets
            
        except Exception as e:
            logger.error(f"Error in get_options: {e}")
            raise
        
    async def add_params(self, asset: str):
        """Добавляет расчетные параметры для всех опционов актива сразу"""
        try:
            asset_file = settings.DATA_FOLDER / f"{asset}.json"
            if not asset_file.exists():
                logger.warning(f"File {asset_file} not found")
                return
            
            # Загружаем данные опционов
            async with aiofiles.open(asset_file, "r", encoding="utf-8") as f:
                options_data = ujson.loads(await f.read())
            
            logger.debug(f"Loaded {len(options_data)} options for {asset}")
            
            # Получаем историческую волатильность
            hist_vol_result = await hist_vol(asset)
            if hist_vol_result is None:
                logger.warning(f"No historical volatility for {asset}")
                return

            hist_vol_value = hist_vol_result  # Берем значение волатильности
            
            # Обрабатываем ВСЕ опционы актива сразу
            updated_options = await process_asset_options(options_data, hist_vol_value)
            
            # Сохраняем обновленные данные
            await save_json(updated_options, asset_file)
            logger.info(f"Added params to {asset} - processed {len(updated_options)} options")
            
            # # Логируем статистику
            # valid_count = sum(1 for opt in updated_options if opt.get('IMPLIED_VOL') is not None)
            # logger.debug(f"Valid calculations: {valid_count}/{len(updated_options)}")
            
        except Exception as e:
            logger.error(f"Error adding params to {asset}: {e}")
    
    # async def add_params(self, asset: str):
    #     """Добавляет расчетные параметры к конкретному активу"""
    #     try:
    #         asset_file = settings.DATA_FOLDER / f"{asset}.json"
    #         if not asset_file.exists():
    #             logger.warning(f"File {asset_file} not found")
    #             return
            
    #         # Загружаем данные опционов
    #         async with aiofiles.open(asset_file, "r", encoding="utf-8") as f:
    #             options_data = ujson.loads(await f.read())
            
    #         logger.debug(f"Loaded {len(options_data)} options for {asset}")
            
    #         # Получаем историческую волатильность
    #         hist_vol_result = await hist_vol(asset)
    #         logger.debug(f"Historical volatility for {asset}: {hist_vol_result}")
            
    #         # Добавляем параметры к каждому опциону
    #         updated_count = 0
    #         for i, option in enumerate(options_data):
    #             success = await self._add_option_params(option, asset, hist_vol_result)
    #             if success:
    #                 updated_count += 1
    #                 if i < 5:  # Логируем первые 5 успешных опционов для отладки
    #                     logger.debug(f"Successfully added params to {option.get('SECID')}")
            
    #         # Сохраняем обновленные данные обратно в файл
    #         await save_json(options_data, asset_file)
    #         logger.info(f"Added params to {asset} - updated {updated_count}/{len(options_data)} options")
            
    #         # Для отладки: выведем первый опцион с параметрами
    #         if updated_count > 0 and len(options_data) > 0:
    #             first_option = options_data[0]
    #             logger.debug(f"First option params: { {k: v for k, v in first_option.items() if k in ['HIST_VOL', 'IMPLIED_VOL', 'THEORETICAL_PRICE', 'DELTA', 'GAMMA', 'VEGA', 'THETA']} }")
            
    #     except Exception as e:
    #         logger.error(f"Error adding params to {asset}: {e}")
    
    # async def _add_option_params(self, option: dict, underlying: str, hist_vol_result) -> bool:
    #     """Добавляет расчетные параметры к одному опциону, возвращает True если успешно"""
    #     try:
    #         # Базовые параметры для расчетов
    #         F0 = option.get('UNDERLYINGSETTLEPRICE', 0)
    #         K = option.get('STRIKE', 0)
    #         expiry_date = option.get('LASTTRADEDATE')
    #         r = settings.IV_RATE
    #         option_type = option.get('OPTIONTYPE', 'C')
    #         prev_settle_price = option.get('PREVSETTLEPRICE', 0)
            
    #         logger.debug(f"Processing option {option.get('SECID')}: F0={F0}, K={K}, expiry={expiry_date}, type={option_type}, prev_price={prev_settle_price}")
            
    #         if F0 <= 0 or K <= 0 or not expiry_date:
    #             logger.debug(f"Skipping option {option.get('SECID')}: invalid F0, K or expiry date")
    #             return False
            
    #         # Добавляем историческую волатильность
    #         if hist_vol_result:
    #             option['HIST_VOL'] = hist_vol_result
    #             logger.debug(f"Added HIST_VOL to {option.get('SECID')}: {hist_vol_result}")
            
    #         # Рассчитываем время до экспирации
    #         T = expiry_time(expiry_date) / (settings.TRADING_DAYS_PER_YEAR * settings.MINUTES_PER_DAY)
    #         logger.debug(f"Time to expiry for {option.get('SECID')}: {T}")
            
    #         if T <= 0:
    #             logger.debug(f"Skipping option {option.get('SECID')}: T <= 0")
    #             return False
            
    #         # # Рассчитываем подразумеваемую волатильность
    #         # iv = await implied_volatility(
    #         #     prev_settle_price=prev_settle_price,
    #         #     underlying_price=F0,
    #         #     strike=K,
    #         #     expiry_time=T,
    #         #     rate=r,
    #         #     option_type=option_type
    #         # )
            
    #         # logger.debug(f"Implied volatility for {option.get('SECID')}: {iv}")
            
    #         # if math.isnan(iv):
    #         #     logger.debug(f"Skipping option {option.get('SECID')}: IV is NaN")
    #         #     return False
            
    #         # Добавляем все расчетные параметры
    #         # option['IMPLIED_VOL'] = round(iv, 4)
    #         theoretical_price = black76_price(F0, K, T, r, hist_vol_result, option_type)
    #         option['THEORETICAL_PRICE'] = round(theoretical_price, 2)
    #         option['DELTA'] = round(delta(F0, K, T, r, hist_vol_result, option_type), 6)
    #         option['GAMMA'] = round(gamma(F0, K, T, r, hist_vol_result), 6)
    #         option['VEGA'] = round(vega(F0, K, T, r, hist_vol_result), 6)
    #         option['THETA'] = round(theta(F0, K, T, r, hist_vol_result, option_type), 6)
            
    #         logger.debug(f"Successfully calculated all params for {option.get('SECID')}")
    #         return True
            
    #     except Exception as e:
    #         logger.error(f"Error adding params to option {option.get('SECID')}: {e}")
    #         return False
    
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
        sem = asyncio.Semaphore(90)

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