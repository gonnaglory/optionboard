import logging, math, asyncio

from typing import Optional, List, Dict
import numpy as np
from aiohttp import ClientSession
from datetime import datetime, timedelta
from calendar import Calendar, THURSDAY

from backend.dbworker import getdb_closes
from backend.config import settings

# ---------------- LOGGER ----------------

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=settings.LOG_LEVEL, format=settings.LOG_FORMAT)

# ---------------- HELPER: ASYNC FETCH ----------------

async def fetch_json(session: ClientSession, url: str, params: dict = None) -> Optional[dict]:
    try:
        async with session.get(url, params=params, timeout=settings.TIMEOUT) as resp:
            if resp.status != 200:
                logger.warning("Fetch failed %s [%s]", url, resp.status)
                return None
            data = await resp.json()
            return data
    except Exception as e:
        logger.exception("Fetch exception %s: %s", url, e)
        return []

# ---------------- MOEX API ----------------

async def candleborders(session: ClientSession, engine: str, market: str, security: str) -> Optional[List[Dict]]:
    logger.debug("Requesting candleborders for %s/%s/%s", engine, market, security)

    url = f"/iss/engines/{engine}/markets/{market}/securities/{security}/candleborders.json"
    j = await fetch_json(session, url)
    if not j:
        return None

    data = j.get("borders", {}).get("data", [])
    columns = j.get("borders", {}).get("columns", [])
    if not data or not columns:
        return None

    idx_interval = columns.index("interval") if "interval" in columns else None
    result = [dict(zip(columns, row)) for row in data if idx_interval is None or row[idx_interval] == 1]
    logger.debug("Got %d candleborders rows", len(result))
    return result

async def candles(session: ClientSession, engine: str, market: str, security: str, date: datetime) -> Optional[List[Dict]]:
    logger.debug("Fetching candles for %s/%s/%s on %s", engine, market, security, date.date())

    base_url = f"/iss/engines/{engine}/markets/{market}/securities/{security}/candles.json"
    from_time = date.strftime("%Y-%m-%d %H:%M")
    till_time = date.replace(hour=23, minute=49).strftime("%Y-%m-%d %H:%M")

    params_list = [
        {"from": from_time, "till": till_time, "interval": 1,
         "iss.meta": "off", "candles.columns": "open,close,high,low,volume,begin", "start": start}
        for start in [0, 500]
    ]

    tasks = [fetch_json(session, base_url, params=p) for p in params_list]
    responses = await asyncio.gather(*tasks)

    all_candles = []
    for resp in responses:
        if not resp:
            continue
        candles_data = resp.get("candles", {}).get("data", [])

        for row in candles_data:
            try:
                ts = datetime.strptime(row[5], "%Y-%m-%d %H:%M:%S") if len(row) > 5 else None
            except (ValueError, IndexError):
                ts = None

            all_candles.append({
                "open": row[0] if len(row) > 0 else None,
                "close": row[1] if len(row) > 1 else None,
                "high": row[2] if len(row) > 2 else None,
                "low": row[3] if len(row) > 3 else None,
                "volume": row[4] if len(row) > 4 else None,
                "timestamp": ts
            })

    logger.debug("Total candles aggregated: %d", len(all_candles))
    return all_candles or None

async def trades(session: ClientSession, engine: str, market: str, security: str) -> Optional[List[Dict]]:
    logger.debug("Fetching trades for %s/%s/%s", engine, market, security)

    url = f"/iss/engines/{engine}/markets/{market}/securities/{security}/trades.json?iss.meta=off"
    j = await fetch_json(session, url)
    if not j:
        return None
    data = j.get("trades", {}).get("data", [])
    columns = j.get("trades", {}).get("columns", [])
    logger.debug("Got %d trades", len(data))
    return [dict(zip(columns, row)) for row in data] if data else None

async def orderbook(session: ClientSession, engine: str, market: str, security: str) -> Optional[List[Dict]]:
    logger.debug("Fetching orderbook for %s/%s/%s", engine, market, security)

    url = f"/iss/engines/{engine}/markets/{market}/securities/{security}/orderbook.json?iss.meta=off"
    j = await fetch_json(session, url)
    if not j:
        return None
    data = j.get("orderbook", {}).get("data", [])
    columns = j.get("orderbook", {}).get("columns", [])
    logger.debug("Got %d orderbook rows", len(data))
    return [dict(zip(columns, row)) for row in data] if data else None

# ---------------- FUTURES CONTRACTS ----------------

def get_third_thursday(year: int, month: int) -> datetime:
    c = Calendar(firstweekday=0)
    thursdays = [day for day, wd in c.itermonthdays2(year, month) if day and wd == THURSDAY]
    return datetime(year, month, thursdays[2])

def get_first_business_day(year: int, month: int) -> datetime:
    first_day = datetime(year, month, 1)
    while first_day.weekday() >= 5:
        first_day += timedelta(days=1)
    return first_day

async def actual_futures(base: str, current_date: datetime) -> str:
    year = current_date.year

    if base in settings.COMMODITIES:
        for m in sorted(settings.ALL_CODES):
            expiry = get_first_business_day(year, m)
            if current_date < expiry:
                sel_month, sel_year = m, year
                break
            if current_date.date() == expiry.date():
                continue
        else:
            sel_month, sel_year = 1, year + 1
        code = settings.ALL_CODES[sel_month]
    else:
        for m in sorted(settings.MONTH_CODES):
            expiry = get_third_thursday(year, m)
            if current_date < expiry:
                sel_month, sel_year = m, year
                break
            if current_date.date() == expiry.date():
                continue
        else:
            sel_month, sel_year = 3, year + 1
        code = settings.MONTH_CODES[sel_month]

    year_code = str(sel_year)[-1]
    return f"{base}{code}{year_code}"

# ---------------- VOLATILITY ----------------

async def hist_vol(underlying: str) -> Optional[float]:
    try:
        # Извлекаем базовый актив из кода фьючерса
        int(underlying[-1])
        underlying = underlying[:2]
    except ValueError:
        pass

    data = await getdb_closes(underlying)

    if not data:
        logger.warning("No data for hist_vol: %s", underlying)
        return None

    # Сортируем по дате
    data_sorted = sorted(data, key=lambda x: x[0])
    
    # Извлекаем цены и проверяем достаточность данных
    prices = np.array([p for _, p in data_sorted], dtype=np.float64)
    
    if len(prices) < 2:
        logger.warning("Not enough price data for hist_vol: %s", underlying)
        return None

    # Вычисляем логарифмические доходности
    log_returns = np.diff(np.log(prices))
    
    window = min(settings.HIST_WINDOW_MINUTES, len(log_returns))
    if window < 10:  # Минимальное окно для надежной волатильности
        logger.warning("Window too small for hist_vol: %d", window)
        return None

    # Эффективный расчет скользящего стандартного отклонения
    try:
        # Используем векторные операции для производительности
        cumsum = np.cumsum(np.insert(log_returns, 0, 0.0))
        cumsum2 = np.cumsum(np.insert(log_returns**2, 0, 0.0))
        
        rolling_mean = (cumsum[window:] - cumsum[:-window]) / window
        rolling_var = (cumsum2[window:] - cumsum2[:-window]) / window - rolling_mean**2
        
        # Избегаем отрицательной дисперсии из-за ошибок округления
        rolling_std = np.sqrt(np.maximum(rolling_var, 0))
        
        # Годовая волатильность
        hist_vol_value = rolling_std[-1] * math.sqrt(settings.TRADING_DAYS_PER_YEAR * settings.MINUTES_PER_DAY)
        
        return float(round(hist_vol_value, 4))
        
    except Exception as e:
        logger.error("Error calculating historical volatility: %s", e)
        return None

def expiry_time(expiry_date_str: str, now: datetime = None) -> int:
    """
    Рассчитывает торговое время до экспирации (в минутах).
    Учитываются рабочие часы, клиринги, выходные и праздники.

    :param expiry_date_str: дата экспирации в формате YYYY-MM-DD
    :param now: текущее время (по умолчанию datetime.now())
    :return: количество минут торгового времени до экспирации
    """
    try:
        expiry_date = datetime.strptime(expiry_date_str, "%Y-%m-%d").date()
    except ValueError:
        logger.error("Invalid expiry date format: %s", expiry_date_str)
        return 0
        
    if now is None:
        now = datetime.now()
        
    today = now.date()

    # Если экспирация уже прошла
    if today > expiry_date or (today == expiry_date and now.time() >= getattr(settings, 'EXPIRY_END', datetime.strptime("18:50", "%H:%M").time())):
        return 0

    total_minutes = 0
    cur_date = today

    # Получаем настройки с значениями по умолчанию
    trading_start = getattr(settings, 'TRADING_START', datetime.strptime("09:00", "%H:%M").time())
    trading_end = getattr(settings, 'TRADING_END', datetime.strptime("23:50", "%H:%M").time())
    expiry_end = getattr(settings, 'EXPIRY_END', datetime.strptime("18:50", "%H:%M").time())
    clearing_periods = getattr(settings, 'CLEARING_PERIODS', [
        (datetime.strptime("14:00", "%H:%M").time(), datetime.strptime("14:05", "%H:%M").time()),
        (datetime.strptime("18:50", "%H:%M").time(), datetime.strptime("19:00", "%H:%M").time())
    ])
    holidays = getattr(settings, 'HOLIDAYS', set())

    while cur_date <= expiry_date:
        # Определить торговые границы для текущего дня
        if cur_date == expiry_date:
            day_start, day_end = trading_start, expiry_end
        else:
            day_start, day_end = trading_start, trading_end

        # Пропуск выходных и праздников
        if cur_date.weekday() >= 5 or cur_date in holidays:
            cur_date += timedelta(days=1)
            continue

        session_start = datetime.combine(cur_date, day_start)
        session_end = datetime.combine(cur_date, day_end)

        # Корректируем старт если начинаем не с утра
        if cur_date == today and now > session_start:
            session_start = max(session_start, now)

        if session_start < session_end:
            minutes = (session_end - session_start).seconds // 60

            # Вычитаем клиринги
            for cl_start, cl_end in clearing_periods:
                cl_start_dt = datetime.combine(cur_date, cl_start)
                cl_end_dt = datetime.combine(cur_date, cl_end)

                # В день экспирации не учитываем вечерний клиринг
                if cur_date == expiry_date and cl_start >= expiry_end:
                    continue

                overlap_start = max(session_start, cl_start_dt)
                overlap_end = min(session_end, cl_end_dt)
                if overlap_start < overlap_end:
                    minutes -= (overlap_end - overlap_start).seconds // 60

            total_minutes += minutes

        cur_date += timedelta(days=1)

    return max(total_minutes, 0)  # Гарантируем неотрицательное значение