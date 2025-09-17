import logging, math, re, asyncio, time
from typing import Optional, List, Dict
import numpy as np
from aiohttp import ClientSession
from aiocache import cached, SimpleMemoryCache
from scipy.stats import norm
from scipy.optimize import brentq
from datetime import datetime, timedelta
from calendar import Calendar, THURSDAY

from backend.dbworker import get_candles_from_db
from backend.config import settings

# ---------------- LOGGER ----------------

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=settings.LOG_LEVEL, format=settings.LOG_FORMAT)

# ---------------- VALIDATION ----------------

_VALID_NAME_RE = re.compile(r"^[A-Za-z0-9_\-\.]+$")


def _validate_name(name: str) -> bool:
    return bool(_VALID_NAME_RE.match(name))

# ---------------- HELPER: ASYNC FETCH ----------------

async def fetch_json(session: ClientSession, url: str, params: dict = None) -> Optional[dict]:
    start_ts = time.perf_counter()
    try:
        async with session.get(url, params=params, timeout=settings.TIMEOUT) as resp:
            if resp.status != 200:
                logger.debug("Fetch failed %s [%s]", url, resp.status)
                return None
            data = await resp.json()
            elapsed = time.perf_counter() - start_ts
            logger.debug("Fetched %s in %.3f sec", url, elapsed)
            return data
    except Exception as e:
        logger.exception("Fetch exception %s: %s", url, e)
        return None

# ---------------- MOEX API ----------------

async def candleborders(session: ClientSession, engine: str, market: str, security: str) -> Optional[List[Dict]]:
    logger.debug("Requesting candleborders for %s/%s/%s", engine, market, security)
    if not (_validate_name(security) and _validate_name(market)):
        logger.warning("Invalid security or market name: %s %s", security, market)
        return None

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
    if not (_validate_name(security) and _validate_name(market)):
        logger.warning("Invalid name in candles: %s %s", security, market)
        return None

    base_url = f"/iss/engines/{engine}/markets/{market}/securities/{security}/candles.json"
    from_time = date.replace(hour=8, minute=59).strftime("%Y-%m-%d %H:%M")
    till_time = date.replace(hour=23, minute=49).strftime("%Y-%m-%d %H:%M")

    params_list = [
        {"from": from_time, "till": till_time, "interval": 1,
         "iss.meta": "off", "candles.columns": "open,close,high,low,volume,begin", "start": start}
        for start in [0, 500]
    ]

    start_ts = time.perf_counter()
    tasks = [fetch_json(session, base_url, params=p) for p in params_list]
    responses = await asyncio.gather(*tasks)
    elapsed = time.perf_counter() - start_ts
    logger.debug("Fetched candles in %.3f sec", elapsed)

    all_candles = []
    for resp in responses:
        if not resp:
            continue
        candles_data = resp.get("candles", {}).get("data", [])
        logger.debug("Got %d rows of candles", len(candles_data))
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
    if not (_validate_name(security) and _validate_name(market)):
        return None
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
    if not (_validate_name(security) and _validate_name(market)):
        return None
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
    """Return last historical volatility for ``underlying`` or ``None``.

    The function fetches OHLC candles from the database and calculates the
    rolling standard deviation of log returns. If there is not enough data to
    perform the calculation, ``None`` is returned instead of an empty list.
    """
    logger.debug("Calculating hist_vol for %s", underlying)
    try:
        int(underlying[-1])
        underlying = underlying[:2]
    except ValueError:
        pass

    t0 = time.perf_counter()
    data = await get_candles_from_db(underlying)
    logger.debug("DB fetch took %.3f sec", time.perf_counter() - t0)
    if not data:
        logger.debug("No data for hist_vol: %s", underlying)
        return None  # no candle data available

    logger.debug("hist_vol got %d records from DB", len(data))

    t1 = time.perf_counter()
    data_sorted = sorted(data, key=lambda x: x[0])
    logger.debug("Sorting took %.3f sec", time.perf_counter() - t1)

    t2 = time.perf_counter()
    timestamps = np.array([t for t, _ in data_sorted])
    prices = np.array([p for _, p in data_sorted], dtype=np.float64)
    log_returns = np.diff(np.log(prices))
    logger.debug("NumPy prep took %.3f sec", time.perf_counter() - t2)

    window = settings.HIST_WINDOW_MINUTES
    if len(log_returns) < window:
        logger.debug("Not enough data for hist_vol: %d < %d", len(log_returns), window)
        return None  # insufficient data for window

    t3 = time.perf_counter()
    cumsum = np.cumsum(np.insert(log_returns, 0, 0.0))
    cumsum2 = np.cumsum(np.insert(log_returns**2, 0, 0.0))
    n = window
    rolling_mean = (cumsum[n:] - cumsum[:-n]) / n
    rolling_var = (cumsum2[n:] - cumsum2[:-n]) / n - rolling_mean**2
    rolling_std = np.sqrt(rolling_var)
    hist_vol = np.round(
        rolling_std * math.sqrt(settings.TRADING_DAYS_PER_YEAR * settings.MINUTES_PER_DAY),
        4,
    )
    vol_timestamps = timestamps[1:][n - 1:]
    logger.debug("NumPy calc took %.3f sec", time.perf_counter() - t3)

    t4 = time.perf_counter()
    # await save_column(underlying, "hist_vol", list(zip(vol_timestamps, hist_vol)))
    # logger.debug("DB save took %.3f sec", time.perf_counter() - t4)

    logger.debug(
        "hist_vol calculation done in %.3f sec, last value=%.4f",
        time.perf_counter() - t0,
        hist_vol[-1],
    )
    return float(hist_vol[-1])

def expiry_time(expiry_date: str) -> int:
    """
    Рассчитывает торговое время до экспирации (в минутах).
    Учитываются рабочие часы, клиринги, выходные и праздники.

    :param expiry_date_str: дата экспирации в формате YYYY-MM-DD
    :param now: текущее время (по умолчанию datetime.now())
    :return: количество минут торгового времени до экспирации
    """
    expiry_date = datetime.strptime(expiry_date, "%Y-%m-%d").date()
    now = datetime.now()
    today = now.date()

    # если экспирация уже прошла
    if now.date() > expiry_date or (now.date() == expiry_date and now.time() >= settings.EXPIRY_END):
        return 0

    total_minutes = 0
    cur_date = today

    while cur_date <= expiry_date:
        # определить торговые границы для текущего дня
        if cur_date == expiry_date:
            day_start, day_end = settings.TRADING_START, settings.EXPIRY_END
        else:
            day_start, day_end = settings.TRADING_START, settings.TRADING_END

        # пропуск выходных и праздников
        if cur_date.weekday() >= 5 or cur_date in settings.HOLIDAYS:
            cur_date += timedelta(days=1)
            continue

        session_start = datetime.combine(cur_date, day_start)
        session_end = datetime.combine(cur_date, day_end)

        # корректируем старт если начинаем не с утра
        if cur_date == today and now > session_start:
            session_start = max(session_start, now)

        if session_start < session_end:
            minutes = (session_end - session_start).seconds // 60

            # вычитаем клиринги
            for cl_start, cl_end in settings.CLEARING_PERIODS:
                cl_start_dt = datetime.combine(cur_date, cl_start)
                cl_end_dt = datetime.combine(cur_date, cl_end)

                # в день экспирации не учитываем вечерний клиринг (торги завершаются в 18:50)
                if cur_date == expiry_date and cl_start >= settings.EXPIRY_END:
                    continue

                overlap_start = max(session_start, cl_start_dt)
                overlap_end = min(session_end, cl_end_dt)
                if overlap_start < overlap_end:
                    minutes -= (overlap_end - overlap_start).seconds // 60

            total_minutes += minutes

        cur_date += timedelta(days=1)

    return total_minutes

# ---------------- BLACK-76 ----------------

def black76_price(F0: float, K: float, T: float, r: float, sigma: float, option_type: str = 'C') -> float:
    if T <= 0 or sigma <= 0:
        intrinsic = max(F0 - K, 0) if option_type == 'C' else max(K - F0, 0)
        return math.exp(-r * T) * intrinsic

    d1 = (math.log(F0 / K) + 0.5 * sigma ** 2 * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    df = math.exp(-r * T)
    if option_type == 'C':
        return df * (F0 * norm.cdf(d1) - K * norm.cdf(d2))
    else:
        return df * (K * norm.cdf(-d2) - F0 * norm.cdf(-d1))


# ---------------- IMPLIED VOLATILITY ----------------

@cached(ttl=600, cache=SimpleMemoryCache)
async def implied_volatility(prev_settle_price: float,
                             underlying_price: float,
                             strike: float,
                             expiry_time: float,
                             rate: float = settings.IV_RATE,
                             option_type: str = 'C',
                             tol: float = settings.IV_TOL,
                             maxiter: int = settings.IV_MAXITER) -> float:
    if expiry_time < 0 or underlying_price <= 0 or strike <= 0:
        return float('nan')
    if option_type not in ('C', 'P'):
        return float('nan')

    if expiry_time == 0:
        intrinsic = black76_price(underlying_price, strike, expiry_time, rate, 1e-12, option_type)
        return 0.0 if abs(prev_settle_price - intrinsic) < tol else float('nan')

    def objective(vol):
        return black76_price(underlying_price, strike, expiry_time, rate, vol, option_type) - prev_settle_price

    try:
        iv = brentq(objective, settings.IV_VOL_LOWER, settings.IV_VOL_UPPER, xtol=tol, maxiter=maxiter)
        return float(iv) if iv >= 0 else float('nan')
    except Exception as e:
        logger.debug("IV root finding failed: %s", e)
        return float('nan')


# ---------------- GREEKS ----------------

def _validate_positive(*values) -> bool:
    return all(v is not None and (not isinstance(v, float) or not math.isnan(v)) and v > 0 for v in values)


def delta(F0: float, K: float, T: float, r: float, sigma: float, option_type: str = 'C') -> float:
    if not _validate_positive(F0, K, sigma) or T <= 0:
        return 0.0
    d1 = (math.log(F0 / K) + 0.5 * sigma ** 2 * T) / (sigma * math.sqrt(T))
    discount = math.exp(-r * T)
    return round(discount * norm.cdf(d1) if option_type == 'C' else -discount * norm.cdf(-d1), 5)


def gamma(F0: float, K: float, T: float, r: float, sigma: float) -> float:
    if not _validate_positive(F0, K, sigma) or T <= 0:
        return 0.0
    d1 = (math.log(F0 / K) + 0.5 * sigma ** 2 * T) / (sigma * math.sqrt(T))
    discount = math.exp(-r * T)
    return round(discount * norm.pdf(d1) / (F0 * sigma * math.sqrt(T)), 5)


def vega(F0: float, K: float, T: float, r: float, sigma: float) -> float:
    if not _validate_positive(F0, K, sigma) or T <= 0:
        return 0.0
    d1 = (math.log(F0 / K) + 0.5 * sigma ** 2 * T) / (sigma * math.sqrt(T))
    discount = math.exp(-r * T)
    return (discount * F0 * norm.pdf(d1) * math.sqrt(T)) / 100


def theta(F0: float, K: float, T_minutes: float, r: float, sigma: float, option_type: str = 'C') -> float:
    # Преобразуем минуты в годы
    T_years = T_minutes / (settings.TRADING_DAYS_PER_YEAR * settings.MINUTES_PER_DAY)
    
    if T_years <= 0 or not _validate_positive(F0, K, sigma):
        return 0.0
    
    d1 = (math.log(F0 / K) + 0.5 * sigma ** 2 * T_years) / (sigma * math.sqrt(T_years))
    d2 = d1 - sigma * math.sqrt(T_years)
    discount = math.exp(-r * T_years)
    
    if option_type == 'C':
        term2 = -r * discount * (F0 * norm.cdf(d1) - K * norm.cdf(d2))
    else:
        term2 = -r * discount * (-F0 * norm.cdf(-d1) + K * norm.cdf(-d2))
    
    term1 = -discount * F0 * norm.pdf(d1) * sigma / (2 * math.sqrt(T_years))
    
    # THETA в годовом выражении (original)
    theta_annual = term1 + term2
    
    # Преобразуем годовую THETA в дневную
    # Делим на количество торговых дней в году
    theta_daily = theta_annual / settings.TRADING_DAYS_PER_YEAR
    
    return round(theta_daily, 6)