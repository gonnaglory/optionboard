# dbworker.py
import logging, asyncio
from datetime import datetime
from typing import List, Tuple, Optional, Dict, Any
from contextlib import asynccontextmanager

from clickhouse_connect import get_client
from clickhouse_connect.driver.asyncclient import AsyncClient

# Импорт настроек
from backend.config import settings

# --- Логирование ---
logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(
        level=getattr(settings, "LOG_LEVEL", "INFO"),
        format=getattr(settings, "LOG_FORMAT", "%(asctime)s %(levelname)s [%(name)s] %(message)s")
    )


# --- Пул соединений и кэширование ---
class ClickHouseConnectionPool:
    """Оптимизированный пул асинхронных соединений для ClickHouse"""
    
    def __init__(self, max_connections: int = 10):
        self.max_connections = max_connections
        self._pool = asyncio.Queue(max_connections)
        self._created_connections = 0
        self._lock = asyncio.Lock()
        
    async def get_connection(self) -> AsyncClient:
        """Получить соединение из пула или создать новое"""
        if not self._pool.empty():
            return await self._pool.get()
            
        async with self._lock:
            if self._created_connections < self.max_connections:
                client = await self._create_async_client()
                self._created_connections += 1
                return client
                
        # Ждем освобождения соединения
        return await self._pool.get()
    
    async def return_connection(self, client: AsyncClient):
        """Вернуть соединение в пул"""
        try:
            self._pool.put_nowait(client)
        except asyncio.QueueFull:
            # Если пул полный - закрываем соединение
            await client.close()
            async with self._lock:
                self._created_connections -= 1
    
    async def _create_async_client(self) -> AsyncClient:
        """Создать оптимизированное асинхронное соединение"""
        base_client = get_client(
            host=getattr(settings, "CLICKHOUSE_HOST", "localhost"),
            port=getattr(settings, "CLICKHOUSE_PORT", 8123),
            username=getattr(settings, "CLICKHOUSE_USER", "default"),
            password=getattr(settings, "CLICKHOUSE_PASSWORD", ""),
            database=getattr(settings, "CLICKHOUSE_DATABASE", "default"),
            secure=getattr(settings, "CLICKHOUSE_SECURE", False),
            connect_timeout=5,
            settings={
                'async_insert': 1,
                'wait_for_async_insert': 0,
                'max_execution_time': 30,
                'max_block_size': 10000,
                'prefer_localhost_replica': 1,
                'use_uncompressed_cache': 1,
                'load_balancing': 'random'
            }
        )
        return AsyncClient(client=base_client)
    
    async def close_all(self):
        """Закрыть все соединения в пуле"""
        while not self._pool.empty():
            client = await self._pool.get()
            await client.close()
        self._created_connections = 0


# Глобальный пул соединений
_connection_pool = ClickHouseConnectionPool(
    max_connections=getattr(settings, "CLICKHOUSE_MAX_CONNECTIONS", 10)
)


@asynccontextmanager
async def get_db_connection():
    """Контекстный менеджер для работы с БД"""
    client = None
    try:
        client = await _connection_pool.get_connection()
        yield client
    finally:
        if client:
            await _connection_pool.return_connection(client)


async def close_connection_pool():
    """Закрыть пул соединений при завершении приложения"""
    await _connection_pool.close_all()


# --- Кэш таблиц и материализованных представлений ---
_table_cache = {}
_mv_cache = {}


def _table_name(underlying: str) -> str:
    """Генерация безопасного имени таблицы с кэшированием"""
    if underlying in _table_cache:
        return _table_cache[underlying]
        
    # Безопасное преобразование имени
    safe = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in underlying)
    table_name = f"{safe}_candles"
    _table_cache[underlying] = table_name
    return table_name


def _mv_name(underlying: str) -> str:
    """Генерация имени материализованного представления"""
    if underlying in _mv_cache:
        return _mv_cache[underlying]
        
    tbl = _table_name(underlying)
    mv_name = f"{tbl}_mv"
    _mv_cache[underlying] = mv_name
    return mv_name


async def _ensure_table(underlying: str) -> str:
    """
    Создаёт оптимизированную таблицу, если её нет.
    """
    tbl = _table_name(underlying)
    
    async with get_db_connection() as client:
        try:
            # Проверяем существование таблицы
            check_result = await client.query(f"""
                SELECT name 
                FROM system.tables 
                WHERE database = currentDatabase() AND name = '{tbl}'
            """)
            
            if check_result.row_count == 0:
                # Оптимизированная структура таблицы
                create_sql = f"""
                CREATE TABLE IF NOT EXISTS {tbl} (
                    timestamp   DateTime64(0) CODEC(Delta, ZSTD),
                    open        Float64 CODEC(Gorilla, ZSTD),
                    high        Float64 CODEC(Gorilla, ZSTD),
                    low         Float64 CODEC(Gorilla, ZSTD),
                    close       Float64 CODEC(Gorilla, ZSTD),
                    volume      UInt64 CODEC(Delta, ZSTD),
                    ingested_at DateTime64(3) DEFAULT now() CODEC(Delta, ZSTD)
                )
                ENGINE = ReplacingMergeTree(ingested_at)
                PARTITION BY toYYYYMM(timestamp)
                ORDER BY (timestamp)
                SETTINGS 
                    index_granularity = 8192,
                    min_rows_for_wide_part = 100000,
                    min_bytes_for_wide_part = 10000000
                """
                await client.command(create_sql)
                logger.info("Created optimized table: %s", tbl)
                
        except Exception as e:
            logger.error("Error ensuring table %s: %s", tbl, e)
            raise
    
    return tbl


async def create_candles_materialized_view(underlying: str) -> bool:
    """
    Создает материализованное представление для ускорения запросов.
    """
    try:
        tbl = _table_name(underlying)
        mv_name = _mv_name(underlying)
        
        async with get_db_connection() as client:
            # Проверяем существование MV
            check = await client.query(f"""
                SELECT name FROM system.tables 
                WHERE database = currentDatabase() AND name = '{mv_name}'
            """)
            
            if check.row_count == 0:
                # Создаем материализованное представление
                await client.command(f"""
                    CREATE MATERIALIZED VIEW IF NOT EXISTS {mv_name}
                    ENGINE = ReplacingMergeTree(ingested_at)
                    PARTITION BY toYYYYMM(timestamp)
                    ORDER BY (timestamp)
                    POPULATE
                    AS SELECT
                        timestamp,
                        open,
                        high,
                        low,
                        close,
                        volume,
                        ingested_at
                    FROM {tbl}
                """)
                logger.info("Created materialized view %s for faster queries", mv_name)
                
        return True
        
    except Exception as e:
        logger.warning("Could not create materialized view for %s: %s", underlying, e)
        return False


async def _execute_command(sql: str):
    """Выполнить команду без возврата данных"""
    async with get_db_connection() as client:
        return await client.command(sql)


async def _execute_query(sql: str, params: dict = None):
    """Выполнить запрос и вернуть результат"""
    async with get_db_connection() as client:
        return await client.query(sql, parameters=params)


# --- Публичное API ---

async def save_candles(underlying: str, candles: List[dict]) -> bool:
    """
    Массовая вставка свечей с оптимизацией производительности.
    
    Args:
        underlying: Базовый актив
        candles: Список свечей с полями timestamp, open, high, low, close, volume
    
    Returns:
        True если успешно, False при ошибке
    """
    if not candles:
        return True

    try:
        tbl = await _ensure_table(underlying)
        
        # Подготовка данных пакетом
        rows = []
        now = datetime.utcnow()
        
        for candle in candles:
            ts = candle.get("timestamp")
            if not ts:
                continue
                
            # Валидация данных
            try:
                rows.append([
                    ts,
                    float(candle.get("open", 0.0)),
                    float(candle.get("high", 0.0)),
                    float(candle.get("low", 0.0)),
                    float(candle.get("close", 0.0)),
                    int(candle.get("volume", 0)),
                    now
                ])
            except (ValueError, TypeError) as e:
                logger.warning("Invalid candle data skipped: %s, error: %s", candle, e)
                continue

        if not rows:
            logger.warning("No valid candles to save for %s", underlying)
            return False

        # Массовая вставка с оптимизацией
        async with get_db_connection() as client:
            await client.insert(
                tbl,
                rows,
                column_names=["timestamp", "open", "high", "low", "close", "volume", "ingested_at"],
            )
        
        logger.debug("Saved %d candles for %s", len(rows), underlying)
        return True
        
    except Exception as e:
        logger.error("Failed to save candles for %s: %s", underlying, e)
        return False


async def get_last_candle_date(underlying: str) -> Optional[datetime]:
    """
    Возвращает максимальный timestamp по таблице или None, если данных нет.
    """
    try:
        tbl = await _ensure_table(underlying)
        
        result = await _execute_query(f"""
            SELECT maxOrNull(timestamp) 
            FROM {tbl}
            SETTINGS max_threads = 1
        """)
        
        if not result or result.row_count == 0:
            return None

        max_ts = result.first_item.get('maxOrNull(timestamp)')
        return max_ts if max_ts else None
        
    except Exception as e:
        logger.error("Error getting last candle date for %s: %s", underlying, e)
        return None


async def getdb_closes(underlying: str) -> Optional[List[Tuple[datetime, float]]]:
    """
    Возвращает список (timestamp, close) для расчета исторической волатильности.
    
    Args:
        underlying: Базовый актив
    
    Returns:
        Список кортежей (timestamp, close) отсортированный по возрастанию времени
    """
    try:
        tbl = await _ensure_table(underlying)
        
        # Оптимизированный запрос для получения цен закрытия
        result = await _execute_query(f"""
            SELECT
                timestamp,
                close
            FROM {tbl}
            ORDER BY timestamp ASC
            LIMIT {settings.HIST_WINDOW_MINUTES + 1}
            SETTINGS 
                max_threads = 1,
                optimize_read_in_order = 1
        """)

        if not result or result.row_count == 0:
            logger.debug("No close data found for %s", underlying)
            return None

        # Быстрое преобразование результата
        closes = [(row[0], float(row[1])) for row in result.result_rows]

        logger.debug("Retrieved %d close prices for %s", len(closes), underlying)
        return closes if closes else None
        
    except Exception as e:
        logger.error("Error getting close prices for %s: %s", underlying, e)
        return None


async def getdb_candles(underlying: str, limit: int = 3000) -> Optional[List[Dict[str, Any]]]:
    """
    Оптимизированная версия получения свечей.
    
    Args:
        underlying: Базовый актив
        limit: Ограничение количества свечей (по умолчанию 3000)
    
    Returns:
        Список словарей с данными свечей, отсортированный по убыванию времени
    """
    try:
        tbl = await _ensure_table(underlying)

        # Оптимизированный запрос без использования argMax для каждой строки
        result = await _execute_query(f"""
            SELECT
                timestamp,
                open,
                high,
                low,
                close,
                volume
            FROM {tbl}
            ORDER BY timestamp DESC
            LIMIT {limit}
            SETTINGS 
                max_threads = 2,
                max_block_size = 10000,
                optimize_read_in_order = 1
        """)

        if not result or result.row_count == 0:
            return []

        # Эффективное преобразование результатов
        candles = []
        for row in result.result_rows:
            candles.append({
                "timestamp": row[0],
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "volume": int(row[5])
            })

        logger.debug("Retrieved %d candles for %s in optimized query", len(candles), underlying)
        return candles
        
    except Exception as e:
        logger.error("Error getting candles for %s: %s", underlying, e)
        return []


async def getdb_candles_fast(underlying: str, limit: int = 3000) -> Optional[List[Dict[str, Any]]]:
    """
    Сверхбыстрая версия через материализованное представление.
    
    Args:
        underlying: Базовый актив
        limit: Ограничение количества свечей
    
    Returns:
        Список словарей с данными свечей
    """
    try:
        tbl = _table_name(underlying)
        mv_name = _mv_name(underlying)

        # Проверяем существование MV
        async with get_db_connection() as client:
            check = await client.query(f"""
                SELECT name FROM system.tables 
                WHERE database = currentDatabase() AND name = '{mv_name}'
            """)
            
            if check.row_count == 0:
                # Если MV нет, создаем его и используем обычную таблицу
                await create_candles_materialized_view(underlying)
                table_to_query = tbl
            else:
                table_to_query = mv_name

        # Запрос к материализованному представлению
        result = await _execute_query(f"""
            SELECT
                timestamp,
                open,
                high,
                low,
                close,
                volume
            FROM {table_to_query}
            ORDER BY timestamp DESC
            LIMIT {limit}
            SETTINGS 
                max_threads = 1,
                max_block_size = 5000,
                optimize_read_in_order = 1,
                use_uncompressed_cache = 1
        """)

        if not result or result.row_count == 0:
            return []

        # Быстрое преобразование без лишних проверок
        candles = []
        for i in range(result.row_count):
            row = result.result_rows[i]
            candles.append({
                "timestamp": row[0],
                "open": row[1],
                "high": row[2],
                "low": row[3],
                "close": row[4],
                "volume": row[5]
            })

        logger.debug("Retrieved %d candles for %s via MV", len(candles), underlying)
        return candles
        
    except Exception as e:
        logger.error("Error in fast candles query for %s: %s", underlying, e)
        # Fallback к обычному методу
        return await getdb_candles(underlying, limit)

# --- Статистика и обслуживание ---

async def optimize_table(underlying: str) -> bool:
    """Принудительная оптимизация таблицы"""
    try:
        tbl = await _ensure_table(underlying)
        # Оптимизируем без FINAL для скорости, FINAL можно запускать ночью
        await _execute_command(f"OPTIMIZE TABLE {tbl}")
        logger.info("Table %s optimization scheduled", tbl)
        return True
    except Exception as e:
        logger.error("Error optimizing table %s: %s", tbl, e)
        return False
