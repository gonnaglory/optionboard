import logging
from datetime import datetime

from sqlalchemy import (
    MetaData, Table, Column,
    DateTime, Float, Integer, select, inspect
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

from backend.config import settings

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=settings.LOG_LEVEL, format=settings.LOG_FORMAT)
 
# Асинхронный движок и сессии
engine = create_async_engine(
    settings.SQL_DATABASE_URL,
    echo=False,
    future=True,
    pool_size=50,
    max_overflow=30,
    pool_pre_ping=True,
    pool_recycle=1800,
    pool_timeout=60,
)
AsyncSessionLocal = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
metadata = MetaData()

async def _get_candle_table(underlying: str) -> Table:
    table_name = f"{underlying}_candles"

    async with engine.begin() as conn:
        def sync_op(sync_conn):
            inspector = inspect(sync_conn)
            if table_name in inspector.get_table_names():
                return Table(table_name, metadata, autoload_with=sync_conn)
            else:
                table = Table(
                    table_name,
                    metadata,
                    Column("timestamp", DateTime, primary_key=True),
                    Column("open", Float, nullable=False),
                    Column("high", Float, nullable=False),
                    Column("low", Float, nullable=False),
                    Column("close", Float, nullable=False),
                    Column("volume", Integer, nullable=False),
                )
                metadata.create_all(sync_conn, tables=[table])

                # sync_conn.execute(
                #     f"""
                #     SELECT create_hypertable('{table_name}', 'timestamp', 
                #         if_not_exists => TRUE, migrate_data => TRUE);
                #     """
                # )
                
                return table

        table = await conn.run_sync(sync_op)

    return table

async def save_candles(underlying: str, candles: list[dict]) -> None:
    if not candles:
        return
    table = await _get_candle_table(underlying)
    insert_stmt = pg_insert(table).values(candles)
    update_stmt = {
        c: insert_stmt.excluded[c]
        for c in ["open", "high", "low", "close", "volume"]
    }
    stmt = insert_stmt.on_conflict_do_update(
        index_elements=["timestamp"],
        set_=update_stmt
    )
    async with engine.begin() as conn:
        await conn.execute(stmt)

# async def save_column(underlying: str, column: str, data: list[tuple[datetime, float]]) -> None:
#     """
#     Обновляет одну колонку (например hist_vol или log_return) в {underlying}_candles.
#     Новые строки не вставляет, только обновляет существующие timestamps.
#     Использует executemany для ускорения. Добавлено логирование времени выполнения.
#     """
#     if not data:
#         return

#     table = await _get_candle_table(underlying)

#     stmt = table.update().where(table.c.timestamp == bindparam("ts")).values({column: bindparam("val")})
#     params = [{"ts": ts, "val": val} for ts, val in data]

#     start_ts = time.perf_counter()
#     async with engine.begin() as conn:
#         await conn.execute(stmt, params)
#     elapsed = time.perf_counter() - start_ts

#     logger.debug(
#         "save_column updated %d rows for %s.%s in %.3f sec (Timescale hypertable=%s)",
#         len(data), underlying, column, elapsed, True
#     )

async def get_last_candle_date(underlying: str) -> datetime | None:
    table = await _get_candle_table(underlying)
    stmt = select(table.c.timestamp).order_by(table.c.timestamp.desc()).limit(1)
    async with engine.connect() as conn:
        result = await conn.execute(stmt)
        return result.scalar_one_or_none()

async def get_candles_from_db(underlying: str) -> list[tuple[datetime, float]] | None:
    table = await _get_candle_table(underlying)
    stmt = (
        select(table.c.timestamp, table.c.close)
        .order_by(table.c.timestamp.desc()).limit(settings.HIST_WINDOW_MINUTES + 1)
    )
    async with engine.connect() as conn:
        result = await conn.execute(stmt)
        rows = result.fetchall()
        
    return None if not rows else rows