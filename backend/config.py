from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict
from datetime import datetime, time

class Settings(BaseSettings):
    # --- Keys / Secrets ---
    KEY: str = ""
    SQL_DATABASE_URL: str = ""
    
    # --- Paths ---
    DATA_FOLDER: Path = Path(__file__).parent / "data"

    # --- Network ---
    TIMEOUT: int = 10

    # --- Market conventions ---
    TRADING_DAYS_PER_YEAR: int = 252
    MINUTES_PER_DAY: int = 865
    HIST_WINDOW_MINUTES: int = TRADING_DAYS_PER_YEAR * MINUTES_PER_DAY // 12

    # --- Implied Volatility Solver ---
    IV_RATE: float = 0.19
    IV_TOL: float = 1e-6
    IV_MAXITER: int = 100
    IV_VOL_LOWER: float = 1e-6
    IV_VOL_UPPER: float = 5.0

    # --- Futures codes ---
    MONTH_CODES: dict = {3: 'H', 6: 'M', 9: 'U', 12: 'Z'}
    ALL_CODES: dict = {
        1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K',
        6: 'M', 7: 'N', 8: 'Q', 9: 'U',
        10: 'V', 11: 'X', 12: 'Z'
    }
    COMMODITIES: list = ['BR', 'NG', 'SU', 'W4']

    # --- Logging ---
    LOG_FORMAT: str = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
    LOG_LEVEL: str = "INFO"
    
    # торговые сессии
    
    TRADING_START: time = time(9, 0)
    TRADING_END: time = time(23, 50)

    # клиринги
    
    CLEARING_PERIODS: list = [
        (time(14, 0), time(14, 5)),
        (time(18, 50), time(19, 5)),
    ]

    # конец торгов в день экспирации
    EXPIRY_END: time = time(18, 50)
    
        # клиринги
    
    HOLIDAYS: list = [
        (datetime(2025, 11, 3),
         datetime(2025, 11, 4),
         datetime(2025, 12, 31),
        )
    ]


    model_config = SettingsConfigDict(env_file='.env')

settings = Settings()