from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict
from datetime import datetime, time

class Settings(BaseSettings):
    # --- Keys / Secrets ---
    MOEX_API_KEY: str = ""
    DBUSER: str = ""
    DBPASS: str = ""
    DBNAME: str = ""
    
    # --- Database ---
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
    
    HOLIDAYS: list[datetime.date] = [
        datetime(2025, 11, 3).date(),
        datetime(2025, 11, 4).date(),
        datetime(2025, 12, 31).date(),
    ]

    @staticmethod
    def _read_secret(secret_name: str) -> str:
        path = Path(f"/run/secrets/{secret_name}")
        if path.exists():
            return path.read_text().strip()
        return ""
    
    # model_config = SettingsConfigDict(env_file='.env')

settings = Settings()

if not settings.MOEX_API_KEY:
    settings.MOEX_API_KEY = settings._read_secret("moex_api_key")
if not settings.DBUSER:
    settings.DBUSER = settings._read_secret("db_user")
if not settings.DBPASS:
    settings.DBPASS = settings._read_secret("db_pass")
if not settings.DBNAME:
    settings.DBNAME = settings._read_secret("db_name")

# Формируем строку подключения, если её нет
if not settings.SQL_DATABASE_URL and all([settings.DBUSER, settings.DBPASS, settings.DBNAME]):
    settings.SQL_DATABASE_URL = f"postgresql+asyncpg://{settings.DBUSER}:{settings.DBPASS}@db:5432/{settings.DBNAME}"