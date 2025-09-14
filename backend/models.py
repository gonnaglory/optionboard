from pydantic import BaseModel, PositiveFloat
from typing import Literal, Any

class OptionData(BaseModel):
    SECID: str
    SHORTNAME: str
    PREVSETTLEPRICE: PositiveFloat
    DECIMALS: int
    MINSTEP: PositiveFloat
    LASTTRADEDATE: str
    PREVOPENPOSITION: Any
    PREVPRICE: Any
    OPTIONTYPE: Literal['C', 'P']
    STRIKE: PositiveFloat
    CENTRALSTRIKE: PositiveFloat
    UNDERLYINGASSET: str
    UNDERLYINGSETTLEPRICE: PositiveFloat
    HIST_VOL : float
    IMPLIED_VOL : float
    THEORETICAL_PRICE : float
    DELTA : float
    GAMMA : float
    VEGA : float
    THETA : float