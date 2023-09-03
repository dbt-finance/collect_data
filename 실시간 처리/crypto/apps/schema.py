from pydantic import BaseModel, validator

class MarketData(BaseModel):
    opening_price: float
    high_price: float
    low_price: float
    last_price: float
    price_change: float
    price_change_percent: float
    volume: float

    @validator(
        "opening_price",
        "high_price",
        "low_price",
        "last_price",
        "price_change",
        "volume",
        pre=True,
        always=True,
    )
    def round_to_two_decimal_places(cls, v):
        return round(v, 2)
