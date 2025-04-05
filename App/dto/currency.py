# dtos/currency.py

# Pydantic model for user input validation
from pydantic import BaseModel

class CurrencyConvert(BaseModel):
    from_currency: str
    to_currency: str
    amount: int