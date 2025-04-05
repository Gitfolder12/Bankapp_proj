from fastapi import APIRouter, HTTPException
from utils.currency import ExchangeRateConverter
from dto.currency import CurrencyConvert

router = APIRouter()

# Route to get total amout currency exchange
@router.post("/currency/", tags=["currency"])
async def convert_currency(currency: CurrencyConvert):
    # Create an instance of ExchangeRateConverter
    converter = ExchangeRateConverter(currency)
    
    # Calculate the converted amount
    result = converter.calculate_exchange_rate()
    
    # Check if there was an error
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    
    return result

