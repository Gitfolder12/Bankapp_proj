import requests
from dto.currency import CurrencyConvert

API_URL = 'https://api.exchangerate-api.com/v4/latest/'

class ExchangeRateConverter:
    """A class to handle currency exchange rate fetching and calculation."""

    def __init__(self, currency: CurrencyConvert):
        self.currency = currency

    def get_exchange_rate(self):
        """Fetches the exchange rate from an external API with error handling."""
        try:
            # Make a request to the API
            response = requests.get(f"{API_URL}{self.currency.from_currency}")
            response.raise_for_status()  # Raise an error for bad HTTP status codes
            
            # Parse the response data
            data = response.json()
            
            # Check if the 'rates' field and requested currency exist
            if 'rates' in data and self.currency.to_currency in data['rates']:
                return data['rates'][self.currency.to_currency]
            else:
                raise ValueError(f"Exchange rate for {self.currency.to_currency} not found.")
        
        except requests.exceptions.RequestException as e:
            # Handle request errors (network issues, bad response, etc.)
            print(f"Error fetching exchange rate data: {e}")
            return None

    def calculate_exchange_rate(self):
        """Calculates the total converted amount based on the exchange rate."""
        rate = self.get_exchange_rate()
        
        if rate is not None:
            total = self.currency.amount * rate
            return f"The total converted amount is: {total} {self.currency.to_currency}"
        else:
            return "Failed to retrieve exchange rate."

