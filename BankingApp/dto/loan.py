# dto/loan.py

# Pydantic model for loan input validation
from decimal import Decimal
from enum import Enum
from pydantic import BaseModel

# Enum for transaction types
class LoanStatusEnum(Enum):
    OWING = "owing"
    PAID  = "paid"
     
class LoanCreate(BaseModel):
    user: int
    amount: Decimal
    term_years: int