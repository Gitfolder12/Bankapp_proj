# dto/transaction.py

# Pydantic model for transaction input validation
from enum import Enum
from pydantic import BaseModel

# Enum for transaction types
class TransactionTypeEnum(Enum):
    WITHDRAWAL = 'withdrawal'
    DEPOSIT = 'deposit'
    
class TransactionCreate(BaseModel):
    user: int
    amount: int
    type: TransactionTypeEnum
    description: str
    
   