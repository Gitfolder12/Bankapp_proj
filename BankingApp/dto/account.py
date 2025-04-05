# dtos/account.py

# Pydantic model for user input validation
from pydantic import BaseModel

class AccountCreate(BaseModel):
    user: int
    number: int
   
   