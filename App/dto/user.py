# dtos/users.py

# Pydantic model for user input validation
from pydantic import BaseModel,EmailStr

class UserCreate(BaseModel):
    firstname: str
    lastname: str
    phone: int
    email: EmailStr
    password: str
    age: int
      