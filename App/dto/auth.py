# dtos/auth.py

# Pydantic model for user authenticate validation
from pydantic import BaseModel,EmailStr

# Request body for user login
class UserLoginRequest(BaseModel):
    email: EmailStr
    password: str