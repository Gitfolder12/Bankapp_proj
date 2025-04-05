# dtos/alert.py

# Pydantic model for user authenticate validation
from pydantic import BaseModel,EmailStr

# Request alert message
class alert(BaseModel):
    subject: str
    body: EmailStr
    to_email:  str