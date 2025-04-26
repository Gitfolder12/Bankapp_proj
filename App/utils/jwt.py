from datetime import datetime, timedelta
from typing import Optional

from fastapi import Depends, HTTPException, Security
from fastapi.security import HTTPBearer
from jose import jwt

security = HTTPBearer()  # Extracts "Authorization: Bearer <token>" header

# Secret key and algorithm for encoding/decoding JWT
SECRET_KEY = "kings"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 22000  # 22000 minutes = 15 days

def verify_token(token: str = Security(security)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload  # Returns decoded data if valid
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")


def create_token(data: dict, expires_in: int = ACCESS_TOKEN_EXPIRE_MINUTES):
    payload = data.copy()
    
    # Set expiration time
    expire_time = datetime.utcnow() + timedelta(minutes=expires_in)
    payload["exp"] = expire_time
    payload["id"] = data["id"]

    # Generate and return token
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)
