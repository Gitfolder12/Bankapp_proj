from fastapi import HTTPException
from peewee import DoesNotExist
from model.user import User
from dto.auth import UserLoginRequest
from utils.auth import verify_password
from utils.index import raise_format_error


def authenticate_user(user_login_request: UserLoginRequest):
    
    try: 
        
        user_record = User.get(User.email == user_login_request.email)
        
        if verify_password(user_login_request.password, user_record.password):
            data = {
                "id": user_record.id,
                "email": user_record.email
            }
            return data
        else:
            raise HTTPException(status_code=401, detail="Invalid Email or Password.")
        
    except Exception as e:
        if isinstance(e, DoesNotExist):
           raise HTTPException(status_code=400, detail=f"User does not exist..")
        raise raise_format_error(e, "An error occurred during authentication.")
