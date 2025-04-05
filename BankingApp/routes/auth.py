from fastapi import APIRouter
from dto.auth import UserLoginRequest
from model.auth import authenticate_user
from utils.jwt import create_token

router = APIRouter()

# Route to get all users
@router.post("/auth/", tags=["auth"])
async def auth_user(user_login_request: UserLoginRequest):
          loggedUser = authenticate_user(user_login_request)  # Call the findAll method from User model
          # jwt goes here          
          token = create_token(loggedUser)
          return token
      
      
   