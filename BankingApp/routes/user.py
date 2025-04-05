from urllib import request
from fastapi import APIRouter, Request
from model.user import User
from dto.user import UserCreate

router = APIRouter()

# Route to get all users
@router.get("/users/", tags=["users"])
async def get_users():
    users = User.findAll()  # Call the findAll method from User model
    return users

# Route to get a user
@router.get("/users/{id}", tags=["users"])
async def get_user(id: str, request: Request):
    print(f"id: {id}")
    # Access the user ID stored in request.state
    user_id_from_request = getattr(request.state, "userid", None)  # Use getattr to safely access state
    

    print(f"Requested user ID: {id}")
    print(f"User ID from request state: {user_id_from_request}")
    user = User.findOne(id)  # Call the findAll method from User model
    return user


# Route to create a new user (POST)
@router.post("/users/", tags=["users"])
async def create_user(user: UserCreate):
    # Create a new user and save to the database
     new_user = User.create_user(user)
     return new_user