from fastapi import APIRouter
from model.transaction import Transaction

router = APIRouter()

# Route to get all withdraw
@router.get("/withdraws/", tags=["withdraws"])
async def get_withdraws():
    withdraws = Transaction.findWithdraws()  
    return withdraws

# Route to get all withdraws for a user
@router.get("/withdraws/user/{user}", tags=["withdraws"])
async def get_withdrawsbyUser(user):
    id = int(user)
    withdraws = Transaction.findWithdrawsbyUser(id)  
    return withdraws