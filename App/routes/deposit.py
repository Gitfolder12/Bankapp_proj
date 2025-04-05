from fastapi import APIRouter
from model.transaction import Transaction

router = APIRouter()

# Route to get all deposits
@router.get("/deposits/", tags=["deposits"])
async def get_deposits():
    deposits = Transaction.findDeposits()  
    return deposits

# Route to get all deposits
@router.get("/deposits/user/{user}", tags=["deposits"])
async def get_depositsbyUser(user):
    id = int(user)
    deposits = Transaction.findDepositsbyUser(id)  
    return deposits



