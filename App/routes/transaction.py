from fastapi import APIRouter, Depends, HTTPException, Request
from middlewares.index import role_middleware
from model.transaction import Transaction
from dto.transaction import TransactionCreate
from model.user import User
from utils.email import send_email_alert, prepare_transaction_email

router = APIRouter()

# Route to get all Transaction
@router.get("/transactions/", tags=["transactions"])
@role_middleware()
async def get_transactions(request: Request):
    print("Inside route")
    print("Request state userid:", getattr(request.state, "userid", None))
    user_id = getattr(request.state, "userid", None)  # Use getattr to safely access state
    if not user_id:
        return {"error": "User ID not found in request state"}

    # Query all transactions for the given user
    transactions = Transaction.select().where(Transaction.user == user_id)

    # Convert the result into a list of dictionaries
    transactions_list = [transaction.__data__ for transaction in transactions]

    return transactions_list

# Route to get all Transaction
@router.get("/transactions/{id}", tags=["transactions"])
# @role_middleware
async def get_transaction(id: int):
          transaction = Transaction.findOne(id)  # Call the findAll method from User model
          return transaction

# Route to create a new transaction
@router.post("/transactions/", tags=["transactions"])
@role_middleware()
async def create_transaction(transaction: TransactionCreate):
    try:
        new_transaction = Transaction.create_transaction(transaction)
        
        # get userid transaction 
        user =  User.findOne(new_transaction["user"])
        # ✅ Prepare the transaction email details
        subject, body, = prepare_transaction_email(user, new_transaction)
        
        # ✅ send email details
        await send_email_alert(subject, body , user["email"])
        
        return new_transaction
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error creating transaction: {str(e)}")

