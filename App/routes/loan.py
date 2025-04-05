from fastapi import APIRouter, HTTPException
from model.loan import Loan
from dto.loan import LoanCreate

router = APIRouter()

# Route to get all loan
@router.get("/loans/", tags=["loans"])
async def get_loans():
    loan = Loan.find_all()  
    return loan

# Route to create a new loan
@router.post("/loans/", tags=["loans"])
async def create_loan(loan: LoanCreate):
    try:
        # Attempt to grant a loan by calling the class method
        new_loan = Loan.grant_loan(loan)

        return new_loan  

    except HTTPException as http_exc:
        # Re-raise the HTTPException for errors related to the loan process
        raise http_exc
    except Exception as e:
        # Handle unexpected exceptions
        raise HTTPException(status_code=500, detail=f"Error creating loan: {str(e)}")
