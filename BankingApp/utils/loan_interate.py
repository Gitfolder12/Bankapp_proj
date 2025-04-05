from fastapi import HTTPException

def calculate_interest_rate(loan_amount):

    if loan_amount < 1000:
       raise HTTPException(status_code=404, detail="minimum loan amount can't be less than : 1000")
    elif loan_amount > 100000:
         raise HTTPException(status_code=404, detail="maximum loan amount can't be above: 100000")
   
    if loan_amount < 5000:
        return 0.1  
    elif 5000 <= loan_amount < 10000:
        return 0.2  
    elif 20000 <= loan_amount < 50000:
        return 0.3  
    elif 50000 <= loan_amount <= 100000:
        return 0.4  
   