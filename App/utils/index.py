import random
from peewee import DoesNotExist
from fastapi import HTTPException
from pydantic import ValidationError

def generate_account_number():
    return random.randint(10000000, 99999999)



def raise_format_error(e: ValidationError, title: str = '') -> str:
    # Join all error messages into a single string
    if isinstance(e, ValidationError):
        errors = e.errors()  # Get all validation errors
        messages = [f"{error['loc'][0]}: {error['msg']}" for error in errors]
        # Handle validation errors
        formatted_message = ", ".join(messages)
        raise HTTPException(
            status_code=400, detail=f"Validation errors: {formatted_message}"
        )
    elif isinstance(e, DoesNotExist):
        raise HTTPException(status_code=400, detail=f"Does not exist..")
    else:
        # Handle generic exceptions
        raise HTTPException(status_code=400, detail=f"{title} Error: {str(e)}")

def calculate_interest_rate(amount):

    if amount < 1000:
       raise ValueError("minimum loan amount can't be less than : 1000")
    elif amount > 100000:
       raise ValueError("maximum loan amount can't be above: 100000")
    
    if amount < 5000:
        return 0.1  
    elif 5000 <= amount < 10000:
        return 0.2  
    elif 10000 <= amount < 50000:
        return 0.3  
    elif 50000 <= amount <= 100000:
        return 0.4     

def calculate_monthly_payment(amount,loan_term_year):
    
    principal = amount
    per_year = 12 
    monthly_rate = calculate_interest_rate(principal) / per_year
    loan_term_year = loan_term_year * per_year
    monthly_rate_to_power = (1 + monthly_rate) ** loan_term_year
    monthly_payment = round(principal * monthly_rate * monthly_rate_to_power / monthly_rate_to_power - 1, 2)
    return monthly_payment

