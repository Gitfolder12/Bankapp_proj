from fastapi import HTTPException
from peewee import Model, IntegerField, DateField, AutoField, DecimalField, CharField, fn
from connection.index import db
from datetime import datetime
from dto.loan import LoanCreate, LoanStatusEnum
from model.transaction import Transaction
from utils.index import calculate_interest_rate


class Loan(Model):
    id = AutoField()
    user = IntegerField()
    amount = DecimalField(max_digits=10, decimal_places=2)
    interest_rate = DecimalField(max_digits=5, decimal_places=2)  
    term_years = IntegerField()  
    date_taken = DateField(default=datetime.now)
    balance = DecimalField(max_digits=10, decimal_places=2)
    status = CharField(choices=[(status.value, status.name) for status in LoanStatusEnum], default=LoanStatusEnum.OWING.value)


    class Meta:
        database = db
        
    def check_Isowing(user_id: int):
        # Check if user still owing 
        is_exist = (Loan
                        .select(Loan.user)
                        .where((Loan.status == 'owing') &  (Loan.user == user_id))
                        .exists())
        return is_exist
    
    def get_total_deposit(user_id: int):
        # Calculate the total deposits for the user
        total_deposits = (Transaction
                            .select(fn.SUM(Transaction.amount))
                            .where((Transaction.type == 'deposit') &  (Transaction.user_id == user_id))  
                            .scalar() or 0.0)
        return total_deposits
            
    

    @classmethod
    def find_all(cls) -> list:
        """Retrieve all loans."""
        return [loan.__data__ for loan in cls.select()]

    @classmethod
    def grant_loan(cls, loan_data: LoanCreate) -> "Loan": 
        """
        Attempts to create a loan for a user if their total deposits meet the loan amount requirement.
        """
        try:
             is_exist = cls.check_Isowing(loan_data.user)
             if is_exist:
                raise ValueError("Loan request denied for User: still owing") 
             
             # Check if deposits meet or exceed the loan amount
             total_deposits = cls.get_total_deposit(loan_data.user)
            
             if total_deposits >= loan_data.amount:
                
                interest_rate  = calculate_interest_rate(loan_data.amount)
                
                # Create the loan record
                loan_dict = loan_data.model_dump()
                loan_dict['balance'] = loan_data.amount
                loan_dict['interest_rate'] = interest_rate
                loan_instance = cls.create(**loan_dict)
                return loan_instance.__data__
             else:
                raise HTTPException(status_code=404, detail="Loan request denied for User: insufficient deposits") 
        except Exception as e:
            print(f"Error while granting loan: {e}")
            raise HTTPException(status_code=500, detail= str(e))
