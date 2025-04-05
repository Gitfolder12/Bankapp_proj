from sqlite3 import IntegrityError
from fastapi import HTTPException
from peewee import Check
from peewee import (
    Model, CharField, DecimalField, ForeignKeyField, 
    TextField, DateTimeField, AutoField
)
from datetime import datetime
from dto.transaction import TransactionCreate, TransactionTypeEnum
from model.user import User 
from model.account import Account
from connection.index import db



# Transaction model
class Transaction(Model):
    id = AutoField()
    user = ForeignKeyField(User, on_delete='CASCADE')
    amount = DecimalField(max_digits=10, decimal_places=2)
    type = CharField(max_length=50)
    created_at = DateTimeField(default=datetime.now)
    updated_at = DateTimeField(default=datetime.now)
    description = TextField(null=True)
    
    class Meta:
        database = db  # This sets the database for the model
        
    
    @classmethod
    def findAll(cls) -> list["Transaction"]:
        return [transaction.__data__ for transaction in cls.select()]
    
    
    @classmethod
    def create_transaction(cls, transaction: TransactionCreate) -> "Transaction":
        
           
            if transaction.type.value in (TransactionTypeEnum.DEPOSIT.value,TransactionTypeEnum.WITHDRAWAL.value):
                id = transaction.user
                user = User.findOne(id)
                if user is None:
                   raise HTTPException(status_code=404, details="user id not valid.")  
               
                # find user account by user_id
                #else:
                account = Account.get_or_none(Account.user == id)
                if account is None:
                    raise HTTPException(status_code=404, detail="No account found for this user.")

                # update user account balance
                if transaction.type.value == TransactionTypeEnum.DEPOSIT.value:
                    account.balance += transaction.amount
                elif transaction.type.value == TransactionTypeEnum.WITHDRAWAL.value:
                   if account.balance >= transaction.amount:
                      account.balance -= transaction.amount
                   else:
                        raise HTTPException(status_code=400, detail="Insufficient funds for withdrawal.")
                
                Account.update(balance=account.balance).where(Account.user == id).execute()

                transaction_dict = transaction.model_dump()
                transaction_dict['type'] = transaction.type.value
                transaction_instance = cls.create(**transaction_dict) 
                return transaction_instance.__data__
            else: 
                 raise HTTPException(status_code=404, details="Transaction type invalid") 
    
    @classmethod
    def findOne(cls, id: int) -> "Transaction":
        transaction = cls.get_or_none(cls.id == id)  # Get Transaction by ID
        if transaction:
            return transaction.__data__ 
        
        return None
             
    @classmethod
    def findDeposits(cls) -> list["Transaction"]:
        deposits = Transaction.select().where(Transaction.type == 'deposit')
        result = []  
        for deposit in deposits:
            result.append(deposit.__data__)
        
        return result
    
    @classmethod
    def findDepositsbyUser(cls, user: int) -> list["Transaction"]:
        deposits = Transaction.select().where((Transaction.type == 'deposit') & (Transaction.user == user))
        result = []  
        for deposit in deposits:
            result.append(deposit.__data__)
        
        return result
    
    
    @classmethod
    def findWithdraws(cls) -> list["Transaction"]:
        withdraws = Transaction.select().where(Transaction.type == 'withdrawal')
        result = []  
        for withdraw in withdraws:
            result.append(withdraw.__data__)
        
        return result
    
    @classmethod
    def findWithdrawsbyUser(cls, user: int) -> list["Transaction"]:
        withdraws = Transaction.select().where((Transaction.type == 'withdrawal') & (Transaction.user == user))
        result = []  
        for withdraw in withdraws:
            result.append(withdraw.__data__)
        
        return result
    
    
    