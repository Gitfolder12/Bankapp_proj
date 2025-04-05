from fastapi import FastAPI
from routes import user, deposit, transaction, withdraw,auth,loan,currency

def register_routers(app: FastAPI):
    app.include_router(auth.router, prefix="/api")
    app.include_router(user.router, prefix="/api")
    app.include_router(deposit.router, prefix="/api")
    app.include_router(withdraw.router, prefix="/api")
    app.include_router(transaction.router, prefix="/api")
    app.include_router(loan.router, prefix="/api")
    app.include_router(currency.router, prefix="/api")
   
   
   
   
   