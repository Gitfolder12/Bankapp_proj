from contextlib import asynccontextmanager
from fastapi import FastAPI 
from connection.index import db
from middlewares.index import auth_middleware, log_requests
from model.index import create_tables
from routes.index import register_routers

                  
# Defining the lifespan context manager
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Perform startup tasks
    print("Application is starting up.")
    
    if db.is_closed():
        db.connect()
        create_tables(db)
               
    register_routers(app)
    

    # Yield control back to FastAPI
    yield

    # Perform shutdown tasks
    print("Application is shutting down.")
    if not db.is_closed():
        db.close()

# Initializing the
app = FastAPI(lifespan=lifespan)

app.middleware('http')(log_requests)
app.middleware('http')(auth_middleware)