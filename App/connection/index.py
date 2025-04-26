
import os
from peewee import PostgresqlDatabase
from dotenv import load_dotenv

# Load env file
load_dotenv()

# Get environment variables (no defaults)
DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
DB_HOST = os.environ["DB_HOST"]
DB_PORT = os.environ["DB_PORT"]

# Initialize the PostgreSQL database connection using env vars
db = PostgresqlDatabase(
    DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)
