from peewee import PostgresqlDatabase
# import redis

# Initialize the PostgreSQL database connection
db = PostgresqlDatabase(
    "mydatabase",
    user="sunny",
    password="sunny",
    host="localhost",
    port="5432"
)


# Connect to Redis for OTP storage
# redis_client = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
