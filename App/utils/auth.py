import bcrypt 
import re

# Function to hash the password using bcrypt
def hash_password(plain_password: str) -> str:
    return bcrypt.hashpw(plain_password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

# Function to verify password during login
def verify_password(plain_password: str, hashed_password: str) -> bool:
    return bcrypt.checkpw(plain_password.encode('utf-8'), hashed_password.encode('utf-8'))



def validate_password(password: str) -> bool:
    """Validate the password to contain at least one lowercase letter, 
    one uppercase letter, one digit, and one special character."""
    
    # Define the regex pattern
    pattern = re.compile(
        r'^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[@$!%*?&])[A-Za-z\d@$!%*?&]{8,}$'
    )
    
    if pattern.match(password):
        return True
    else:
        return False
