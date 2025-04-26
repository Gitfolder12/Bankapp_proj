<<<<<<< Updated upstream
from fastapi import Request, Response
import time, base64
=======
from functools import wraps
from fastapi import HTTPException, Request, Response
import time, logging
>>>>>>> Stashed changes
from starlette.status import HTTP_401_UNAUTHORIZED
from utils.jwt import verify_token

# Define valid credentials (Replace these with a more secure approach)
VALID_USERNAME = "admin"
VALID_PASSWORD = "password"

private_routes = ["/api/users/", "/transaction"]


async def log_requests(request: Request, call_next):
    print(f"imcoming log request:")
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    print(f"Request: {request.method} {request.url} - {process_time:.2f}s")
    return response


<<<<<<< Updated upstream
async def auth_middleware(request: Request, call_next):
    # Get Authorization header
    auth_header = request.headers.get("Authorization")
    
    print(request.url.path, "dsdjsjdj")
    path = request.url.path
    
    privateRouteChecker = False
    
    if path in private_routes:
        privateRouteChecker = True
    
    if privateRouteChecker: 
        if not auth_header:
            return Response(content="Invalid Authorization...", status_code=HTTP_401_UNAUTHORIZED)
        else:
            headers = auth_header.split(" ")
            print(f"header: {headers}")
            bearer = headers[0]
            print(f"bearer: {bearer}")
            if bearer != "Bearer":
=======

def role_middleware(required_role: str = None):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs,):
            # Try to get the request object
            request: Request = kwargs.get('request')
            if not request:
                for arg in args:
                    if isinstance(arg, Request):
                        request = arg
                        break
            if not request:
                raise HTTPException(status_code=500, detail="Request object not found")

            # Auth header check
            auth_header = request.headers.get("Authorization")
            if not auth_header:
                return Response(content="Invalid Authorization...", status_code=HTTP_401_UNAUTHORIZED)

            parts = auth_header.split(" ")
            if len(parts) != 2 or parts[0] != "Bearer":
>>>>>>> Stashed changes
                return Response(content="Authorization method must be Bearer", status_code=HTTP_401_UNAUTHORIZED)
            else:
                token = headers[1]
                verified = verify_token(token)
                print(f"verified: {verified}")
            
            if not verified:
                return Response(content="Not verified token.", status_code=HTTP_401_UNAUTHORIZED)
            else:
                    userid = verified["id"]
                    # Store userid in request.state
                    request.state.userid = userid  # Use request.state to store the user ID
                    print(f"Request ID: {request.state.userid}")  #

<<<<<<< Updated upstream
    # Proceed to the next middleware/route if authentication succeeds
    return await call_next(request)

=======
            # Extract user data
            print(verified, "verified")
            userid = verified["id"]
            role = verified.get("role")

            request.state.userid = userid
            request.state.role = role

            print(f"User ID: {userid}, Role: {role}")
            # Check for role match
            if required_role and role != required_role:
                raise HTTPException(status_code=HTTP_401_UNAUTHORIZED, detail="Insufficient role")

            return await func(*args, **kwargs)
        return wrapper
    return decorator
>>>>>>> Stashed changes

