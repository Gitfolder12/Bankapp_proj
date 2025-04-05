from fastapi import Request, Response
import time, base64
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

    # Proceed to the next middleware/route if authentication succeeds
    return await call_next(request)


