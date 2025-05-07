import redis
try:
    r = redis.Redis(host='localhost', port=6379, db=0, socket_connect_timeout=5)
    r.ping()
    print("SUCCESS: Connected to Redis and ping OK!")
except redis.exceptions.ConnectionError as e:
    print(f"FAILED: Could not connect to Redis: {e}")
except Exception as e:
    print(f"FAILED: An unexpected error occurred: {e}")