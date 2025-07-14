import os
from redis import Redis  # this import lets pylance know redis is typed

HOST = os.getenv("REDIS_HOST")
PORT = os.getenv("REDIS_PORT")

if not HOST:
    raise ValueError(
        "REDIS_HOST environment variable is not set. Please set it in your .env file."
    )
if not PORT:
    raise ValueError(
        "REDIS_PORT environment variable is not set. Please set it in your .env file."
    )

PORT = int(PORT)  # Ensure PORT is an integer

redis: Redis = Redis(host=HOST, port=PORT, db=0, decode_responses=True)
# Redis[str] is a type hint indicating that the Redis client will be used with string keys and values.
# The decode_responses=True argument ensures that the Redis client decodes responses to strings.
