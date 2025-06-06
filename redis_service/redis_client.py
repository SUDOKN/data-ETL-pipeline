from redis.asyncio.client import Redis # this import lets pylance know redis is typed

redis: Redis[str] = Redis(host="localhost", port=6379, db=0, decode_responses=True)
# Redis[str] is a type hint indicating that the Redis client will be used with string keys and values.
# The decode_responses=True argument ensures that the Redis client decodes responses to strings.