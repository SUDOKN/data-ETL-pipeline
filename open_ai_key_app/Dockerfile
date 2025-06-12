# Use the official Redis image as a base
FROM redis:8.0-alpine

# Copy custom configuration file
COPY redis.conf /usr/local/etc/redis/redis.conf

# Run Redis with the custom configuration
CMD ["redis-server", "/usr/local/etc/redis/redis.conf"]
