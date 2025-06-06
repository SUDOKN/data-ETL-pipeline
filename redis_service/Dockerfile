# Use the official Redis image as a base
FROM redis:7.2-alpine

# Copy custom configuration file
COPY redis.conf /usr/local/etc/redis/redis.conf

# Run Redis with the custom configuration
CMD ["redis-server", "/usr/local/etc/redis/redis.conf"]
