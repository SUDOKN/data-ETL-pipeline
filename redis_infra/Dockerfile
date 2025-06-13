# Use the official Redis image as a base
FROM redis:8.0-alpine

# Create the data and log directory
RUN mkdir -p /var/lib/redis && \
  mkdir -p /var/log/redis && \
  chown -R redis:redis /var/lib/redis /var/log/redis

# Copy custom configuration file
COPY redis.conf /usr/local/etc/redis/redis.conf

# Run Redis with the custom configuration
CMD ["redis-server", "/usr/local/etc/redis/redis.conf"]
