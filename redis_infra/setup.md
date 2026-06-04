docker build -t redis-app .
docker run -d \
  --name redis \
  --restart unless-stopped \
  --log-driver=journald \
  -p 6379:6379 \
  redis-app