docker compose -f postgres_infra/docker-compose.yml up -d
docker ps
docker logs sudokn_postgres
docker exec -it sudokn_postgres psql -U litellm -d litellm -c "\l"