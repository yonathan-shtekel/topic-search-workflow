

# Simple Makefile for docker-compose tasks

# Bring up containers in the background
up:
	docker-compose up -d

# Stop and remove containers, networks, images, and volumes
down:
	docker-compose down

# Build or rebuild services
build:
	docker-compose build

# Stop containers
stop:
	docker-compose stop

# Remove stopped containers
rm:
	docker-compose rm -f


run-worker:
	poetry run python apps.search_worker.run_worker.py