#!/bin/bash

# Script to start PostgreSQL database for Torii indexer

echo "Starting PostgreSQL database for Torii..."

# Check if Docker is running (try both with and without sudo)
if docker info > /dev/null 2>&1; then
    DOCKER_CMD="docker"
    COMPOSE_CMD="docker compose"
elif sudo docker info > /dev/null 2>&1; then
    DOCKER_CMD="sudo docker"
    COMPOSE_CMD="sudo docker compose"
else
    echo "Error: Docker is not running or not accessible. Please:"
    echo "1. Start Docker: sudo systemctl start docker"
    echo "2. Add user to docker group: sudo usermod -aG docker \$USER"
    echo "3. Log out and back in, or run: newgrp docker"
    exit 1
fi

# Start PostgreSQL container
$COMPOSE_CMD -f docker-compose.postgres.yml up -d

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL to be ready..."
sleep 5

# Check if database is accessible
for i in {1..30}; do
    if $DOCKER_CMD exec torii-postgres pg_isready -U torii -d torii > /dev/null 2>&1; then
        echo "✅ PostgreSQL is ready!"
        break
    fi
    echo "Waiting for PostgreSQL... ($i/30)"
    sleep 2
done

# Display connection information
echo ""
echo "🐘 PostgreSQL Database Information:"
echo "Host: localhost"
echo "Port: 5432"
echo "Database: torii"
echo "Username: torii"
echo "Password: torii"
echo ""
echo "Connection URL: postgresql://torii:torii@localhost:5432/torii"
echo ""
echo "To connect manually:"
echo "  $DOCKER_CMD exec -it torii-postgres psql -U torii -d torii"
echo ""
echo "To stop the database:"
echo "  $COMPOSE_CMD -f docker-compose.postgres.yml down"
echo ""
echo "To stop and remove data:"
echo "  $COMPOSE_CMD -f docker-compose.postgres.yml down -v"