#!/bin/bash

# Ensure script stops on first error
set -e

echo "Starting YouTube Analytics Pipeline..."

# check if docker is running
if ! docker info > /dev/null 2>&1; then
  echo "Error: Docker is not running. Please start Docker."
  exit 1
fi

# Build and start the container in detached mode
echo "Starting Docker services..."
docker-compose up -d --build

# Wait for container to be ready (simplistic approach)
echo "Waiting for services to stabilize..."
sleep 5

# Run the pipeline inside the container
echo "Running extraction pipeline..."
docker-compose exec -T pipeline uv run python src/main.py

echo "Pipeline execution finished."
