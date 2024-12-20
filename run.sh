#!/bin/sh

# Check and build the osseahorse image
if [ "$(docker images -q osseahorse:latest 2> /dev/null)" = "" ]; then
  echo "Building osseahorse Docker image..."
  (cd src/engine && docker build -t osseahorse .)
else
  echo "osseahorse Docker image already exists. Skipping build."
fi

# Check and build the oscoral image
if [ "$(docker images -q oscoral:latest 2> /dev/null)" = "" ]; then
  echo "Building oscoral Docker image..."
  (cd src/api && docker build -t oscoral .)
else
  echo "oscoral Docker image already exists. Skipping build."
fi

# Start the containers using docker-compose
echo "Starting Docker Compose..."
docker compose up
