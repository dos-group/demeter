#!/bin/bash

SCRIPTPATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
APP_NAME=demeter_py

## Build the image
docker build -t $APP_NAME .

## Create folder, if not exists
mkdir -p app/artifacts
chmod -R 775 app/artifacts

## Stop and remove a running container
docker stop $APP_NAME
docker rm $APP_NAME

# run app
docker run -it --rm -p 5000:5000 -v ${SCRIPTPATH}/app/artifacts:/app/app/artifacts:z \
  --env WORKERS_PER_CORE=0.5 \
  --env MAX_WORKERS=2 \
  --env WEB_CONCURRENCY=2 \
  --env PORT=5000 \
  --env TIMEOUT=600 \
  --env GRACEFUL_TIMEOUT=0 \
  --env PRE_START_PATH=/app/app/prepare_model.sh \
  --name="$APP_NAME" $APP_NAME