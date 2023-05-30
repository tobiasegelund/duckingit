#!/bin/bash

if ! [ $(which docker) ]; then
    echo "You must install Docker before proceeding"
    exit 1
fi

if ! [ $(which terraform) ]; then
    echo "You must install Terraform before proceeding"
    exit 1
fi

mkdir -p release/

docker pull tobiasegelund/aws-lambda-duckdb-python:latest
docker create --name duck tobiasegelund/aws-lambda-duckdb-python:latest
docker cp duck:/tmp/release/duckdb-layer.zip release/duckdb-layer.zip
docker rm duck