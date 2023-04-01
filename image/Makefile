ID := XXXX
IMAGE_URL := "${ID}.dkr.ecr.eu-central-1.amazonaws.com/aws-lambda-duckdb-python"

all: build copy

run:
	docker run \
		-e AWS_ACCESS_KEY_ID='${AWS_ACCESS_KEY_ID}' \
		-e AWS_SECRET_ACCESS_KEY='${AWS_SECRET_ACCESS_KEY}' \
		-p 9000:8080 \
		--rm \
		duckdb

build:
	docker build . -t duckdb

curl:
	curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{}'

enter:
	docker run \
		-e AWS_ACCESS_KEY_ID='${AWS_ACCESS_KEY_ID}' \
		-e AWS_SECRET_ACCESS_KEY='${AWS_SECRET_ACCESS_KEY}' \
		--entrypoint /bin/bash \
		-it \
		--rm \
		duckdb

tag:
	docker tag duckdb:latest ${IMAGE_URL}:latest

push:
	docker push -a ${IMAGE_URL}

copy:
	mkdir -p release/
	docker create --name duck duckdb
	docker cp duck:/tmp/release/duckdb-layer.zip release/duckdb-layer.zip
	docker rm duck
