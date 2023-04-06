release: build copy

test-unit:
	pytest tests/unit -s

test-integration:
	pytest tests/integration -s

build:
	cd image/ && docker build . -t duckdb

copy:
	cd image/ && \
	mkdir -p release/ && \
	docker create --name duck duckdb && \
	docker cp duck:/tmp/release/duckdb-layer.zip release/duckdb-layer.zip && \
	docker rm duck

enter:
	docker run \
		-e AWS_ACCESS_KEY_ID='${AWS_ACCESS_KEY_ID}' \
		-e AWS_SECRET_ACCESS_KEY='${AWS_SECRET_ACCESS_KEY}' \
		--entrypoint /bin/bash \
		-it \
		--rm \
		duckdb