release-image: build-image copy-image
release-infra: tf-init tf-apply
setup: install-dev install-precommit

test-unit:
	pytest tests/unit -s

test-integration:
	pytest tests/integration -s

lint:
	flake8 duckingit tests

install-dev:
	pip install pip -U && pip install -r requirements/dev.txt

install-precommit:
	pre-commit install

build-image:
	cd image/ && docker build . -t duckdb

copy-image:
	cd image/ && \
	mkdir -p release/ && \
	docker create --name duck duckdb && \
	docker cp duck:/tmp/release/duckdb-layer.zip release/duckdb-layer.zip && \
	docker rm duck

enter-image:
	docker run \
		-e AWS_ACCESS_KEY_ID='${AWS_ACCESS_KEY_ID}' \
		-e AWS_SECRET_ACCESS_KEY='${AWS_SECRET_ACCESS_KEY}' \
		--entrypoint /bin/bash \
		-it \
		--rm \
		duckdb

tf-init:
	cd infrastructure/aws && terraform init

tf-plan:
	cd infrastructure/aws && terraform plan

tf-apply:
	cd infrastructure/aws && terraform apply -auto-approve

tf-destroy:
	cd infrastructure/aws && terraform apply -auto-approve

build-package:
	python3 -m build

upload-package:
	python3 -m twine upload dist/*

clean:
	rm -rf dist/
	rm -rf *.egg-info
