.PHONY: install up down produce bronze silver gold pipeline lint format test clean

install:
	pip install -r requirements-dev.txt && pip install -e .

up:
	docker compose up -d

down:
	docker compose down -v

produce:
	python producer/produce_events.py --customers 50 --orders 500

bronze:
	python -m medallion.cli bronze

silver:
	python -m medallion.cli silver

gold:
	python -m medallion.cli gold

pipeline:
	python -m medallion.cli all

lint:
	ruff check . && ruff format --check .

format:
	ruff format . && ruff check --fix .

test:
	pytest -q

clean:
	rm -rf lakehouse spark-warehouse metastore_db derby.log .pytest_cache
