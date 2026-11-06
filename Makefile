.PHONY: install test run

install:
	pip install -r requirements.txt

test:
	pytest -v

run:
	python -m src.runner
