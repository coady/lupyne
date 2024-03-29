check:
	python -m pytest -s --cov=lupyne.engine tests/test_engine.py
	python -m pytest -s --cov-append --cov=lupyne.services tests/test_rest.py tests/test_graphql.py

lint:
	ruff check .
	ruff format --check .
	mypy -p lupyne.engine

html:
	PYTHONPATH=$(PWD) python -m mkdocs build
