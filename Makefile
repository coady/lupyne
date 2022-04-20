check:
	pytest -s --cov=lupyne.engine tests/test_engine.py
	python3 -m pytest -s --cov-append --cov=lupyne.services tests/test_rest.py tests/test_graphql.py

lint:
	black --check .
	flake8
	mypy -p lupyne.engine

html:
	PYTHONPATH=$(PWD) python3 -m mkdocs build
