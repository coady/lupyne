check:
	pytest --cov=lupyne.engine tests/test_engine.py
	python3 -m pytest --cov=lupyne.services tests/test_rest.py tests/test_graphql.py

lint:
	python3 setup.py check -ms
	black --check .
	flake8
	mypy -p lupyne.engine

html:
	PYTHONPATH=$(PWD) mkdocs build
