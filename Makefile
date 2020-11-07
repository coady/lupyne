check:
	pytest --cov=lupyne.engine tests/test_engine.py
	pytest --cov=lupyne.services tests/test_rest.py tests/test_graphql.py

lint:
	python3 setup.py check -ms
	black --check .
	flake8
	mypy -p lupyne.engine

html:
	PYTHONPATH=$(PWD) mkdocs build

legacy:
	pytest --cov=lupyne.server.legacy tests/test_server.py
	pytest -vk example
