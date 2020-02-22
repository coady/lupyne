all: check html

html:
	make -C docs $@ SPHINXOPTS=-W

pages: html
	ghp-import -n docs/_build/$?

check:
	python3 setup.py $@ -ms
	black --check -q .
	flake8
	mypy -p lupyne.engine
	pytest --cov=lupyne tests/test_engine.py tests/test_rest.py
	pytest --cov=lupyne --cov-append tests/test_graphql.py --cov-fail-under=100
	pytest tests/test_server.py
	pytest -vk example
