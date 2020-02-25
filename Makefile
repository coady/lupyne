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
	pytest --cov=lupyne.engine tests/test_engine.py --cov-fail-under=100
	pytest --cov=lupyne.server tests/test_rest.py tests/test_graphql.py --cov-fail-under=100

legacy:
	pytest --cov=lupyne.server.legacy tests/test_server.py
	pytest -vk example
