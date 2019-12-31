all: check html

html:
	make -C docs $@ SPHINXOPTS=-W

pages: html
	ghp-import -nm "GH pages autocommit." docs/_build/$?

check:
	python3 setup.py $@ -ms
	black --check -q .
	flake8
	mypy -p lupyne.engine
	make engine server
	pytest -vk example

engine server:
	pytest tests/test_$@.py --cov=lupyne.$@ --cov-fail-under=100
