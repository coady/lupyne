all: check
	make -C docs html SPHINXOPTS=-W

check:
	python3 setup.py $@ -ms
	black --check -q .
	flake8
	mypy -p lupyne.engine
	make engine server
	pytest -vk example

engine server:
	pytest tests/test_$@.py --cov=lupyne.$@ --cov-fail-under=100
