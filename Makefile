all: check
	make -C docs html SPHINXOPTS=-W

check:
	python3 setup.py $@ -ms
	black --check -q .
	flake8
	pytest-2.7 tests/test_engine.py --cov=lupyne.engine --cov-fail-under=100
	pytest --cov --cov-fail-under=100
	pytest -vk example
