all: check html

clean:
	make -C docs $@
	hg st -in | xargs rm
	rm -rf dist lupyne.egg-info

html:
	make -C docs $@ SPHINXOPTS=-W SPHINXBUILD=sphinx-build
	rst2$@.py README.rst docs/_build/README.$@
	python -m examples.spatial > docs/_build/spatial.kml

dist: html
	python setup.py sdist
	cd docs/_build/html && zip -r ../../../$@/docs.zip .

check:
	python setup.py $@ -mrs
	flake8
	python -m examples
	pytest-2.7 tests/test_engine.py --cov=lupyne.engine  --cov-fail-under=100
	pytest --cov --cov-fail-under=100
	pytest -vk example
