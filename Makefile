all: check html

clean:
	make -C docs $@
	hg st -in | xargs rm
	rm -rf dist lupyne.egg-info

html:
	make -C docs $@ SPHINXOPTS=-W
	rst2$@.py README.rst docs/_build/README.$@
	python -m examples.spatial > docs/_build/spatial.kml

dist: html
	python setup.py sdist
	cd docs/_build/html && zip -r ../../../$@/docs.zip .

check:
	python setup.py $@ -mrs
	flake8
	python -m examples
	py.test --cov --cov-fail-under=100
	py.test -vk example
