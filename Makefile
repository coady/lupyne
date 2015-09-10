all: check html

clean:
	make -C docs $@
	hg st -in | xargs rm
	rm -rf dist lupyne.egg-info

html:
	make -C docs $@ SPHINXOPTS=-W
	rst2$@.py README.rst docs/_build/README.$@

dist: html
	python setup.py sdist
	cd docs/_build/html && zip -r ../../../$@/docs.zip .

check:
	python setup.py $@ -mrs
	pep8
	py.test --cov --cov-fail-under=100
	py.test -vk example
