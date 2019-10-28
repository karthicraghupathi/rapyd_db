build:
	python setup.py sdist

upload:
	python setup.py sdist upload

.PHONY: build register upload
