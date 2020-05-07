build:
	python setup.py sdist

upload:
	python setup.py sdist upload

testall:
	python -m unittest discover -v

testmysql:
	python -m unittest -v rapyd_db.tests.test_mysql

.PHONY: build register upload testall testmysql
