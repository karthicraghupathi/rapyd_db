build:
	python setup.py sdist

upload:
	python setup.py sdist upload

testall:
	python -m unittest discover -v

testmysql:
	python -m unittest -v rapyd_db.tests.test_mysql

testmongo:
	python -m unittest -v rapyd_db.tests.test_mongo

testmssql:
	python -m unittest -v rapyd_db.tests.test_mssql

.PHONY: build register upload testall testmysql testmongo testmssql
