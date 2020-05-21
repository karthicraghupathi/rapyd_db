Rapyd DB
=============

An opinionated lightweight wrapper around various SQL backend drivers.

Support right now is available for the following:

- MySQL / MariaDB via ``mysqlclient``
- Microsoft SQL Server via ``pymssql``
- mongoDB via ``pymongo``

Features
--------

- Opens and closes a connection for every query. I realize this is not what everyone needs. But I use this workflow in a lot of my projects, hence, opinionated.
- Uses ``yield`` to return a generator to fetch large amount of data from a DB without loading everything into the memory.
- Logs last executed query and time for query execution with a unique ID so queries can be traced in log messages.

Installation
------------

.. code-block::

    pip install rapyd_db

Make sure you are also installing the appropriate drivers for your respective backend as mentioned above.

Or you can install with driver like this:

.. code-block::

    pip install rapyd_db[mysql]

Usage
-----

MySQL Backend
*************

.. code-block::

    # import this
    from rapyd_db.backends.mysql import MySQL

    # create the DB object; no connection is done at this point
    # any argument accepted by the underlying DB driver can be passed
    # `cursorclass` will be overwritten
    db = MySQL(host='', user='', passwd='')

    # run queries like so

    # select large data with stream=True; returns a generator
    rows = db.execute("SELECT * FROM blah", stream=True)
    for row in rows:
        print(row)

    # insert data with stream=False (default); returns rows_affected, lastrowid and results of the query
    rows_affected, last_inserted_id, results = db.execute(
        'INSERT INTO blah(key) VALUE (%s)',
        ('value1', )
    )


This is an excerpt of the log messages using ``basicConfig``. This will change depending on your logging configuration::

    INFO:rapyd_db.backends:f2e47d87874d4055beba66b6c8221aff - Connecting to DB
    INFO:rapyd_db.backends.mysql:f2e47d87874d4055beba66b6c8221aff - Starting executing query at 2019-10-28 15:47:31.182261
    INFO:rapyd_db.backends.mysql:f2e47d87874d4055beba66b6c8221aff - SELECT * FROM blah
    INFO:rapyd_db.backends.mysql:f2e47d87874d4055beba66b6c8221aff - 2844047 row(s) affected in 10 second(s)
    INFO:rapyd_db.backends.mysql:f2e47d87874d4055beba66b6c8221aff - Ended query execution at 2019-10-28 15:47:41.747841
    INFO:rapyd_db.backends:f2e47d87874d4055beba66b6c8221aff - Closed connection to DB

MSSQL Backend
*************

TODO

Mongo Backend
*************

TODO

Testing
-------

For these tests to run successfully, the test rig should have access to a DB backend.

Running Tests
*************

A very easy way to set these environment variables would be to add them to a shell script file and source them like so:

.. code-block::

    source ./test_environment.sh

To run all tests, run the following command:

.. code-block::

    python -m unittest discover -v

    or

    make testall

To run tests for a specific backend, issue the following command:

.. code-block::

    python -m unittest -v rapyd_db.tests.test_mysql

    or

    make testmysql
