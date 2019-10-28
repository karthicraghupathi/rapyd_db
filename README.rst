SQL Bootstrap
=============

An opinionated lightweight wrapper around various SQL backend drivers.

Support right now is available for the following:

- MySQL / MariaDB via ``mysqlclient``

Features
--------

- Opens and closes a connection for every query. I realize this is not what everyone needs. But I use this workflow in a lot of my projects, hence, opinionated.
- Uses ``yield`` to return a generator to fetch large amount of data from a DB without loading everything into the memory.
- Logs last executed query and time for query execution with a unique ID so queries can be traced in log messages.

Installation
------------

.. code-block::

    pip install sql_bootstrap

Make sure you are also installing the appropriate drivers for your respective backend as mentioned above.

Or you can install with driver like this:

.. code-block::

    pip install sql_bootstrap[mysql]

Usage
-----

.. code-block::

    # import this
    from sql_bootstrap.backends.mysql import MySQL

    # create the DB object; no connection is done at this point
    # any argument accepted by the underlying DB driver can be passed
    db = MySQL(host='', user='', passwd='', db='')

    # run queries like so

    # select large data
    rows = db.execute("SELECT * FROM blah")
    for row in rows:
        print(row)

    # insert data
    db.execute(
        'INSERT INTO blah(key) VALUE (%s)',
        ('value1', )
    )


This is an excerpt of the log messages using ``basicConfig``. This will change depending on your logging configuration::

    INFO:sql_bootstrap.backends:f2e47d87874d4055beba66b6c8221aff - Connecting to DB
    INFO:sql_bootstrap.backends.mysql:f2e47d87874d4055beba66b6c8221aff - Starting executing query at 2019-10-28 15:47:31.182261
    INFO:sql_bootstrap.backends.mysql:f2e47d87874d4055beba66b6c8221aff - SELECT * FROM blah
    INFO:sql_bootstrap.backends.mysql:f2e47d87874d4055beba66b6c8221aff - 2844047 row(s) affected in 10 second(s)
    INFO:sql_bootstrap.backends.mysql:f2e47d87874d4055beba66b6c8221aff - Ended query execution at 2019-10-28 15:47:41.747841
    INFO:sql_bootstrap.backends:f2e47d87874d4055beba66b6c8221aff - Closed connection to DB

TODO
----

- Add support for other DB backends.
- Add tests.
