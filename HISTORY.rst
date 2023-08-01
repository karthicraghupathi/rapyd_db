. :changelog:

History
========

0.0.9 (2023-08-01)
------------------

- pymongo 4+ no longer executes operations after `MongoClient`` is closed.
  As a result, the `Mongo` backend will now return a `list` from the context
  manager to prevent the following error:
  ```
  pymongo.errors.InvalidOperation: Cannot use MongoClient after close
  ```

  - https://pymongo.readthedocs.io/en/stable/migrate-to-pymongo4.html#mongoclient-cannot-execute-operations-after-close

0.0.8 (2021-01-15)
------------------

- Switched from using `_last_executed` to `_executed` private member in
  MySQL / MariaDB backend as `mysqlclient` dropped `_last_executed` as part of:
  
  - https://github.com/PyMySQL/mysqlclient/blob/master/HISTORY.rst#whats-new-in-1314
  - https://github.com/PyMySQL/mysqlclient/pull/283
