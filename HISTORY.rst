. :changelog:

History
========

0.0.8 (2021-01-15)
------------------

- Switched from using `_last_executed` to `_executed` private member in
  MySQL / MariaDB backend as `mysqlclient` dropped `_last_executed` as part of:
  
  - https://github.com/PyMySQL/mysqlclient/blob/master/HISTORY.rst#whats-new-in-1314
  - https://github.com/PyMySQL/mysqlclient/pull/283
