# mfnavdata
POC to getting Historical NAV of schemes from CMOTS and update in Postgres

# how to run
You need to sen the environment variables required for MF Database
```
go build
./navdata
```

This will pull all scheme codes from CMOTS and update the nav hostory for all ISINS

Note:
1. If you pass specific schemecodes as arguments it will only update nav hostory for those eg ./navdata 100.0 2001.0
2. If scheme code has multiple ISINs it will fill the entry for both

the db schema for NAV History is:

```
mfcore=# select * from nav_history;
    id    |     isin     | scheme_code | nav_value |  nav_date  
----------+--------------+-------------+-----------+------------
 69126541 | INF397L01075 | 11277.0     |   10.5465 | 2013-03-07
 69128594 | INF397L01075 | 11277.0     |   20.6837 | 2019-04-16
 ...
```

