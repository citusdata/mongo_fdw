MongoDB FDW for PostgreSQL
==========================

This PostgreSQL extension implements a Foreign Data Wrapper (FDW) for
[MongoDB][1]. For an example demonstrating this wrapper's use, see [our blog
post][2]. Please also note that this version of `mongo_fdw` only works with
PostgreSQL 9.2 or 9.3.


Installation
------------

The MongoDB FDW includes the official MongoDB C Driver version 0.6. When you
type `make`, the C driver's source code also gets automatically compiled and
linked.

To build on POSIX-compliant systems (like Linux and OS X), you need to ensure
the `pg_config` executable is in your path when you run `make`. This executable
is typically in your PostgreSQL installation's `bin` directory. For example:

```sh
PATH=/usr/local/pgsql/bin/:$PATH make
sudo PATH=/usr/local/pgsql/bin/:$PATH make install
```

Note that we have tested the `mongo_fdw` extension only on Fedora and Ubuntu
systems. If you run into issues on other systems, please [let us know][3].


Usage
-----

The following parameters can be set on a MongoDB foreign server object:

  * `address`: the address or hostname of the MongoDB server.
               Defaults to `127.0.0.1`
  * `port`: the port number of the MongoDB server. Defaults to `27017`

The following parameters can be set on a MongoDB foreign table object:

  * `database`: the name of the MongoDB database to query. Defaults to `test`
  * `collection`: the name of the MongoDB collection to query. Defaults to
                  the foreign table name used in the relevant `CREATE` command

As an example, the following commands demonstrate loading the `mongo_fdw`
wrapper, creating a server, and then creating a foreign table associated with
a MongoDB collection. The commands also show specifying option values in the
`OPTIONS` clause. If an option value isn't provided, the wrapper uses the
default value mentioned above.

`mongo_fdw` can collect data distribution statistics will incorporate them when
estimating costs for the query execution plan. To see selected execution plans
for a query, just run `EXPLAIN`.

We also currently use the internal PostgreSQL `NAME` type to represent the BSON
object identifier type (the `_id` field).

```sql
-- load extension first time after install
CREATE EXTENSION mongo_fdw;

-- create server object
CREATE SERVER mongo_server FOREIGN DATA WRAPPER mongo_fdw
OPTIONS (address '127.0.0.1', port '27017');

-- create foreign table
CREATE FOREIGN TABLE customer_reviews
(
    _id NAME,
    customer_id TEXT,
    review_date TIMESTAMP,
    review_rating INTEGER,
    product_id CHAR(10),
    product_title TEXT,
    product_group TEXT,
    product_category TEXT,
    similar_product_ids CHAR(10)[]
)
SERVER mongo_server
OPTIONS (database 'test', collection 'customer_reviews');

-- collect data distribution statistics
ANALYZE customer_reviews;
```


Limitations
-----------

  * If the BSON document key contains uppercase letters or occurs within a
    nested document, `mongo_fdw` requires the corresponding column names to be
	declared in double quotes. For example, a nested field such as `"review": {
	"Votes": 19 }` should be declared as `"review.Votes" INTEGER` in the create
	table statement.

  * Note that PostgreSQL limits column names to 63 characters by default. If
    you need column names that are longer, you can increase the `NAMEDATALEN`
	constant in `src/include/pg_config_manual.h`, compile, and reinstall.


License
-------

Copyright © 2012–2014 Citus Data, Inc.

This program is free software: you can redistribute it and/or modify it under
the terms of the GNU Lesser General Public License as published by the Free
Software Foundation, either version 3 of the License, or (at your option) any
later version.

See the [LICENSE][4] file for full details.

[1]: http://www.mongodb.com
[2]: http://www.citusdata.com/blog/51-run-sql-on-mongodb
[3]: https://github.com/citusdata/mongo_fdw/issues/new
[4]: LICENSE
