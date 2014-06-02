MongoDB Foreign Date Wrapper for PostgreSQL
===========================================

This [MongoDB][1] extension implements the PostgreSQL's Foreign Data Wrapper for. 

Please also note that this version of `mongo_fdw` only works with
PostgreSQL Version 9.3 and greater.


Installation
------------

This [MongoDB][1] FDW is compatible with two [MongoDB][1]'s ""C"" drivers, [legacy driver][6] and [MongoDB][1]'s [Meta Driver][7]. This is compile time decision which driver you want to use with this FDW.

The [MongoDB][1] FDW includes the [legacy MongoDB C Driver][6] version 0.6. When you
type `make -f Makefile.legacy`, the C driver's source code also gets automatically compiled and linked. The other option is to compile using [MongoDB's Meta Driver][7]. You need to download the Meta C driver from MongoDB site.

To build on POSIX-compliant systems (like Linux and OS X), you need to ensure
the `pg_config` executable is in your path when you run `make`. This executable
is typically in your PostgreSQL installation's `bin` directory. For example:

Compile using MonoDB's legacy C driver.

```sh
PATH=/usr/local/pgsql/bin/:$PATH make -f Makefile.legacy
sudo PATH=/usr/local/pgsql/bin/:$PATH make install
```

Compile using MonoDB's Meta C driver.

Add `#deine META_DRIVER` in `config.h` file, then

```sh
PATH=/usr/local/pgsql/bin/:$PATH make -f Makefile.meta
sudo PATH=/usr/local/pgsql/bin/:$PATH make install
```

Note that we have tested the `mongo_fdw` extension only on MacOS X, Fedora and Ubuntu
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

```sql
Examples with MongoDB equelent statments.

-- load extension first time after install
`CREATE EXTENSION mongo_fdw;`

-- create server object
`CREATE SERVER mongo_server
         FOREIGN DATA WRAPPER mongo_fdw
         OPTIONS (address '127.0.0.1', port '27017');`

-- create foreign table
`CREATE FOREIGN TABLE warehouse(
		 _id NAME,
         warehouse_id int,
         warehouse_name text,
         warehouse_created timestamptz)
SERVER mongo_server
         OPTIONS (database 'db', collection 'warehouse');`


-- select from table
`SELECT * FROM warehouse WHERE warehouse_id = 1;`

_id                     | warehouse_id | warehouse_name |     warehouse_created
------------------------+----------------+---------------------------
53720b1904864dc1f5a571a0|            1 | UPS            | 12-DEC-14 12:12:10 +05:00


`db.warehouse.find({"warehouse_id" : 1}).pretty()`
{
	"_id" : ObjectId("53720b1904864dc1f5a571a0"),
	"warehouse_id" : 1,
	"warehouse_name" : "UPS",
	"warehouse_created" : ISODate("2014-12-12T07:12:10Z")
}


-- insert row in table
`INSERT INTO warehouse values (0, 1, 'UPS', to_date('2014-12-12T07:12:10Z'));`

`db.warehouse.insert`
(
    {
        "warehouse_id" : NumberInt(1),
        "warehouse_name" : "UPS",
        "warehouse_created" : ISODate("2014-12-12T07:12:10Z")
    }
);

-- delete row from table
`DELETE FROM warehouse where warehouse_id = 3;`

>    db.warehouse.remove({"warehouse_id" : 2})


-- update a row of table
`UPDATE warehouse set warehouse_name = 'UPS_NEW' where warehouse_id = 1;`

db.warehouse.update
(
   {
        "warehouse_id" : 1
   },
   {
        "warehouse_id" : 1,
        "warehouse_name" : "UPS_NEW"
   }
)

-- explain a table
`EXPLAIN SELECT * FROM warehouse WHERE warehouse_id = 1;
                           QUERY PLAN
 -----------------------------------------------------------------
 Foreign Scan on warehouse  (cost=0.00..0.00 rows=1000 width=44)
   Filter: (warehouse_id = 1)
   Foreign Namespace: db.warehouse
 Planning time: 0.671 ms
(4 rows)`

-- collect data distribution statistics`
ANALYZE warehouse;
```

Limitations
-----------

  * If the BSON document key contains uppercase letters or occurs within a
    nested document, `mongo_fdw` requires the corresponding column names to be
	declared in double quotes. For example, a nested field such as `"review": {
	"Votes": 19 }` should be declared as `"review.Votes" INTEGER` in the `CREATE
	TABLE` statement.

  * Note that PostgreSQL limits column names to 63 characters by default. If
    you need column names that are longer, you can increase the `NAMEDATALEN`
	constant in `src/include/pg_config_manual.h`, compile, and reinstall.


Contributing
------------

Have a fix for a bug or an idea for a great new feature? Great! Check out the
contribution guidelines [here][4]. For all other types of questions or comments
about the wrapper please contact us at `ibrar.ahmed` `@` `enterprisedb.com`.


Support
-------

This project will be modified to maintain compatibility with new PostgreSQL
releases. The project owners set aside a day every month to look over open
issues and support emails, but are not engaged in active feature development.
Reported bugs will be addressed by apparent severity.


License
-------

Portions Copyright © 2004-2014, EnterpriseDB Corporation.
Portions Copyright © 2012–2014 Citus Data, Inc.

This program is free software: you can redistribute it and/or modify it under
the terms of the GNU Lesser General Public License as published by the Free
Software Foundation, either version 3 of the License, or (at your option) any
later version.

See the [`LICENSE`][5] file for full details.

[1]: http://www.mongodb.com
[2]: http://www.citusdata.com/blog/51-run-sql-on-mongodb
[3]: https://github.com/ibrarahmad/mongo_fdw/issues/new
[4]: CONTRIBUTING.md
[5]: LICENSE
[6]: https://github.com/mongodb/mongo-c-driver-legacy
[7]: https://github.com/mongodb/mongo-meta-driver
