# MongoDB Foreign Data Wrapper for PostgreSQL

This PostgreSQL extension implements a Foreign Data Wrapper (FDW) for
[MongoDB][1].

Please note that this version of mongo_fdw works with PostgreSQL and EDB
Postgres Advanced Server 10, 11, 12, 13, and 14.

Installation
------------
To compile the [MongoDB][1] foreign data wrapper, mongo-c and json-c
libraries are needed. To build and install mongo-c and json-c libraries, there
are two ways. You can either use script `autogen.sh` or you can manually
perform all required steps listed.

## Installation using script
Number of manual steps needs to be performed to compile and install required
mongo-c and json-c libraries. If you want to avoid the manual steps, there is a
shell script available which will download and install the appropriate drivers
and libraries for you.

Here is how it works:

To install mongo-c and json-c libraries at custom locations, you need to
export environment variables `MONGOC_INSTALL_DIR` and `JSONC_INSTALL_DIR`
respectively. If these variables are not set then these libraries will be
installed in the default location. Please note that you need to have the
required permissions on the directory where you want to install the libraries.

Build with [MongoDB][1]'s legacy branch driver
   * autogen.sh --with-legacy

Build [MongoDB][1]'s master branch driver
   * autogen.sh --with-master

The script autogen.sh will do all the necessary steps to build with legacy and
meta driver accordingly.

## Steps for manual installation
### mongo-c
#### meta driver
1. Download and extract source code of mongoc driver for version `1.17.3`

	```sh
	wget https://github.com/mongodb/mongo-c-driver/releases/download/1.17.3/mongo-c-driver-1.17.3.tar.gz
	tar xzf mongo-c-driver-1.17.3.tar.gz
	rm -rf mongo-c-driver
	mv mongo-c-driver-1.17.3 mongo-c-driver
	cd mongo-c-driver
	```

2. Configure mongoc driver

	```sh
	cmake -DENABLE_AUTOMATIC_INIT_AND_CLEANUP=OFF .
	```

	To install at custom location:

	```sh
	cmake -DCMAKE_INSTALL_PREFIX=YOUR_INSTALLATION_DIRECTORY -DENABLE_AUTOMATIC_INIT_AND_CLEANUP=OFF .
	```

3. Compile and install

	```sh
	cmake --build .
	cmake --build . --target install
	```

For more details on installation of mongo-c driver, you can refer [here][5].

#### Legacy driver
* Checkout, extract legacy branch

	```sh
	wget https://github.com/mongodb/mongo-c-driver/archive/v0.8.tar.gz
	tar -zxf v0.8.tar.gz
	rm -rf mongo-c-driver
	mv  mongo-c-driver-0.8 mongo-c-driver
	```

### json-c
1. Download and extract source code

	```sh
	wget https://github.com/json-c/json-c/archive/json-c-0.15-20200726.tar.gz
	tar -xzf json-c-0.15-20200726.tar.gz
	rm -rf json-c
	mv json-c-json-c-0.15-20200726/ json-c
	cd json-c
	```

2. Configure

	```sh
	cmake .
	```
	To install at custom location:

	```sh
	cmake -DCMAKE_INSTALL_PREFIX=YOUR_INSTALLATION_DIRECTORY .
	```

3. Compile and install

	```sh
	make
	make install
	```

For more details on installation of json-c library, you can refer [here][6].

### How to compile against mongo-c Meta or Legacy driver?
To compile against legacy driver, 'Makefile.legacy' must be used and
'Makefile.meta' must be used to compile against the meta driver. For example,
this can be achieved by copying required Makefile as shown below:
For meta,

	cp Makefile.meta Makefile

For legacy,

	cp Makefile.legacy Makefile

The default compilation is with Meta driver.

## Mongo_fdw configuration, compilation and installation
The `PKG_CONFIG_PATH` environment variable must be set to mongo-c-driver source
directory for successful compilation as shown below,

```sh
export PKG_CONFIG_PATH=$YOUR_MONGO_FDW_SOURCE_DIR/mongo-c-driver/src/libmongoc/src:$YOUR_MONGO_FDW_SOURCE_DIR/mongo-c-driver/src/libbson/src
```

The `LD_LIBRARY_PATH` environment variable must include the path to the mongo-c
installation directory containing the libmongoc-1.0.so and libbson-1.0.so
files. For example, assuming the installation directory is /home/mongo-c and
the libraries were created under it in lib64 sub-directory, then we can define
the `LD_LIBRARY_PATH` as:

```sh
export LD_LIBRARY_PATH=/home/mongo-c/lib64:$LD_LIBRARY_PATH
```

Note: This `LD_LIBRARY_PATH` environment variable setting must be in effect
when the `pg_ctl` utility is executed to start or restart PostgreSQL or
EDB Postgres Advanced Server.


1. To build on POSIX-compliant systems you need to ensure the
   `pg_config` executable is in your path when you run `make`. This
   executable is typically in your PostgreSQL installation's `bin`
   directory. For example:

    ```sh
    export PATH=/usr/local/pgsql/bin/:$PATH
    ```

2. Compile the code using make.

    ```sh
    make USE_PGXS=1
    ```

3. Finally install the foreign data wrapper.

    ```sh
    make USE_PGXS=1 install
    ```

4. Running regression test.

    ```sh
    make USE_PGXS=1 installcheck
    ```
   However, make sure to set the `MONGO_HOST`, `MONGO_PORT`, `MONGO_USER_NAME`,
   and `MONGO_PWD` environment variables correctly. The default settings can be
   found in the `mongodb_init.sh` script.


If you run into any issues, please [let us know][2].

Enhancements
-----------
The following enhancements are added to the latest version of mongo_fdw:

### Write-able FDW
The previous version was only read-only, the latest version provides the
write capability. The user can now issue an insert, update, and delete
statements for the foreign tables using the mongo_fdw.

### Connection Pooling
The latest version comes with a connection pooler that utilizes the
same mango database connection for all the queries in the same session.
The previous version would open a new [MongoDB][1] connection for every
query. This is a performance enhancement.

### JOIN push-down
mongo_fdw now also supports join push-down. The joins between two
foreign tables from the same remote MySQL server are pushed to a remote
server, instead of fetching all the rows for both the tables and
performing a join locally, thereby may enhance the performance. Currently,
joins involving only relational and arithmetic operators in join-clauses
are pushed down to avoid any potential join failure. Also, only the
INNER and LEFT/RIGHT OUTER joins are supported, and not the FULL OUTER,
SEMI, and ANTI join. Moreover, only joins between two tables are pushed
down and not when either inner or outer relation is the join itself.

### New MongoDB C Driver Support
The third enhancement is to add a new [MongoDB][1]' C driver. The
current implementation is based on the legacy driver of MongoDB. But
[MongoDB][1] is provided completely new library for driver called
MongoDB's meta driver. Added support for the same. Now compile time
option is available to use legacy and meta driver.

In order to use MongoDB driver 1.17.0+, take the following steps:

  * clone `libmongoc` version 1.17.0+
    (https://github.com/mongodb/mongo-c-driver) and follow the install
    directions given there.  `libbson` is now maintained in a subdirectory
    of the `libmongoc`.
    (https://github.com/mongodb/mongo-c-driver/tree/master/src/libbson).
  * ensure pkg-config / pkgconf is installed on your system.
  * run `make -f Makefile.meta && make -f Makefile.meta install`
  * if you get an error when trying to `CREATE EXTENSION mongo_fdw;`,
    then try running `ldconfig`

Usage
-----
The following parameters can be set on a MongoDB foreign server object:

  * `address`: Address or hostname of the MongoDB server. Defaults to
    `127.0.0.1`
  * `port`: Port number of the MongoDB server. Defaults to `27017`.
  * `use_remote_estimate`: Controls whether mongo_fdw uses exact rows from
    remote collection to obtain cost estimates. Default is `false`.

The following options are only supported with meta driver:

  * `authentication_database`: Database against which user will be
    authenticated against. Only valid with password based authentication.
  * `replica_set`: Replica set the server is member of. If set,
    driver will auto-connect to correct primary in the replica set when
    writing.
  * `read_preference`: primary [default], secondary, primaryPreferred,
    secondaryPreferred, or nearest.
  * `ssl`: false [default], true to enable ssl. See
    http://mongoc.org/libmongoc/current/mongoc_ssl_opt_t.html to
    understand the options.
  * `pem_file`: The .pem file that contains both the TLS/SSL certificate and
    key.
  * `pem_pwd`: The password to decrypt the certificate key file(i.e. pem_file)
  * `ca_file`: The .pem file that contains the root certificate chain from the
    Certificate Authority.
  * `ca_dir`: The absolute path to the `ca_file`.
  * `crl_file`: The .pem file that contains the Certificate Revocation List.
  * `weak_cert_validation`: false [default], This is to enable or disable the
    validation checks for TLS/SSL certificates and allows the use of invalid
	certificates to connect if set to `true`.
  * `enable_join_pushdown`: If `true`, pushes the join between two foreign
	tables from the same foreign server, instead of fetching all the rows
	for both the tables and performing a join locally. This option can also
	be set for an individual table, and if any of the tables involved in the
	join has set it to false then the join will not be pushed down. The
	table-level value of the option takes precedence over the server-level
	option value. Default is `true`.

The following parameters can be set on a MongoDB foreign table object:

  * `database`: Name of the MongoDB database to query. Defaults to
    `test`.
  * `collection`: Name of the MongoDB collection to query. Defaults to
    the foreign table name used in the relevant `CREATE` command.
  * `enable_join_pushdown`: Similar to the server-level option, but can be
    configured at table level as well. Default is `true`.

The following parameters can be supplied while creating user mapping:

  * `username`: Username to use when connecting to MongoDB.
  * `password`: Password to authenticate to the MongoDB server.

As an example, the following commands demonstrate loading the
`mongo_fdw` wrapper, creating a server, and then creating a foreign
table associated with a MongoDB collection. The commands also show
specifying option values in the `OPTIONS` clause. If an option value
isn't provided, the wrapper uses the default value mentioned above.

`mongo_fdw` can collect data distribution statistics will incorporate
them when estimating costs for the query execution plan. To see selected
execution plans for a query, just run `EXPLAIN`.

Examples
--------

Examples with [MongoDB][1]'s equivalent statements.

```sql
-- load extension first time after install
CREATE EXTENSION mongo_fdw;

-- create server object
CREATE SERVER mongo_server
	FOREIGN DATA WRAPPER mongo_fdw
	OPTIONS (address '127.0.0.1', port '27017');

-- create user mapping
CREATE USER MAPPING FOR postgres
	SERVER mongo_server
	OPTIONS (username 'mongo_user', password 'mongo_pass');

-- create foreign table
CREATE FOREIGN TABLE warehouse
	(
		_id name,
		warehouse_id int,
		warehouse_name text,
		warehouse_created timestamptz
	)
	SERVER mongo_server
	OPTIONS (database 'db', collection 'warehouse');

-- Note: first column of the table must be "_id" of type "name".

-- select from table
SELECT * FROM warehouse WHERE warehouse_id = 1;
           _id            | warehouse_id | warehouse_name |     warehouse_created
--------------------------+--------------+----------------+---------------------------
 53720b1904864dc1f5a571a0 |            1 | UPS            | 2014-12-12 12:42:10+05:30
(1 row)

db.warehouse.find
(
	{
		"warehouse_id" : 1
	}
).pretty()
{
	"_id" : ObjectId("53720b1904864dc1f5a571a0"),
	"warehouse_id" : 1,
	"warehouse_name" : "UPS",
	"warehouse_created" : ISODate("2014-12-12T07:12:10Z")
}

-- insert row in table
INSERT INTO warehouse VALUES (0, 2, 'Laptop', '2015-11-11T08:13:10Z');

db.warehouse.insert
(
	{
		"warehouse_id" : NumberInt(2),
		"warehouse_name" : "Laptop",
		"warehouse_created" : ISODate("2015-11-11T08:13:10Z")
	}
)

-- delete row from table
DELETE FROM warehouse WHERE warehouse_id = 2;

db.warehouse.remove
(
	{
		"warehouse_id" : 2
	}
)

-- update a row of table
UPDATE warehouse SET warehouse_name = 'UPS_NEW' WHERE warehouse_id = 1;

db.warehouse.update
(
	{
		"warehouse_id" : 1
	},
	{
		"warehouse_id" : 1,
		"warehouse_name" : "UPS_NEW",
		"warehouse_created" : ISODate("2014-12-12T07:12:10Z")
	}
)

-- explain a table
EXPLAIN SELECT * FROM warehouse WHERE warehouse_id = 1;
                           QUERY PLAN
-----------------------------------------------------------------
 Foreign Scan on warehouse  (cost=0.00..0.00 rows=1000 width=84)
   Filter: (warehouse_id = 1)
   Foreign Namespace: db.warehouse
(3 rows)

-- collect data distribution statistics
ANALYZE warehouse;

```

Limitations
-----------

  * If the BSON document key contains uppercase letters or occurs within
    a nested document, `mongo_fdw` requires the corresponding column names
    to be declared in double quotes.

  * Note that PostgreSQL limits column names to 63 characters by
    default. If you need column names that are longer, you can increase the
    `NAMEDATALEN` constant in `src/include/pg_config_manual.h`, compile,
    and re-install.


Contributing
------------
Have a fix for a bug or an idea for a great new feature? Great! Check
out the contribution guidelines [here][3].


Support
-------
This project will be modified to maintain compatibility with new
PostgreSQL and EDB Postgres Advanced Server releases.

If you need commercial support, please contact the EnterpriseDB sales
team, or check whether your existing PostgreSQL support provider can
also support mongo_fdw.


License
-------
Portions Copyright (c) 2004-2022, EnterpriseDB Corporation.
Portions Copyright © 2012–2014 Citus Data, Inc.

This program is free software: you can redistribute it and/or modify it
under the terms of the GNU Lesser General Public License as published by
the Free Software Foundation, either version 3 of the License, or (at
your option) any later version.

See the [`LICENSE`][4] file for full details.

[1]: http://www.mongodb.com
[2]: https://github.com/enterprisedb/mongo_fdw/issues/new
[3]: CONTRIBUTING.md
[4]: LICENSE
[5]: http://mongoc.org/libmongoc/1.17.3/installing.html#configuring-the-build
[6]: https://github.com/json-c/json-c/tree/json-c-0.15-20200726#build-instructions--
