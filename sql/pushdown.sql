\set MONGO_HOST			'\'localhost\''
\set MONGO_PORT			'\'27017\''
\set MONGO_USER_NAME	'\'edb\''
\set MONGO_PASS			'\'edb\''

-- Before running this file User must create database mongo_fdw_regress and
-- mongo_fdw_regress1 databases on MongoDB with all permission for 'edb' user
-- with 'edb' password and ran mongodb_init.sh file to load collections.

\c contrib_regression
CREATE EXTENSION IF NOT EXISTS mongo_fdw;
CREATE SERVER mongo_server FOREIGN DATA WRAPPER mongo_fdw
  OPTIONS (address :MONGO_HOST, port :MONGO_PORT);
CREATE USER MAPPING FOR public SERVER mongo_server;

-- Create foreign tables
CREATE FOREIGN TABLE f_mongo_test (_id name, a int, b varchar)
  SERVER mongo_server OPTIONS (database 'mongo_fdw_regress', collection 'mongo_test');
CREATE FOREIGN TABLE f_test_tbl1 (_id name, c1 INTEGER, c2 VARCHAR(10), c3 CHAR(9), c4 INTEGER, c5 pg_catalog.Date, c6 DECIMAL, c7 INTEGER, c8 INTEGER)
  SERVER mongo_server OPTIONS (database 'mongo_fdw_regress', collection 'test_tbl1');
CREATE FOREIGN TABLE f_test_tbl2 (_id name, c1 INTEGER, c2 VARCHAR(14), c3 VARCHAR(13))
  SERVER mongo_server OPTIONS (database 'mongo_fdw_regress', collection 'test_tbl2');
CREATE FOREIGN TABLE f_test_tbl3 (_id name, name TEXT, marks FLOAT ARRAY, pass BOOLEAN)
  SERVER mongo_server OPTIONS (database 'mongo_fdw_regress', collection 'test_tbl3');

-- Inserts some values in mongo_test collection.
INSERT INTO f_mongo_test VALUES ('0', 1, 'One');
INSERT INTO f_mongo_test VALUES ('0', 2, 'Two');
INSERT INTO f_mongo_test VALUES ('0', 3, 'Three');

SET datestyle TO ISO;

-- Sample data
SELECT c1, c2, c3, c4, c5, c6, c7, c8 FROM f_test_tbl1 ORDER BY c1;

-- WHERE clause pushdown
EXPLAIN (VERBOSE, COSTS FALSE)
SELECT c1, c2, c6 AS "salary", c8 FROM f_test_tbl1 e
  WHERE c6 IN (1600, 2450)
  ORDER BY c1;
SELECT c1, c2, c6 AS "salary", c8 FROM f_test_tbl1 e
  WHERE c6 IN (1600, 2450)
  ORDER BY c1;

EXPLAIN (VERBOSE, COSTS FALSE)
SELECT c1, c2, c6 FROM f_test_tbl1 e
  WHERE c6 > 3000
  ORDER BY c1;
SELECT c1, c2, c6 FROM f_test_tbl1 e
  WHERE c6 > 3000
  ORDER BY c1;

EXPLAIN (VERBOSE, COSTS FALSE)
SELECT c1, c2, c6, c8 FROM f_test_tbl1 e
  WHERE c6 = 1500
  ORDER BY c1;
SELECT c1, c2, c6, c8 FROM f_test_tbl1 e
  WHERE c6 = 1500
  ORDER BY c1;

EXPLAIN (VERBOSE, COSTS FALSE)
SELECT c1, c2, c6, c8 FROM f_test_tbl1 e
  WHERE c6 BETWEEN 1000 AND 4000
  ORDER BY c1;
SELECT c1, c2, c6, c8 FROM f_test_tbl1 e
  WHERE c6 BETWEEN 1000 AND 4000
  ORDER BY c1;

EXPLAIN (VERBOSE, COSTS FALSE)
SELECT c1, c2, c4, c6, c8 FROM f_test_tbl1 e
  WHERE c4 IS NOT NULL
  ORDER BY c1;
SELECT c1, c2, c4, c6, c8 FROM f_test_tbl1 e
  WHERE c4 IS NOT NULL
  ORDER BY c1;

EXPLAIN (VERBOSE, COSTS FALSE)
SELECT c1, c2, c5 FROM f_test_tbl1 e
  WHERE c5 <= '1980-12-17'
  ORDER BY c1;
SELECT c1, c2, c5 FROM f_test_tbl1 e
  WHERE c5 <= '1980-12-17'
  ORDER BY c1;

EXPLAIN (VERBOSE, COSTS FALSE)
SELECT c1, c2, c6, c8 FROM f_test_tbl1 e
  WHERE c2 IN ('EMP6', 'EMP12', 'EMP5')
  ORDER BY c1;
SELECT c1, c2, c6, c8 FROM f_test_tbl1 e
  WHERE c2 IN ('EMP6', 'EMP12', 'EMP5')
  ORDER BY c1;

EXPLAIN (VERBOSE, COSTS FALSE)
SELECT c1, c2, c6, c8 FROM f_test_tbl1 e
  WHERE c3 LIKE 'SALESMAN'
  ORDER BY c1;
SELECT c1, c2, c6, c8 FROM f_test_tbl1 e
  WHERE c3 LIKE 'SALESMAN'
  ORDER BY c1;

EXPLAIN (VERBOSE, COSTS FALSE)
SELECT c1, c2, c6, c8 FROM f_test_tbl1 e
  WHERE c3 LIKE 'MANA%'
  ORDER BY c1;
SELECT c1, c2, c6, c8 FROM f_test_tbl1 e
  WHERE c3 LIKE 'MANA%'
  ORDER BY c1;

EXPLAIN (VERBOSE, COSTS FALSE)
SELECT a FROM f_mongo_test
  WHERE a%2 = 1
  ORDER BY a;
SELECT a FROM f_mongo_test
  WHERE a%2 = 1
  ORDER BY a;

EXPLAIN (VERBOSE, COSTS FALSE)
SELECT a, b FROM f_mongo_test
  WHERE a >= 1 AND b LIKE '%O%'
  ORDER BY a;
SELECT a, b FROM f_mongo_test
  WHERE a >= 1 AND b LIKE '%O%'
  ORDER BY a;

EXPLAIN (VERBOSE, COSTS FALSE)
SELECT c1, c2, c5 FROM f_test_tbl1 e
  WHERE c5 <= '1980-12-17' AND c2 IN ('EMP1', 'EMP5', 'EMP10') AND c1 = 100
  ORDER BY c1;
SELECT c1, c2, c5 FROM f_test_tbl1 e
  WHERE c5 <= '1980-12-17' AND c2 IN ('EMP1', 'EMP5', 'EMP10') AND c1 = 100
  ORDER BY c1;

EXPLAIN (VERBOSE, COSTS FALSE)
SELECT c1, c2 FROM f_test_tbl1
  WHERE c2 = 'EMP10';
SELECT c1, c2 FROM f_test_tbl1
  WHERE c2 = 'EMP10';

EXPLAIN (VERBOSE, COSTS FALSE)
SELECT c1, c2 FROM f_test_tbl1
  WHERE c2 < 'EMP10';
SELECT c1, c2 FROM f_test_tbl1
  WHERE c2 < 'EMP10';

-- Should not push down if two columns of same table is
-- involved in single WHERE clause operator expression.
EXPLAIN (VERBOSE, COSTS FALSE)
SELECT c1, c6 FROM f_test_tbl1
  WHERE c1 = c6 AND c1 = 1100
  ORDER BY c1;
SELECT c1, c6 FROM f_test_tbl1
  WHERE c1 = c6 AND c1 = 1100
  ORDER BY c1;

-- Nested operator expression in WHERE clause. Shouldn't push down.
EXPLAIN (VERBOSE, COSTS FALSE)
SELECT c1, c2 FROM f_test_tbl1
  WHERE (c1 > 1000) > FALSE;
SELECT c1, c2 FROM f_test_tbl1
  WHERE (c1 > 1000) > FALSE;
EXPLAIN (VERBOSE, COSTS FALSE)
SELECT c1, c2 FROM f_test_tbl1
  WHERE (c1 > 1000) > 0::BOOLEAN;
SELECT c1, c2 FROM f_test_tbl1
  WHERE (c1 > 1000) > 0::BOOLEAN;

-- Shouldn't push down operators where the constant is an array.
EXPLAIN (VERBOSE, COSTS FALSE)
SELECT name, marks FROM f_test_tbl3
  WHERE marks = ARRAY[23::FLOAT, 24::FLOAT]
  ORDER BY name;
SELECT name, marks FROM f_test_tbl3
  WHERE marks = ARRAY[23::FLOAT, 24::FLOAT]
  ORDER BY name;

-- Pushdown in prepared statement.
PREPARE pre_stmt_f_mongo_test(int) AS
  SELECT b FROM f_mongo_test WHERE a = $1 ORDER BY b;
EXPLAIN (VERBOSE, COSTS FALSE)
EXECUTE pre_stmt_f_mongo_test(1);
EXECUTE pre_stmt_f_mongo_test(1);
EXPLAIN (VERBOSE, COSTS FALSE)
EXECUTE pre_stmt_f_mongo_test(2);
EXECUTE pre_stmt_f_mongo_test(2);

-- FDW-297: Only operator expressions should be pushed down in WHERE clause.
EXPLAIN (VERBOSE, COSTS FALSE)
SELECT name, marks FROM f_test_tbl3
  WHERE pass = true
  ORDER BY name;
SELECT name, marks FROM f_test_tbl3
  WHERE pass = true
  ORDER BY name;

-- Cleanup
DELETE FROM f_mongo_test WHERE a != 0;
DROP FOREIGN TABLE f_mongo_test;
DROP FOREIGN TABLE f_test_tbl1;
DROP FOREIGN TABLE f_test_tbl2;
DROP FOREIGN TABLE f_test_tbl3;
DROP USER MAPPING FOR public SERVER mongo_server;
DROP SERVER mongo_server;
DROP EXTENSION mongo_fdw;
