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

-- Pushdown in prepared statement.
INSERT INTO f_mongo_test VALUES ('0', 1, 'One');
INSERT INTO f_mongo_test VALUES ('0', 2, 'Two');
INSERT INTO f_mongo_test VALUES ('0', 3, 'Three');

PREPARE pre_stmt_f_mongo_test(int) AS
  SELECT b FROM f_mongo_test WHERE a = $1 ORDER BY b;
EXPLAIN (VERBOSE, COSTS FALSE)
EXECUTE pre_stmt_f_mongo_test(1);
EXECUTE pre_stmt_f_mongo_test(1);
EXPLAIN (VERBOSE, COSTS FALSE)
EXECUTE pre_stmt_f_mongo_test(2);
EXECUTE pre_stmt_f_mongo_test(2);

-- Cleanup
DELETE FROM f_mongo_test WHERE a != 0;
DROP FOREIGN TABLE f_mongo_test;
DROP FOREIGN TABLE f_test_tbl1;
DROP FOREIGN TABLE f_test_tbl2;
DROP USER MAPPING FOR public SERVER mongo_server;
DROP SERVER mongo_server;
DROP EXTENSION mongo_fdw;
