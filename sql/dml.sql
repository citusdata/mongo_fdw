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
CREATE FOREIGN TABLE f_mongo_test (_id name, a int, b varchar) SERVER mongo_server
  OPTIONS (database 'mongo_fdw_regress', collection 'mongo_test');
CREATE FOREIGN TABLE f_mongo_test1 (_id name, a int, b varchar) SERVER mongo_server
  OPTIONS (database 'mongo_fdw_regress1', collection 'mongo_test1');
CREATE FOREIGN TABLE f_mongo_test2 (_id name, a int, b varchar) SERVER mongo_server
  OPTIONS (database 'mongo_fdw_regress2', collection 'mongo_test2');
-- Creating foreign table without specifying database.
CREATE FOREIGN TABLE f_mongo_test3 (_id name, a int, b varchar) SERVER mongo_server
  OPTIONS (collection 'mongo_test3');

-- Verify the INSERT/UPDATE/DELETE operations on a collection (mongo_test)
-- exist in a database (mongo_fdw_regress) in mongoDB.
SELECT a,b FROM f_mongo_test ORDER BY 1, 2;
INSERT INTO f_mongo_test VALUES ('0', 10 , 'INSERT');
SELECT a,b FROM f_mongo_test ORDER BY 1, 2;
UPDATE f_mongo_test SET b = 'UPDATE' WHERE a = 10;
SELECT a,b FROM f_mongo_test ORDER BY 1, 2;
DELETE FROM f_mongo_test WHERE a = 10;
SELECT a,b FROM f_mongo_test ORDER BY 1, 2;

-- Verify the INSERT/UPDATE/DELETE operations on a collection (mongo_test1)
-- not exist in a database (mongo_fdw_regress1) in mongoDB.
SELECT a,b FROM f_mongo_test1 ORDER BY 1, 2;
INSERT INTO f_mongo_test1 VALUES ('0', 10 , 'INSERT');
SELECT a,b FROM f_mongo_test1 ORDER BY 1, 2;
UPDATE f_mongo_test1 SET b = 'UPDATE' WHERE a = 10;
SELECT a,b FROM f_mongo_test1 ORDER BY 1, 2;
DELETE FROM f_mongo_test1 WHERE a = 10;
SELECT a,b FROM f_mongo_test1 ORDER BY 1, 2;

-- Verify the INSERT/UPDATE/DELETE operations on a collection (mongo_test2)
-- not exist in a non exist database (mongo_fdw_regress2) in mongoDB.
SELECT a,b FROM f_mongo_test2 ORDER BY 1, 2;
INSERT INTO f_mongo_test2 VALUES ('0', 10 , 'INSERT');
SELECT a,b FROM f_mongo_test2 ORDER BY 1, 2;
UPDATE f_mongo_test2 SET b = 'UPDATE' WHERE a = 10;
SELECT a,b FROM f_mongo_test2 ORDER BY 1, 2;
DELETE FROM f_mongo_test2 WHERE a = 10;
SELECT a,b FROM f_mongo_test2 ORDER BY 1, 2;

-- Verify the INSERT/UPDATE/DELETE operations on a collection (mongo_test)
-- when foreign table created without database option.
SELECT a,b FROM f_mongo_test3 ORDER BY 1, 2;
INSERT INTO f_mongo_test3 VALUES ('0', 10 , 'INSERT');
SELECT a,b FROM f_mongo_test3 ORDER BY 1, 2;
UPDATE f_mongo_test3 SET b = 'UPDATE' WHERE a = 10;
SELECT a,b FROM f_mongo_test3 ORDER BY 1, 2;
DELETE FROM f_mongo_test3 WHERE a = 10;
SELECT a,b FROM f_mongo_test3 ORDER BY 1, 2;

-- FDW-158: Fix server crash when analyzing a foreign table.
SELECT reltuples FROM pg_class WHERE relname = 'f_mongo_test';
ANALYZE f_mongo_test;
-- Should give correct number of rows now.
SELECT reltuples FROM pg_class WHERE relname = 'f_mongo_test';
-- Check count using select query on table.
SELECT count(*) FROM f_mongo_test;

-- Some more variants of vacuum and analyze
VACUUM f_mongo_test;
VACUUM FULL f_mongo_test;
VACUUM FREEZE f_mongo_test;
ANALYZE f_mongo_test;
ANALYZE f_mongo_test(a);
VACUUM ANALYZE f_mongo_test;

-- Cleanup
DROP FOREIGN TABLE f_mongo_test;
DROP FOREIGN TABLE f_mongo_test1;
DROP FOREIGN TABLE f_mongo_test2;
DROP FOREIGN TABLE f_mongo_test3;
DROP USER MAPPING FOR public SERVER mongo_server;
DROP SERVER mongo_server;
DROP EXTENSION mongo_fdw;
