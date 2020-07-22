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

-- Validate extension, server and mapping details
SELECT e.fdwname AS "Extension", srvname AS "Server", s.srvoptions AS "Server_Options", u.umoptions AS "User_Mapping_Options"
  FROM pg_foreign_data_wrapper e LEFT JOIN pg_foreign_server s ON e.oid = s.srvfdw LEFT JOIN pg_user_mapping u ON s.oid = u.umserver
  WHERE e.fdwname = 'mongo_fdw'
  ORDER BY 1, 2, 3, 4;

-- Create foreign tables and perform basic SQL operations
CREATE FOREIGN TABLE f_mongo_test (_id name, a int, b varchar)
  SERVER mongo_server OPTIONS (database 'mongo_fdw_regress', collection 'mongo_test');
SELECT a, b FROM f_mongo_test ORDER BY 1, 2;
INSERT INTO f_mongo_test VALUES ('0', 2, 'mongo_test insert');
SELECT a, b FROM f_mongo_test ORDER BY 1, 2;
UPDATE f_mongo_test SET b = 'mongo_test update' WHERE a = 2;
SELECT a, b FROM f_mongo_test ORDER BY 1, 2;
DELETE FROM f_mongo_test WHERE a = 2;
SELECT a, b FROM f_mongo_test ORDER BY 1, 2;
DROP FOREIGN TABLE f_mongo_test;
DROP USER MAPPING FOR public SERVER mongo_server;
DROP SERVER mongo_server;

-- Create server with authentication_database option
-- authentication_database options is not supported with legacy driver
-- so below queries will fail when compiled with legacy driver.
CREATE SERVER mongo_server FOREIGN DATA WRAPPER mongo_fdw
  OPTIONS (address :MONGO_HOST, port :MONGO_PORT, authentication_database 'NOT_EXIST_DB');
CREATE USER MAPPING FOR public SERVER mongo_server
  OPTIONS (username :MONGO_USER_NAME, password :MONGO_PASS);
CREATE FOREIGN TABLE f_mongo_test (_id name, a int, b varchar)
  SERVER mongo_server OPTIONS (database 'mongo_fdw_regress', collection 'mongo_test');
-- Below query will fail with authentication error as user cannot be
-- authenticated against given authentication_database.
SELECT a, b FROM f_mongo_test ORDER BY 1, 2;
-- Now changed to valid authentication_database so select query should work.
ALTER SERVER mongo_server
  OPTIONS (SET authentication_database 'mongo_fdw_regress');
SELECT a, b FROM f_mongo_test ORDER BY 1, 2;

-- Cleanup
DROP FOREIGN TABLE f_mongo_test;
DROP USER MAPPING FOR public SERVER mongo_server;
DROP SERVER mongo_server;
DROP EXTENSION mongo_fdw;
