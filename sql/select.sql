\set MONGO_HOST			'\'localhost\''
\set MONGO_PORT			'\'27017\''
\set MONGO_USER_NAME	'\'edb\''
\set MONGO_PASS			'\'edb\''

-- Before running this file User must create database mongo_fdw_regress &
-- mongo_fdw_regress1 databases on MongoDB with all permission for
-- 'edb' user with 'edb' password and ran mongodb_init.sh
-- file to load collections.

\c contrib_regression
CREATE EXTENSION IF NOT EXISTS mongo_fdw;
CREATE SERVER mongo_server FOREIGN DATA WRAPPER mongo_fdw
  OPTIONS (address :MONGO_HOST, port :MONGO_PORT);
CREATE USER MAPPING FOR public SERVER mongo_server;

-- Create foreign tables
CREATE FOREIGN TABLE f_mongo_test (_id name, a int, b varchar)
  SERVER mongo_server OPTIONS (database 'mongo_fdw_regress', collection 'mongo_test');
CREATE FOREIGN TABLE f_test_tbl1 (_id NAME, c1 INTEGER, c2 VARCHAR(10), c3 CHAR(9),c4 INTEGER, c5 pg_catalog.Date, c6 DECIMAL, c7 INTEGER, c8 INTEGER)
  SERVER mongo_server OPTIONS (database 'mongo_fdw_regress', collection 'test_tbl1');
CREATE FOREIGN TABLE f_test_tbl2 (_id NAME, c1 INTEGER, c2 VARCHAR(14), c3 VARCHAR(13))
  SERVER mongo_server OPTIONS (database 'mongo_fdw_regress', collection 'test_tbl2');
CREATE FOREIGN TABLE countries (_id NAME, name VARCHAR, population INTEGER, capital VARCHAR, hdi FLOAT)
  SERVER mongo_server OPTIONS (database 'mongo_fdw_regress', collection 'countries');
CREATE FOREIGN TABLE country_elections (_id NAME, "lastElections.type" VARCHAR, "lastElections.date" pg_catalog.TIMESTAMP)
  SERVER mongo_server OPTIONS (database 'mongo_fdw_regress', collection 'countries');
CREATE FOREIGN TABLE main_exports (_id NAME, "mainExports" TEXT[] )
  SERVER mongo_server OPTIONS (database 'mongo_fdw_regress', collection 'countries');
CREATE FOREIGN TABLE test_json ( __doc json)
  SERVER mongo_server OPTIONS (database 'mongo_fdw_regress', collection 'warehouse');
CREATE FOREIGN TABLE test_jsonb ( __doc jsonb)
  SERVER mongo_server OPTIONS (database 'mongo_fdw_regress', collection 'warehouse');
CREATE FOREIGN TABLE test_text ( __doc text)
  SERVER mongo_server OPTIONS (database 'mongo_fdw_regress', collection 'warehouse');
CREATE FOREIGN TABLE test_varchar ( __doc varchar)
  SERVER mongo_server OPTIONS (database 'mongo_fdw_regress', collection 'warehouse');

SET datestyle TO ISO;

-- Retrieve data from foreign table using SELECT statement.
SELECT c1, c2, c3, c4, c5, c6, c7, c8 FROM f_test_tbl1
  ORDER BY c1 DESC, c8;
SELECT DISTINCT c8 FROM f_test_tbl1 ORDER BY 1;
SELECT c2 AS "Employee Name" FROM f_test_tbl1 ORDER BY c2 COLLATE "C";
SELECT c8, c6, c7 FROM f_test_tbl1 ORDER BY 1, 2, 3;
SELECT c1, c2, c3, c4, c5, c6, c7, c8 FROM f_test_tbl1
  WHERE c1 = 100 ORDER BY 1;
SELECT c1, c2, c3, c4, c5, c6, c7, c8 FROM f_test_tbl1
  WHERE c1 = 100 OR c1 = 700 ORDER BY 1;
SELECT c1, c2, c3 FROM f_test_tbl1 WHERE c3 like 'SALESMAN' ORDER BY 1;
SELECT c1, c2, c3 FROM f_test_tbl1 WHERE c1 IN (100, 700) ORDER BY 1;
SELECT c1, c2, c3 FROM f_test_tbl1 WHERE c1 NOT IN (100, 700) ORDER BY 1 LIMIT 5;
SELECT c1, c2, c8 FROM f_test_tbl1 WHERE c8 BETWEEN 10 AND 20 ORDER BY 1;
SELECT c1, c2, c6 FROM f_test_tbl1 ORDER BY 1 OFFSET 5;

-- Retrieve data from foreign table using group by clause.
SELECT c8 "Department", COUNT(c1) "Total Employees" FROM f_test_tbl1
  GROUP BY c8 ORDER BY c8;
SELECT c8, SUM(c6) FROM f_test_tbl1
  GROUP BY c8 HAVING c8 IN (10, 30) ORDER BY c8;
SELECT c8, SUM(c6) FROM f_test_tbl1
  GROUP BY c8 HAVING SUM(c6) > 9400 ORDER BY c8;

-- Retrieve data from foreign table using sub-queries.
SELECT c1, c2, c6 FROM f_test_tbl1
  WHERE c8 <> ALL (SELECT c1 FROM f_test_tbl2 WHERE c1 IN (10, 30, 40))
  ORDER BY c1;
SELECT c1, c2, c3 FROM f_test_tbl2
  WHERE EXISTS (SELECT 1 FROM f_test_tbl1 WHERE f_test_tbl2.c1 = f_test_tbl1.c8)
  ORDER BY 1, 2;
SELECT c1, c2, c3, c4, c5, c6, c7, c8 FROM f_test_tbl1
  WHERE c8 NOT IN (SELECT c1 FROM f_test_tbl2) ORDER BY c1;

-- Retrieve data from foreign table using UNION operator.
SELECT c1, c2 FROM f_test_tbl2 UNION
SELECT c1, c2 FROM f_test_tbl1 ORDER BY c1;

SELECT c1, c2 FROM f_test_tbl2 UNION ALL
SELECT c1, c2 FROM f_test_tbl1 ORDER BY c1;

-- Retrieve data from foreign table using INTERSECT operator.
SELECT c1, c2 FROM f_test_tbl1 WHERE c1 >= 800 INTERSECT
SELECT c1, c2 FROM f_test_tbl1 WHERE c1 >= 400 ORDER BY c1;

SELECT c1, c2 FROM f_test_tbl1 WHERE c1 >= 800 INTERSECT ALL
SELECT c1, c2 FROM f_test_tbl1 WHERE c1 >= 400 ORDER BY c1;

-- Retrieve data from foreign table using EXCEPT operator.
SELECT c1, c2 FROM f_test_tbl1 EXCEPT
SELECT c1, c2 FROM f_test_tbl1 WHERE c1 > 900 ORDER BY c1;

SELECT c1, c2 FROM f_test_tbl1 EXCEPT ALL
SELECT c1, c2 FROM f_test_tbl1 WHERE c1 > 900 ORDER BY c1;

-- Retrieve data from foreign table using CTE (with clause).
WITH
  with_qry AS (SELECT c1, c2, c3 FROM f_test_tbl2)
SELECT e.c2, e.c6, w.c1, w.c2 FROM f_test_tbl1 e, with_qry w
  WHERE e.c8 = w.c1 ORDER BY e.c8, e.c2 COLLATE "C";

WITH
  test_tbl2_costs AS (SELECT d.c2, SUM(c6) test_tbl2_total FROM f_test_tbl1 e, f_test_tbl2 d
    WHERE e.c8 = d.c1 GROUP BY 1),
  avg_cost AS (SELECT SUM(test_tbl2_total)/COUNT(*) avg FROM test_tbl2_costs)
SELECT * FROM test_tbl2_costs
  WHERE test_tbl2_total > (SELECT avg FROM avg_cost) ORDER BY c2 COLLATE "C";

-- Retrieve data from foreign table using window clause.
SELECT c8, c1, c6, AVG(c6) OVER (PARTITION BY c8) FROM f_test_tbl1
  ORDER BY c8, c1;
SELECT c8, c1, c6, COUNT(c6) OVER (PARTITION BY c8) FROM f_test_tbl1
  WHERE c8 IN (10, 30, 40, 50, 60, 70) ORDER BY c8, c1;
SELECT c8, c1, c6, SUM(c6) OVER (PARTITION BY c8) FROM f_test_tbl1
  ORDER BY c8, c1;

-- Views
CREATE VIEW smpl_vw AS
  SELECT c1, c2, c3, c4, c5, c6, c7, c8 FROM f_test_tbl1 ORDER BY c1;
SELECT * FROM smpl_vw ORDER BY 1;

CREATE VIEW comp_vw (s1, s2, s3, s6, s7, s8, d2) AS
  SELECT s.c1, s.c2, s.c3, s.c6, s.c7, s.c8, d.c2
    FROM f_test_tbl2 d, f_test_tbl1 s WHERE d.c1 = s.c8 AND d.c1 = 10
    ORDER BY s.c1;
SELECT * FROM comp_vw ORDER BY 1;

CREATE TEMPORARY VIEW temp_vw AS
  SELECT c1, c2, c3 FROM f_test_tbl2;
SELECT * FROM temp_vw ORDER BY 1, 2;

CREATE VIEW mul_tbl_view AS
  SELECT d.c1 dc1, d.c2 dc2, e.c1 ec1, e.c2 ec2, e.c6 ec6
    FROM f_test_tbl2 d INNER JOIN f_test_tbl1 e ON d.c1 = e.c8 ORDER BY d.c1;
SELECT * FROM mul_tbl_view ORDER BY 1, 2, 3;

-- Foreign-Foreign table joins

-- CROSS JOIN.
SELECT f_test_tbl2.c2, f_test_tbl1.c2
  FROM f_test_tbl2 CROSS JOIN f_test_tbl1 ORDER BY 1, 2;
-- INNER JOIN.
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d, f_test_tbl1 e WHERE d.c1 = e.c8 ORDER BY 1, 3;
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d INNER JOIN f_test_tbl1 e ON d.c1 = e.c8 ORDER BY 1, 3;
-- OUTER JOINS.
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d LEFT OUTER JOIN f_test_tbl1 e ON d.c1 = e.c8 ORDER BY 1, 3;
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d RIGHT OUTER JOIN f_test_tbl1 e ON d.c1 = e.c8 ORDER BY 1, 3;
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d FULL OUTER JOIN f_test_tbl1 e ON d.c1 = e.c8 ORDER BY 1, 3;

-- Local-Foreign table joins.
CREATE TABLE l_test_tbl1 AS
  SELECT c1, c2, c3, c4, c5, c6, c7, c8 FROM f_test_tbl1;
CREATE TABLE l_test_tbl2 AS
  SELECT c1, c2, c3 FROM f_test_tbl2;

-- CROSS JOIN.
SELECT f_test_tbl2.c2, l_test_tbl1.c2 FROM f_test_tbl2 CROSS JOIN l_test_tbl1 ORDER BY 1, 2;
-- INNER JOIN.
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM l_test_tbl2 d, f_test_tbl1 e WHERE d.c1 = e.c8 ORDER BY 1, 3;
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d INNER JOIN l_test_tbl1 e ON d.c1 = e.c8 ORDER BY 1, 3;
-- OUTER JOINS.
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d LEFT OUTER JOIN l_test_tbl1 e ON d.c1 = e.c8 ORDER BY 1, 3;
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d RIGHT OUTER JOIN l_test_tbl1 e ON d.c1 = e.c8 ORDER BY 1, 3;
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d FULL OUTER JOIN l_test_tbl1 e ON d.c1 = e.c8 ORDER BY 1, 3;

-- Retrieve complex data containing Sub-fields, dates, Arrays
SELECT * FROM countries ORDER BY _id;
SELECT * FROM country_elections ORDER BY _id;
SELECT * FROM main_exports ORDER BY _id;

-- Retrieve complex data containing Json objects (__doc tests)
SELECT json_data.key AS key1, json_data.value AS value1
  FROM test_json, json_each_text(test_json.__doc) AS json_data
  WHERE key NOT IN ('_id') ORDER BY json_data.key COLLATE "C";
SELECT json_data.key AS key1, json_data.value AS value1
  FROM test_jsonb, jsonb_each_text(test_jsonb.__doc) AS json_data
  WHERE key NOT IN ('_id') ORDER BY json_data.key COLLATE "C";
SELECT json_data.key AS key1, json_data.value AS value1
  FROM test_text, json_each_text(test_text.__doc::json) AS json_data
  WHERE key NOT IN ('_id') ORDER BY json_data.key COLLATE "C";
SELECT json_data.key AS key1, json_data.value AS value1
  FROM test_varchar, json_each_text(test_varchar.__doc::json) AS json_data
  WHERE key NOT IN ('_id') ORDER BY json_data.key COLLATE "C";

-- Inserts some values in mongo_test collection.
INSERT INTO f_mongo_test VALUES ('0', 1, 'One');
INSERT INTO f_mongo_test VALUES ('0', 2, 'Two');
INSERT INTO f_mongo_test VALUES ('0', 3, 'Three');
INSERT INTO f_mongo_test VALUES ('0', 4, 'Four');
INSERT INTO f_mongo_test VALUES ('0', 5, 'Five');
INSERT INTO f_mongo_test VALUES ('0', 6, 'Six');
INSERT INTO f_mongo_test VALUES ('0', 7, 'Seven');
INSERT INTO f_mongo_test VALUES ('0', 8, 'Eight');
INSERT INTO f_mongo_test VALUES ('0', 9, 'Nine');
INSERT INTO f_mongo_test VALUES ('0', 10, 'Ten');

-- Retrieve Data From foreign tables in functions.
CREATE OR REPLACE FUNCTION test_param_where() RETURNS void AS $$
DECLARE
  n varchar;
BEGIN
  FOR x IN 1..9 LOOP
    SELECT b INTO n FROM f_mongo_test WHERE a = x;
    RAISE NOTICE 'Found number %', n;
  END LOOP;
  return;
END
$$ LANGUAGE plpgsql;

SELECT test_param_where();

-- Cleanup
DELETE FROM f_mongo_test WHERE a != 0;
DROP TABLE l_test_tbl1;
DROP TABLE l_test_tbl2;
DROP VIEW smpl_vw;
DROP VIEW comp_vw;
DROP VIEW temp_vw;
DROP VIEW mul_tbl_view;
DROP FUNCTION test_param_where();
DROP FOREIGN TABLE f_mongo_test;
DROP FOREIGN TABLE f_test_tbl1;
DROP FOREIGN TABLE f_test_tbl2;
DROP FOREIGN TABLE countries;
DROP FOREIGN TABLE country_elections;
DROP FOREIGN TABLE main_exports;
DROP FOREIGN TABLE test_json;
DROP FOREIGN TABLE test_jsonb;
DROP FOREIGN TABLE test_text;
DROP FOREIGN TABLE test_varchar;
DROP USER MAPPING FOR public SERVER mongo_server;
DROP SERVER mongo_server;
DROP EXTENSION mongo_fdw;
