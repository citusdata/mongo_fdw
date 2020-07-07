\c postgres postgres
CREATE EXTENSION mongo_fdw;
CREATE SERVER mongo_server FOREIGN DATA WRAPPER mongo_fdw OPTIONS (address '127.0.0.1', port '27017');
\! mongoimport --db mongo_fdw_regress --collection countries --jsonArray --drop --maintainInsertionOrder --host='127.0.0.1' --port=27017 --quiet < data/mongo_fixture.json
CREATE USER MAPPING FOR postgres SERVER mongo_server;

CREATE FOREIGN TABLE department(_id NAME, department_id int, department_name text) SERVER mongo_server OPTIONS(database 'testdb', collection 'department');
CREATE FOREIGN TABLE employee(_id NAME, emp_id int, emp_name text, emp_dept_id int) SERVER mongo_server OPTIONS(database 'testdb', collection 'employee');

INSERT INTO department SELECT 0, i, 'dept - ' || i FROM generate_series(1,10) i;
INSERT INTO employee SELECT 0, i, 'emp - ' || i, (i - 1)%10 + 1 FROM generate_series(1,100) i;

SELECT count(*) FROM department;
SELECT count(*) FROM employee;

EXPLAIN (COSTS FALSE) SELECT emp_id , emp_name , emp_dept_id, department_id , department_name  FROM department d, employee e WHERE d.department_id = e.emp_dept_id ORDER by emp_id;

EXPLAIN (COSTS FALSE) SELECT emp_id , emp_name , emp_dept_id, department_id , department_name FROM department d, employee e WHERE d.department_id IN (SELECT department_id FROM department) ORDER by emp_id;

SELECT emp_id , emp_name , emp_dept_id, department_id , department_name FROM department d, employee e WHERE d.department_id = e.emp_dept_id AND e.emp_dept_id > 5 ORDER by emp_id, department_id;
SELECT emp_id , emp_name , emp_dept_id, department_id , department_name FROM department d, employee e WHERE d.department_id IN (SELECT department_id FROM department WHERE department_id < 3) ORDER by emp_id, department_id;

DELETE FROM employee WHERE emp_id = 10;

UPDATE employee SET emp_name = 'Updated emp' WHERE emp_id = 20;
SELECT emp_id, emp_name FROM employee WHERE emp_name like 'Updated emp' ORDER BY emp_id;

SELECT emp_id , emp_name , emp_dept_id FROM employee ORDER by emp_id LIMIT 10;
SELECT emp_id , emp_name , emp_dept_id FROM employee WHERE emp_id IN (1) ORDER by emp_id;
SELECT emp_id , emp_name , emp_dept_id FROM employee WHERE emp_id IN (1,3,4,5) ORDER by emp_id;
SELECT emp_id , emp_name , emp_dept_id FROM employee WHERE emp_id IN (10000,1000) ORDER by emp_id;

SELECT emp_id , emp_name , emp_dept_id FROM employee WHERE emp_id NOT IN (1)  ORDER by emp_id LIMIT 5;
SELECT emp_id , emp_name , emp_dept_id FROM employee WHERE emp_id NOT IN (1,3,4,5) ORDER by emp_id LIMIT 5;
SELECT emp_id , emp_name , emp_dept_id FROM employee WHERE emp_id NOT IN (10000,1000) ORDER by emp_id LIMIT 5;

SELECT emp_id , emp_name , emp_dept_id FROM employee WHERE emp_id NOT IN (SELECT emp_id FROM employee WHERE emp_id IN (1,10)) ORDER by emp_id;
SELECT emp_id , emp_name , emp_dept_id FROM employee WHERE emp_name NOT IN ('emp - 1', 'emp - 2') ORDER by emp_id LIMIT 5;
SELECT emp_id , emp_name , emp_dept_id FROM employee WHERE emp_name NOT IN ('emp - 10') ORDER by emp_id LIMIT 5;

DELETE FROM employee;
DELETE FROM department;

CREATE FOREIGN TABLE countries (
_id NAME,
name VARCHAR,
population INTEGER,
capital VARCHAR,
hdi FLOAT
) SERVER mongo_server OPTIONS (database 'mongo_fdw_regress', collection 'countries');
SELECT * FROM countries ORDER BY _id;
-- 
-- Subfields and dates
CREATE FOREIGN TABLE country_elections (
_id NAME,
"lastElections.type" VARCHAR,
"lastElections.date" TIMESTAMP
) SERVER mongo_server OPTIONS (database 'mongo_fdw_regress', collection 'countries');
SELECT * FROM country_elections ORDER BY _id;
-- 
-- Arrays
CREATE FOREIGN TABLE main_exports (
_id NAME,
"mainExports" TEXT[]
) SERVER mongo_server OPTIONS (database 'mongo_fdw_regress', collection 'countries');
SELECT * FROM main_exports ORDER BY _id;

-- __doc tests

-- the collection warehouse must contain the following data
-- use testdb;
-- db.warehouse.insert ({"_id" : ObjectId("58a1ebbaf543ec0b90545859"),"warehouse_id" : NumberInt(1),"warehouse_name" : "UPS","warehouse_created" : ISODate("2014-12-12T07:12:10Z")});
-- db.warehouse.insert ({"_id" : ObjectId("58a1ebbaf543ec0b9054585a"),"warehouse_id" : NumberInt(2),"warehouse_name" : "Laptop","warehouse_created" : ISODate("2015-11-11T08:13:10Z")});


CREATE FOREIGN TABLE test_json(__doc json) SERVER mongo_server OPTIONS (database 'testdb', collection 'warehouse');
CREATE FOREIGN TABLE test_jsonb(__doc jsonb) SERVER mongo_server OPTIONS (database 'testdb', collection 'warehouse');
CREATE FOREIGN TABLE test_text(__doc text) SERVER mongo_server OPTIONS (database 'testdb', collection 'warehouse');
CREATE FOREIGN TABLE test_varchar(__doc varchar) SERVER mongo_server OPTIONS (database 'testdb', collection 'warehouse');

SELECT * FROM test_json ORDER BY __doc::text COLLATE "C";
SELECT * FROM test_jsonb ORDER BY __doc::text COLLATE "C";
SELECT * FROM test_text ORDER BY __doc::text COLLATE "C";
SELECT * FROM test_varchar ORDER BY __doc::text COLLATE "C";

-- where clause push down test
CREATE FOREIGN TABLE test_numbers(_id NAME, a int, b text) SERVER mongo_server OPTIONS (database 'testdb', collection 'test_numbers');
insert into test_numbers values('1', 1, 'One');
insert into test_numbers values('2', 2, 'Two');
insert into test_numbers values('3', 3, 'Three');
insert into test_numbers values('4', 4, 'Four');
insert into test_numbers values('5', 5, 'Five');
insert into test_numbers values('6', 6, 'Six');
insert into test_numbers values('7', 7, 'Seven');
insert into test_numbers values('8', 8, 'Eight');
insert into test_numbers values('9', 9, 'Nine');
insert into test_numbers values('10', 10, 'Ten');

create or replace function test_param_where() returns void as $$
DECLARE
  n varchar;
BEGIN
  FOR x IN 1..9 LOOP
    select b into n from test_numbers where a=x;
    raise notice 'Found Item %', n;
  end loop;
  return;
END
$$ LANGUAGE plpgsql;

SELECT test_param_where();

PREPARE test_where_pd(int) as SELECT b FROM test_numbers WHERE a =$1;
explain (verbose, costs false) execute test_where_pd(1);
explain (verbose, costs false) execute test_where_pd(2);
explain (verbose, costs false) execute test_where_pd(3);
explain (verbose, costs false) execute test_where_pd(4);
explain (verbose, costs false) execute test_where_pd(5);
explain (verbose, costs false) execute test_where_pd(6);
explain (verbose, costs false) execute test_where_pd(7);
explain (verbose, costs false) execute test_where_pd(8);
explain (verbose, costs false) execute test_where_pd(9);

execute test_where_pd(1);
execute test_where_pd(2);
execute test_where_pd(3);
execute test_where_pd(4);
execute test_where_pd(5);
execute test_where_pd(6);
execute test_where_pd(7);
execute test_where_pd(8);
execute test_where_pd(9);

--
-- fdw-108: After a change to a pg_foreign_server or pg_user_mapping catalog
-- entry, connection should be invalidated.
--

-- Alter one of the SERVER option
-- Set wrong address for mongo_server
ALTER SERVER mongo_server OPTIONS (SET address '127.0.0.5');
-- Should fail with an error
INSERT INTO test_numbers VALUES ('11', 11, 'Eleven');
UPDATE test_numbers SET a = 11 WHERE a = 10;
DELETE FROM test_numbers WHERE a = 10;
SELECT * FROM test_numbers;
-- Set correct address for mongo_server
ALTER SERVER mongo_server OPTIONS (SET address '127.0.0.1');
-- Should able to insert the data
INSERT INTO test_numbers VALUES ('12', 12, 'Twelve');

-- Change the user mapping options
-- Set wrong username, password for postgres user
DROP USER MAPPING FOR postgres SERVER mongo_server;
CREATE USER MAPPING FOR postgres SERVER mongo_server OPTIONS (username 'wrong', password 'wrong');
-- Should fail with an error
INSERT INTO test_numbers VALUES ('13', 13, 'Thirteen');
UPDATE test_numbers SET a = 11 WHERE a = 10;
DELETE FROM test_numbers WHERE a = 10;
SELECT * FROM test_numbers;
-- Set default username, password for postgres user
DROP USER MAPPING FOR postgres SERVER mongo_server;
CREATE USER MAPPING FOR postgres SERVER mongo_server;
-- Should able to insert the data
INSERT INTO test_numbers VALUES ('14', 14, 'Fourteen');

DELETE FROM test_numbers;
DROP FOREIGN TABLE test_numbers;

DROP FOREIGN TABLE test_json;
DROP FOREIGN TABLE test_jsonb;
DROP FOREIGN TABLE test_text;
DROP FOREIGN TABLE test_varchar;

DROP FOREIGN TABLE department;
DROP FOREIGN TABLE employee;
DROP FOREIGN TABLE countries;
DROP FOREIGN TABLE country_elections;
DROP FOREIGN TABLE main_exports;
DROP USER MAPPING FOR postgres SERVER mongo_server;
DROP EXTENSION mongo_fdw CASCADE;
