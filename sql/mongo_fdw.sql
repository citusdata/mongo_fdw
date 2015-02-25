CREATE SERVER mongo_server FOREIGN DATA WRAPPER mongo_fdw OPTIONS (address '127.0.0.1', port '27017');
\! mongoimport --db mongo_fdw_regress --collection countries --jsonArray --drop --quiet < data/mongo_fixture.json

CREATE USER MAPPING FOR ibrar SERVER mongo_server;

CREATE FOREIGN TABLE department(_id NAME, department_id int, department_name text) SERVER mongo_server OPTIONS(database 'testdb', collection 'department');
CREATE FOREIGN TABLE employee(_id NAME, emp_id int, emp_name text, emp_dept_id int) SERVER mongo_server OPTIONS(database 'testdb', collection 'employee');

INSERT INTO department VALUES(0, generate_series(1,10), 'dept - ' || generate_series(1,10));
INSERT INTO employee VALUES(0, generate_series(1,100), 'emp - ' || generate_series(1,100), generate_series(1,10));

SELECT count(*) FROM department;
SELECT count(*) FROM employee;

EXPLAIN (COSTS FALSE) SELECT emp_id , emp_name , emp_dept_id, department_id , department_name  FROM department d, employee e WHERE d.department_id = e.emp_dept_id ORDER by emp_id;

EXPLAIN (COSTS FALSE) SELECT emp_id , emp_name , emp_dept_id, department_id , department_name FROM department d, employee e WHERE d.department_id IN (SELECT department_id FROM department) ORDER by emp_id;

SELECT emp_id , emp_name , emp_dept_id, department_id , department_name FROM department d, employee e WHERE d.department_id = e.emp_dept_id ORDER by emp_id;
SELECT emp_id , emp_name , emp_dept_id, department_id , department_name FROM department d, employee e WHERE d.department_id IN (SELECT department_id FROM department) ORDER by emp_id;

DELETE FROM employee WHERE emp_id = 10;

UPDATE employee SET emp_name = 'Updated emp' WHERE emp_id = 20;
SELECT emp_id, emp_name FROM employee WHERE emp_name like 'Updated emp';

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
SELECT * FROM countries;
-- 
-- Subfields and dates
CREATE FOREIGN TABLE country_elections (
_id NAME,
"lastElections.type" VARCHAR,
"lastElections.date" TIMESTAMP
) SERVER mongo_server OPTIONS (database 'mongo_fdw_regress', collection 'countries');
SELECT * FROM country_elections;
-- 
-- Arrays
CREATE FOREIGN TABLE main_exports (
_id NAME,
"mainExports" TEXT[]
) SERVER mongo_server OPTIONS (database 'mongo_fdw_regress', collection 'countries');
SELECT * FROM main_exports;

DROP FOREIGN TABLE department;
DROP FOREIGN TABLE employee;
DROP FOREIGN TABLE countries;
DROP FOREIGN TABLE country_elections;
DROP FOREIGN TABLE main_exports;
DROP USER MAPPING FOR postgres SERVER mongo_server;
DROP EXTENSION mongo_fdw CASCADE;
