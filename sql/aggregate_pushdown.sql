\set MONGO_HOST			`echo \'"$MONGO_HOST"\'`
\set MONGO_PORT			`echo \'"$MONGO_PORT"\'`
\set MONGO_USER_NAME	`echo \'"$MONGO_USER_NAME"\'`
\set MONGO_PASS			`echo \'"$MONGO_PWD"\'`

-- Before running this file User must create database mongo_fdw_regress on
-- MongoDB with all permission for 'edb' user with 'edb' password and ran
-- mongodb_init.sh file to load collections.

\c contrib_regression
CREATE EXTENSION IF NOT EXISTS mongo_fdw;
CREATE SERVER mongo_server FOREIGN DATA WRAPPER mongo_fdw
  OPTIONS (address :MONGO_HOST, port :MONGO_PORT);
CREATE USER MAPPING FOR public SERVER mongo_server;

-- Create foreign tables.
CREATE FOREIGN TABLE fdw137_t1 (_id NAME, c1 INTEGER, c2 TEXT, c3 CHAR(9), c4 INTEGER, c5 pg_catalog.Date, c6 DECIMAL, c7 INTEGER, c8 INTEGER)
  SERVER mongo_server OPTIONS (database 'mongo_fdw_regress', collection 'test_tbl1');
CREATE FOREIGN TABLE fdw137_t2 (_id NAME, c1 INTEGER, c2 TEXT, c3 TEXT)
  SERVER mongo_server OPTIONS (database 'mongo_fdw_regress', collection 'test_tbl2');

INSERT INTO fdw137_t1 VALUES (0, 1500, 'EMP15', 'FINANCE', 1300, '2000-12-25', 950.0, 400, 60);
INSERT INTO fdw137_t1 VALUES (0, 1600, 'EMP16', 'ADMIN', 600);
INSERT INTO fdw137_t2 VALUES (0, 50, 'TESTING', 'NASHIK');
INSERT INTO fdw137_t2 VALUES (0);

-- Create local table.
CREATE TABLE fdw137_local AS
  SELECT c1, c2, c3, c4, c5, c6, c7, c8 FROM fdw137_t1;

-- Simple aggregates
EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(*), sum(c1), avg(c1), min(c4), max(c1), sum(c1) * (random() <= 1)::int AS sum2 FROM fdw137_t1 WHERE c4 > 600 GROUP BY c4 ORDER BY 1, 2;
SELECT count(*), sum(c1), avg(c1), min(c4), max(c1), sum(c1) * (random() <= 1)::int AS sum2 FROM fdw137_t1 WHERE c4 > 600 GROUP BY c4 ORDER BY 1, 2;

-- GROUP BY clause HAVING expressions
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c1, sum(c1), count(*) FROM fdw137_t1 GROUP BY c1 HAVING min(c1) > 500 ORDER BY c1;
SELECT c1, sum(c1), count(*) FROM fdw137_t1 GROUP BY c1 HAVING min(c1) > 500 ORDER BY c1;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c8, min(c2) FROM fdw137_t1 WHERE c3 = 'ADMIN' GROUP BY c8 HAVING min(c8) = 20 ORDER BY c8;
SELECT c8, min(c2) FROM fdw137_t1 WHERE c3 = 'ADMIN' GROUP BY c8 HAVING min(c8) = 20 ORDER BY c8;

-- Multi-column GROUP BY clause. Push-down.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c2, sum(c1) FROM fdw137_t1 GROUP BY c1, c2 HAVING min(c1) > 500 ORDER BY 1;
SELECT c2, sum(c1) FROM fdw137_t1 GROUP BY c1, c2 HAVING min(c1) > 500 ORDER BY 1;

-- Aggregation on expression. Don't push-down.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c1, sum(c1+2) FROM fdw137_t1 GROUP BY c1 HAVING min(c1) > 500 ORDER BY c1;
SELECT c1, sum(c1+2) FROM fdw137_t1 GROUP BY c1 HAVING min(c1) > 500 ORDER BY c1;

-- Aggregate with unshippable GROUP BY clause are not pushed
EXPLAIN (VERBOSE, COSTS OFF)
SELECT max(c4) FROM fdw137_t1 GROUP BY c4 * (random() <= 1)::int ORDER BY 1;
SELECT max(c4) FROM fdw137_t1 GROUP BY c4 * (random() <= 1)::int ORDER BY 1;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c1, sum(c1) FROM fdw137_t1 GROUP BY c1 HAVING min(c1 * 3) > 500 ORDER BY c1;
SELECT c1, sum(c1) FROM fdw137_t1 GROUP BY c1 HAVING min(c1 * 3) > 500 ORDER BY c1;


-- Using expressions in HAVING clause. Pushed down.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c3, count(*) FROM fdw137_t1 GROUP BY c3 HAVING abs(max(c8)) = abs(10) ORDER BY 1, 2;
SELECT c3, count(*) FROM fdw137_t1 GROUP BY c3 HAVING abs(max(c8)) = abs(10) ORDER BY 1, 2;

-- Unshippable HAVING clause will be evaluated locally, and other qual in HAVING clause is pushed down
EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(*) FROM (SELECT c3, count(c1) FROM fdw137_t1 GROUP BY c3 HAVING (avg(c1) / avg(c1)) * random() <= 1 and min(c1) > 100) x;
SELECT count(*) FROM (SELECT c3, count(c1) FROM fdw137_t1 GROUP BY c3 HAVING (avg(c1) / avg(c1)) * random() <= 1 and min(c1) > 100) x;

-- Aggregate over join query
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(t1.c8), avg(t2.c1) FROM fdw137_t1 t1 INNER JOIN fdw137_t2 t2 ON (t1.c8 = t2.c1) WHERE t1.c8 > 10 ORDER BY 1;
SELECT sum(t1.c8), avg(t2.c1) FROM fdw137_t1 t1 INNER JOIN fdw137_t2 t2 ON (t1.c8 = t2.c1) WHERE t1.c8 > 10 ORDER BY 1;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, count(*), t2.c4 FROM fdw137_t2 t1 INNER JOIN fdw137_t1 t2 ON (t1.c1 = t2.c8) GROUP BY t1.c1, t2.c4 ORDER BY 1, 3;
SELECT t1.c1, count(*), t2.c4 FROM fdw137_t2 t1 INNER JOIN fdw137_t1 t2 ON (t1.c1 = t2.c8) GROUP BY t1.c1, t2.c4 ORDER BY 1, 3;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(t2.c1), t1.c8, avg(t1.c8) FROM fdw137_t1 t1 LEFT JOIN fdw137_t2 t2 ON (t1.c8 = t2.c1) WHERE t1.c8 > 10 GROUP BY t1.c8 HAVING avg(t1.c8)*1 > 10 ORDER BY 2;
SELECT sum(t2.c1), t1.c8, avg(t1.c8) FROM fdw137_t1 t1 LEFT JOIN fdw137_t2 t2 ON (t1.c8 = t2.c1) WHERE t1.c8 > 10 GROUP BY t1.c8 HAVING avg(t1.c8)*1 > 10 ORDER BY 2;

-- Aggregate is not pushed down as aggregation contains random()
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(c1 * (random() <= 1)::int) AS sum, avg(c1) FROM fdw137_t1 ORDER BY 1;
SELECT sum(c1 * (random() <= 1)::int) AS sum, avg(c1) FROM fdw137_t1 ORDER BY 1;

-- Not pushed down due to local conditions present in underneath input rel
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(t1.c8) FROM fdw137_t1 t1 INNER JOIN fdw137_t2 t2 ON (t1.c8 = t2.c1) WHERE ((t1.c8 * t2.c1)/(t1.c8 * t2.c1)) * random() <= 1 ORDER BY 1;
SELECT sum(t1.c8) FROM fdw137_t1 t1 INNER JOIN fdw137_t2 t2 ON (t1.c8 = t2.c1) WHERE ((t1.c8 * t2.c1)/(t1.c8 * t2.c1)) * random() <= 1 ORDER BY 1;

-- Aggregates in subquery are pushed down.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(x.a), sum(x.a) FROM (SELECT c8 a, sum(c1) b FROM fdw137_t1 GROUP BY c8 ORDER BY 1, 2) x;
SELECT count(x.a), sum(x.a) FROM (SELECT c8 a, sum(c1) b FROM fdw137_t1 GROUP BY c8 ORDER BY 1, 2) x;

-- Aggregate is still pushed down by taking unshippable expression out
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c4 * (random() <= 1)::int AS sum1, sum(c1) AS sum2 FROM fdw137_t1 GROUP BY c4 ORDER BY 1, 2;
SELECT c4 * (random() <= 1)::int AS sum1, sum(c1) AS sum2 FROM fdw137_t1 GROUP BY c4 ORDER BY 1, 2;

-- Testing ORDER BY, DISTINCT, FILTER and Ordered-sets within aggregates
-- ORDER BY within aggregates (same column used to order) are not pushed
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(c1 ORDER BY c1) FROM fdw137_t1 WHERE c1 < 500 GROUP BY c2 ORDER BY 1;
SELECT sum(c1 ORDER BY c1) FROM fdw137_t1 WHERE c1 < 500 GROUP BY c2 ORDER BY 1;

-- ORDER BY within aggregate (different column used to order also using DESC)
-- are not pushed.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(c8 ORDER BY c1 desc) FROM fdw137_t1 WHERE c1 > 1000 and c8 > 20;
SELECT sum(c8 ORDER BY c1 desc) FROM fdw137_t1 WHERE c1 > 1000 and c8 > 20;

-- DISTINCT within aggregate. Don't push down.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(DISTINCT (c1)) FROM fdw137_t1 WHERE c4 = 600 and c1 < 500;
SELECT sum(DISTINCT (c1)) FROM fdw137_t1 WHERE c4 = 600 and c1 < 500;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(DISTINCT (t1.c1)) FROM fdw137_t1 t1 join fdw137_t2 t2 ON (t1.c8 = t2.c1) WHERE t1.c8 < 30 GROUP BY (t2.c1) ORDER BY 1;
SELECT sum(DISTINCT (t1.c1)) FROM fdw137_t1 t1 join fdw137_t2 t2 ON (t1.c8 = t2.c1) WHERE t1.c8 < 30 GROUP BY (t2.c1) ORDER BY 1;

-- DISTINCT, ORDER BY and FILTER within aggregate, not pushed down.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(c1), sum(DISTINCT c1 ORDER BY c1) filter (WHERE c1%3 < 2), c4 FROM fdw137_t1 WHERE c4 = 600 GROUP BY c4;
SELECT sum(c1), sum(DISTINCT c1 ORDER BY c1) filter (WHERE c1%3 < 2), c4 FROM fdw137_t1 WHERE c4 = 600 GROUP BY c4;

-- FILTER within aggregate, not pushed
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(c1) filter (WHERE c1 < 1000 and c4 > 500) FROM fdw137_t1 GROUP BY c4 ORDER BY 1 nulls last;
SELECT sum(c1) filter (WHERE c1 < 1000 and c4 > 500) FROM fdw137_t1 GROUP BY c4 ORDER BY 1 nulls last;

-- Outer query is aggregation query
EXPLAIN (VERBOSE, COSTS OFF)
SELECT DISTINCT (SELECT count(*) filter (WHERE t2.c1 = 20 and t2.c1 < 30) FROM fdw137_t1 t1 WHERE t1.c1 = 500) FROM fdw137_t2 t2 ORDER BY 1;
SELECT DISTINCT (SELECT count(*) filter (WHERE t2.c1 = 20 and t2.c1 < 30) FROM fdw137_t1 t1 WHERE t1.c1 = 500) FROM fdw137_t2 t2 ORDER BY 1;

-- Inner query is aggregation query
EXPLAIN (VERBOSE, COSTS OFF)
SELECT DISTINCT (SELECT count(t1.c1) filter (WHERE t2.c1 = 20 and t2.c1 < 30) FROM fdw137_t1 t1 WHERE t1.c1 > 600) FROM fdw137_t2 t2 ORDER BY 1;
SELECT DISTINCT (SELECT count(t1.c1) filter (WHERE t2.c1 = 20 and t2.c1 < 30) FROM fdw137_t1 t1 WHERE t1.c1 > 600) FROM fdw137_t2 t2 ORDER BY 1;

-- Ordered-sets within aggregate, not pushed down.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c8, rank('10'::varchar) within group (ORDER BY c3), percentile_cont(c8/200::numeric) within group (ORDER BY c1) FROM fdw137_t1 GROUP BY c8 HAVING percentile_cont(c8/200::numeric) within group (ORDER BY c1) < 500 ORDER BY c8;
SELECT c8, rank('10'::varchar) within group (ORDER BY c3), percentile_cont(c8/200::numeric) within group (ORDER BY c1) FROM fdw137_t1 GROUP BY c8 HAVING percentile_cont(c8/200::numeric) within group (ORDER BY c1) < 500 ORDER BY c8;

-- Subquery in FROM clause HAVING aggregate
EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(*), x.b FROM fdw137_t1, (SELECT c1 a, sum(c1) b FROM fdw137_t2 GROUP BY c1) x WHERE fdw137_t1.c8 = x.a GROUP BY x.b ORDER BY 1, 2;
SELECT count(*), x.b FROM fdw137_t1, (SELECT c1 a, sum(c1) b FROM fdw137_t2 GROUP BY c1) x WHERE fdw137_t1.c8 = x.a GROUP BY x.b ORDER BY 1, 2;

-- Join with IS NULL check in HAVING
EXPLAIN (VERBOSE, COSTS OFF)
SELECT avg(t1.c1), sum(t2.c1) FROM fdw137_t1 t1 join fdw137_t2 t2 ON (t1.c8 = t2.c1) GROUP BY t2.c1 HAVING avg(t1.c1) is null ORDER BY 1 nulls last, 2;
SELECT avg(t1.c1), sum(t2.c1) FROM fdw137_t1 t1 join fdw137_t2 t2 ON (t1.c8 = t2.c1) GROUP BY t2.c1 HAVING avg(t1.c1) is null ORDER BY 1 nulls last, 2;

-- ORDER BY expression is part of the target list but not pushed down to
-- foreign server.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(c1) * (random() <= 1)::int AS sum FROM fdw137_t1 ORDER BY 1;
SELECT sum(c1) * (random() <= 1)::int AS sum FROM fdw137_t1 ORDER BY 1;

-- LATERAL join, with parameterization
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c8, sum FROM fdw137_t1 t1, lateral (SELECT sum(t2.c1) sum FROM fdw137_t2 t2 GROUP BY t2.c1) qry WHERE t1.c8 * 2 = qry.sum ORDER BY 1;

-- Check with placeHolderVars
EXPLAIN (VERBOSE, COSTS OFF)
SELECT q.b, count(fdw137_t1.c1), sum(q.a) FROM fdw137_t1 left join (SELECT min(13), avg(fdw137_t1.c1), sum(fdw137_t2.c1) FROM fdw137_t1 right join fdw137_t2 ON (fdw137_t1.c8 = fdw137_t2.c1) WHERE fdw137_t1.c8 = 20) q(a, b, c) ON (fdw137_t1.c8 = q.b) WHERE fdw137_t1.c1 between 100 and 500 GROUP BY q.b ORDER BY 1 nulls last, 2;
SELECT q.b, count(fdw137_t1.c1), sum(q.a) FROM fdw137_t1 left join (SELECT min(13), avg(fdw137_t1.c1), sum(fdw137_t2.c1) FROM fdw137_t1 right join fdw137_t2 ON (fdw137_t1.c8 = fdw137_t2.c1) WHERE fdw137_t1.c8 = 20) q(a, b, c) ON (fdw137_t1.c8 = q.b) WHERE fdw137_t1.c1 between 100 and 500 GROUP BY q.b ORDER BY 1 nulls last, 2;

-- Not supported cases

-- The COUNT of column
EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(c8) FROM fdw137_t1 ;
SELECT count(c8) FROM fdw137_t1 ;

-- Grouping sets
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c8, sum(c1) FROM fdw137_t1 WHERE c8 > 10 GROUP BY rollup(c8) ORDER BY 1 nulls last;
SELECT c8, sum(c1) FROM fdw137_t1 WHERE c8 > 10 GROUP BY rollup(c8) ORDER BY 1 nulls last;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT c8, sum(c1) FROM fdw137_t1 WHERE c8 > 3 GROUP BY cube(c8) ORDER BY 1 nulls last;
SELECT c8, sum(c1) FROM fdw137_t1 WHERE c8 > 3 GROUP BY cube(c8) ORDER BY 1 nulls last;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT c8, c4, sum(c1) FROM fdw137_t1 WHERE c8 > 20 GROUP BY grouping sets(c8, c4) ORDER BY 1 nulls last, 2 nulls last;
SELECT c8, c4, sum(c1) FROM fdw137_t1 WHERE c8 > 20 GROUP BY grouping sets(c8, c4) ORDER BY 1 nulls last, 2 nulls last;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT c8, sum(c1), grouping(c8) FROM fdw137_t1 WHERE c8 > 10 GROUP BY c8 ORDER BY 1 nulls last;
SELECT c8, sum(c1), grouping(c8) FROM fdw137_t1 WHERE c8 > 10 GROUP BY c8 ORDER BY 1 nulls last;

-- DISTINCT itself is not pushed down, whereas underneath aggregate is pushed
EXPLAIN (VERBOSE, COSTS OFF)
SELECT DISTINCT sum(c1) s FROM fdw137_t1 WHERE c1 > 1000 GROUP BY c1 ORDER BY 1;
SELECT DISTINCT sum(c1) s FROM fdw137_t1 WHERE c1 > 1000 GROUP BY c1 ORDER BY 1;

-- WindowAgg
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c8, sum(c8), count(c8) over (partition by c8%2) FROM fdw137_t1 WHERE c8 > 10 GROUP BY c8 ORDER BY 1;
SELECT c8, sum(c8), count(c8) over (partition by c8%2) FROM fdw137_t1 WHERE c8 > 10 GROUP BY c8 ORDER BY 1;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT c8, array_agg(c8) over (partition by c8%2 ORDER BY c8 desc) FROM fdw137_t1 WHERE c8 > 10 GROUP BY c8 ORDER BY 1;
SELECT c8, array_agg(c8) over (partition by c8%2 ORDER BY c8 desc) FROM fdw137_t1 WHERE c8 > 10 GROUP BY c8 ORDER BY 1;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT c8, array_agg(c8) over (partition by c8%2 ORDER BY c8 range between current row and unbounded following) FROM fdw137_t1 WHERE c8 > 10 GROUP BY c8 ORDER BY 1;
SELECT c8, array_agg(c8) over (partition by c8%2 ORDER BY c8 range between current row and unbounded following) FROM fdw137_t1 WHERE c8 > 10 GROUP BY c8 ORDER BY 1;

-- User defined function for user defined aggregate, VARIADIC
CREATE FUNCTION least_accum(anyelement, variadic anyarray)
returns anyelement language sql AS
  'SELECT least($1, min($2[i])) FROM generate_subscripts($2,2) g(i)';
CREATE aggregate least_agg(variadic items anyarray) (
  stype = anyelement, sfunc = least_accum
);
-- Not pushed down due to user defined aggregate
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c2, least_agg(c1) FROM fdw137_t1 GROUP BY c2 ORDER BY c2;
SELECT c2, least_agg(c1) FROM fdw137_t1 GROUP BY c2 ORDER BY c2;

-- Test partition-wise aggregate
SET enable_partitionwise_aggregate TO ON;

-- Create the partition tables
CREATE TABLE fprt1 (_id NAME, c1 INTEGER, c2 INTEGER, c3 TEXT) PARTITION BY RANGE(c1);
CREATE FOREIGN TABLE ftprt1_p1 PARTITION OF fprt1 FOR VALUES FROM (1) TO (4)
  SERVER mongo_server OPTIONS (database 'mongo_fdw_regress', collection 'test1');
CREATE FOREIGN TABLE ftprt1_p2 PARTITION OF fprt1 FOR VALUES FROM (5) TO (8)
  SERVER mongo_server OPTIONS (database 'mongo_fdw_regress', collection 'test2');

-- Plan with partitionwise aggregates is enabled
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c1, sum(c1) FROM fprt1 GROUP BY c1 ORDER BY 2;
SELECT c1, sum(c1) FROM fprt1 GROUP BY c1 ORDER BY 2;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT c1, sum(c2), min(c2), count(*) FROM fprt1 GROUP BY c1 HAVING avg(c2) < 22 ORDER BY 2;
SELECT c1, sum(c2), min(c2), count(*) FROM fprt1 GROUP BY c1 HAVING avg(c2) < 22 ORDER BY 2;

-- Check with whole-row reference
-- Should have all the columns in the target list for the given relation
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c1, count(t1) FROM fprt1 t1 GROUP BY c1 HAVING avg(c2) < 22 ORDER BY 1;
SELECT c1, count(t1) FROM fprt1 t1 GROUP BY c1 HAVING avg(c2) < 22 ORDER BY 1;

SET enable_partitionwise_aggregate TO OFF;

-- Support enable_aggregate_pushdown option at server level and table level.
-- Check only boolean values are accepted.
ALTER SERVER mongo_server OPTIONS (ADD enable_aggregate_pushdown 'non-bolean');

-- Test the option at server level.
ALTER SERVER mongo_server OPTIONS (ADD enable_aggregate_pushdown 'false');
EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(*) FROM fdw137_t1 GROUP BY c1 HAVING min(c1) > 500 ORDER BY 1;

ALTER SERVER mongo_server OPTIONS (SET enable_aggregate_pushdown 'true');
EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(*) FROM fdw137_t1 GROUP BY c1 HAVING min(c1) > 500 ORDER BY 1;

-- Test the option at table level. Setting option at table level does not
-- affect the setting at server level.
ALTER FOREIGN TABLE fdw137_t1 OPTIONS (ADD enable_aggregate_pushdown 'false');
EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(*) FROM fdw137_t1 GROUP BY c1 HAVING min(c1) > 500 ORDER BY 1;

ALTER SERVER mongo_server OPTIONS (SET enable_aggregate_pushdown 'false');
ALTER FOREIGN TABLE fdw137_t1 OPTIONS (SET enable_aggregate_pushdown 'true');
EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(*) FROM fdw137_t1 GROUP BY c1 HAVING min(c1) > 500 ORDER BY 1;

-- Test option for aggregation over join. Allow aggregation only if enabled for
-- both the relations involved in the join.
ALTER SERVER mongo_server OPTIONS (SET enable_aggregate_pushdown 'true');
ALTER FOREIGN TABLE fdw137_t1 OPTIONS (SET enable_aggregate_pushdown 'false');
ALTER FOREIGN TABLE fdw137_t2 OPTIONS (ADD enable_aggregate_pushdown 'false');
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(t2.c1), t1.c8 FROM fdw137_t1 t1 LEFT JOIN fdw137_t2 t2 ON (t1.c8 = t2.c1) GROUP BY t1.c8 ORDER BY 2;

ALTER SERVER mongo_server OPTIONS (SET enable_aggregate_pushdown 'true');
ALTER FOREIGN TABLE fdw137_t1 OPTIONS (SET enable_aggregate_pushdown 'true');
ALTER FOREIGN TABLE fdw137_t2 OPTIONS (SET enable_aggregate_pushdown 'false');
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(t2.c1), t1.c8 FROM fdw137_t1 t1 LEFT JOIN fdw137_t2 t2 ON (t1.c8 = t2.c1) GROUP BY t1.c8 ORDER BY 2;

ALTER SERVER mongo_server OPTIONS (SET enable_aggregate_pushdown 'false');
ALTER FOREIGN TABLE fdw137_t1 OPTIONS (SET enable_aggregate_pushdown 'true');
ALTER FOREIGN TABLE fdw137_t2 OPTIONS (SET enable_aggregate_pushdown 'true');
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(t2.c1), t1.c8 FROM fdw137_t1 t1 LEFT JOIN fdw137_t2 t2 ON (t1.c8 = t2.c1) GROUP BY t1.c8 ORDER BY 2;

-- Check when enable_join_pushdown is OFF and enable_aggregate_pushdown is ON.
-- Shouldn't push down join as well as aggregation.
ALTER SERVER mongo_server OPTIONS (ADD enable_join_pushdown 'false');
ALTER SERVER mongo_server OPTIONS (SET enable_aggregate_pushdown 'true');
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(t2.c1), t1.c8 FROM fdw137_t1 t1 LEFT JOIN fdw137_t2 t2 ON (t1.c8 = t2.c1) GROUP BY t1.c8 ORDER BY 2;

-- Cleanup
DELETE FROM fdw137_t1 WHERE c8 IS NULL;
DELETE FROM fdw137_t1 WHERE c8 = 60;
DELETE FROM fdw137_t2 WHERE c1 IS NULL;
DELETE FROM fdw137_t2 WHERE c1 = 50;
DROP FOREIGN TABLE fdw137_t1;
DROP FOREIGN TABLE fdw137_t2;
DROP FOREIGN TABLE ftprt1_p1;
DROP FOREIGN TABLE ftprt1_p2;
DROP TABLE fprt1;
DROP USER MAPPING FOR public SERVER mongo_server;
DROP SERVER mongo_server;
DROP EXTENSION mongo_fdw;
