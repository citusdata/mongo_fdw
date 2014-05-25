CREATE SERVER mongo_server FOREIGN DATA WRAPPER mongo_fdw OPTIONS (address '127.0.0.1', port '27017');

\! mongoimport --db mongo_fdw_regress --collection countries --jsonArray --drop --quiet < test/sql/mongo_fixture.json

--
-- Basic types
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
