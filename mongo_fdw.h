/*-------------------------------------------------------------------------
 *
 * mongo_fdw.h
 *
 * Type and function declarations for MongoDB foreign data wrapper.
 *
 * Copyright (c) 2012, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#ifndef MONGO_FDW_H
#define MONGO_FDW_H

#include "bson.h"
#include "mongo.h"

#include "fmgr.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "utils/datetime.h"
#include "nodes/pg_list.h"
#include "nodes/relation.h"
#include "utils/timestamp.h"


/* Defines for valid option names */
#define OPTION_NAME_ADDRESS "address"
#define OPTION_NAME_PORT "port"
#define OPTION_NAME_USE_AUTH "use_auth"
#define OPTION_NAME_DATABASE "database"
#define OPTION_NAME_COLLECTION "collection"
#define OPTION_NAME_USERNAME "username"
#define OPTION_NAME_PASSWORD "password"

/* Default values for option parameters */
#define DEFAULT_IP_ADDRESS "127.0.0.1"
#define DEFAULT_PORT_NUMBER 27017
#define DEFAULT_DATABASE_NAME "test"

/* Defines for sending queries and converting types */
#define EQUALITY_OPERATOR_NAME "="
#define INITIAL_ARRAY_CAPACITY 8
#define MONGO_TUPLE_COST_MULTIPLIER 5
#define MONGO_CONNECTION_COST_MULTIPLIER 5
#define POSTGRES_TO_UNIX_EPOCH_DAYS (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE)
#define POSTGRES_TO_UNIX_EPOCH_USECS (POSTGRES_TO_UNIX_EPOCH_DAYS * USECS_PER_DAY)


/*
 * MongoValidOption keeps an option name and a context. When an option is passed
 * into mongo_fdw objects (server and foreign table), we compare this option's
 * name and context against those of valid options.
 */
typedef struct MongoValidOption
{
	const char *optionName;
	Oid optionContextId;

} MongoValidOption;


/* Array of options that are valid for mongo_fdw */
static const uint32 ValidOptionCount = 7;
static const MongoValidOption ValidOptionArray[] =
{
	/* foreign server options */
	{ OPTION_NAME_ADDRESS, ForeignServerRelationId },
	{ OPTION_NAME_PORT,  ForeignServerRelationId },
	{ OPTION_NAME_USE_AUTH, ForeignServerRelationId },

	/* foreign table options */
	{ OPTION_NAME_DATABASE, ForeignTableRelationId },
	{ OPTION_NAME_COLLECTION, ForeignTableRelationId },

	/* user mapping options */
	{ OPTION_NAME_USERNAME, UserMappingRelationId },
	{ OPTION_NAME_PASSWORD, UserMappingRelationId }
};


/*
 * MongoFdwOptions holds the option values to be used when connecting to Mongo.
 * To resolve these values, we first check foreign table's options, and if not
 * present, we then fall back to the default values specified above.
 */
typedef struct MongoFdwOptions
{
	char *addressName;
	int32 portNumber;
	bool useAuth;
	char *databaseName;
	char *collectionName;
	char *username;
	char *password;

} MongoFdwOptions;


/*
 * MongoFdwExecState keeps foreign data wrapper specific execution state that we
 * create and hold onto when executing the query.
 */
typedef struct MongoFdwExecState
{
	struct HTAB *columnMappingHash;
	mongo *mongoConnection;
	mongo_cursor *mongoCursor;
	bson *queryDocument;

} MongoFdwExecState;


/*
 * ColumnMapping reprents a hash table entry that maps a column name to column
 * related information. We construct these hash table entries to speed up the
 * conversion from BSON documents to PostgreSQL tuples; and each hash entry maps
 * the column name to the column's tuple index and its type-related information.
 */
typedef struct ColumnMapping
{
	char columnName[NAMEDATALEN];
	uint32 columnIndex;
	Oid columnTypeId;
	int32 columnTypeMod;
	Oid columnArrayTypeId;

} ColumnMapping;


/* Function declarations related to creating the mongo query */
extern List * ApplicableOpExpressionList(RelOptInfo *baserel);
extern bson * QueryDocument(Oid relationId, List *opExpressionList);
extern List * ColumnList(RelOptInfo *baserel);

/* Function declarations for foreign data wrapper */
extern Datum mongo_fdw_handler(PG_FUNCTION_ARGS);
extern Datum mongo_fdw_validator(PG_FUNCTION_ARGS);


#endif   /* MONGO_FDW_H */
