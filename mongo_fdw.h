/*-------------------------------------------------------------------------
 *
 * mongo_fdw.h
 *
 * Type and function declarations for MongoDB foreign data wrapper.
 *
 * Copyright (c) 2012-2014 Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef MONGO_FDW_H
#define MONGO_FDW_H

#include "bson.h"
#include "mongo.h"

#include "postgres.h"
#include "utils/hsearch.h"
#include "fmgr.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "utils/datetime.h"
#include "nodes/pg_list.h"
#include "nodes/relation.h"
#include "utils/timestamp.h"
#include "access/reloptions.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "nodes/makefuncs.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/plancat.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/memutils.h"



/* Defines for valid option names */
#define OPTION_NAME_ADDRESS "address"
#define OPTION_NAME_PORT "port"
#define OPTION_NAME_DATABASE "database"
#define OPTION_NAME_COLLECTION "collection"

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
static const uint32 ValidOptionCount = 4;
static const MongoValidOption ValidOptionArray[] =
{
	/* foreign server options */
	{ OPTION_NAME_ADDRESS, ForeignServerRelationId },
	{ OPTION_NAME_PORT,  ForeignServerRelationId },

	/* foreign table options */
	{ OPTION_NAME_DATABASE, ForeignTableRelationId },
	{ OPTION_NAME_COLLECTION, ForeignTableRelationId }
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
	char *databaseName;
	char *collectionName;

} MongoFdwOptions;


/*
* MongoFdwExecState keeps foreign data wrapper specific execution state that we
* create and hold onto when executing the query.
*/
/*
* Execution state of a foreign insert/update/delete operation.
*/
typedef struct MongoFdwModifyState
{
	Relation		rel;				/* relcache entry for the foreign table */

	List			*target_attrs;		/* list of target attribute numbers */

	/* info about parameters for prepared statement */
	int				p_nums;				/* number of parameters to transmit */
	FmgrInfo		*p_flinfo;			/* output conversion functions for them */

	struct HTAB 	*columnMappingHash;

	mongo			*mongoConnection;	/* MongoDB connection */
	mongo_cursor	*mongoCursor;		/* MongoDB cursor */
	bson			*queryDocument;		/* Bson Document */

	MongoFdwOptions *mongoFdwOptions;

	/* working memory context */
	MemoryContext temp_cxt;         /* context for per-tuple temporary data */
} MongoFdwModifyState;


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
extern MongoFdwOptions * MongoGetOptions(Oid foreignTableId);
extern void MongoFreeOptions(MongoFdwOptions *mongoFdwOptions);

mongo* GetConnection(char *host, int32 port);
void cleanup_connection(void);
void ReleaseConnection(mongo *conn);

#endif   /* MONGO_FDW_H */
