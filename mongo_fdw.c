/*-------------------------------------------------------------------------
 *
 * mongo_fdw.c
 *
 * Function definitions for MongoDB foreign data wrapper. These functions access
 * data stored in MongoDB through the official C driver.
 *
 * Copyright (c) 2012-2014 Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "mongo_fdw.h"

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
//#include "utils/json.h"
#include "utils/jsonapi.h"

#if PG_VERSION_NUM >= 90300
	#include "access/htup_details.h"
#endif


/* Local functions forward declarations */
static StringInfo OptionNamesString(Oid currentContextId);
static void MongoGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel,
								   Oid foreignTableId);
static void MongoGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel,
								 Oid foreignTableId);
static ForeignScan * MongoGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel,
										 Oid foreignTableId, ForeignPath *bestPath,
										 List *targetList, List *restrictionClauses);
static void MongoExplainForeignScan(ForeignScanState *scanState,
									ExplainState *explainState);
static void MongoBeginForeignScan(ForeignScanState *scanState, int executorFlags);
static TupleTableSlot * MongoIterateForeignScan(ForeignScanState *scanState);
static void MongoEndForeignScan(ForeignScanState *scanState);
static void MongoReScanForeignScan(ForeignScanState *scanState);
static Const * SerializeDocument(bson *document);
static bson * DeserializeDocument(Const *constant);
static double ForeignTableDocumentCount(Oid foreignTableId);
static MongoFdwOptions * MongoGetOptions(Oid foreignTableId);
static char * MongoGetOptionValue(Oid foreignTableId, const char *optionName);
static HTAB * ColumnMappingHash(Oid foreignTableId, List *columnList);
static void FillTupleSlot(const bson *bsonDocument, const char *bsonDocumentKey,
						  HTAB *columnMappingHash, Datum *columnValues,
						  bool *columnNulls);
static bool ColumnTypesCompatible(bson_type bsonType, Oid columnTypeId);
static Datum ColumnValueArray(bson_iterator *bsonIterator, Oid valueTypeId);
static Datum ColumnValue(bson_iterator *bsonIterator, Oid columnTypeId,
						 int32 columnTypeMod);
static void MongoFreeScanState(MongoFdwExecState *executionState);
static bool MongoAnalyzeForeignTable(Relation relation,
									 AcquireSampleRowsFunc *acquireSampleRowsFunc,
									 BlockNumber *totalPageCount);
static int MongoAcquireSampleRows(Relation relation, int errorLevel,
								  HeapTuple *sampleRows, int targetRowCount,
								  double *totalRowCount, double *totalDeadRowCount);

/* the null action object used for pure validation */
static JsonSemAction nullSemAction =
{
	NULL, NULL, NULL, NULL, NULL,
	NULL, NULL, NULL, NULL, NULL
};


/* declarations for dynamic loading */
PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(mongo_fdw_handler);
PG_FUNCTION_INFO_V1(mongo_fdw_validator);


/*
 * mongo_fdw_handler creates and returns a struct with pointers to foreign table
 * callback functions.
 */
Datum
mongo_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdwRoutine = makeNode(FdwRoutine);

	fdwRoutine->GetForeignRelSize = MongoGetForeignRelSize;
	fdwRoutine->GetForeignPaths = MongoGetForeignPaths;
	fdwRoutine->GetForeignPlan = MongoGetForeignPlan;
	fdwRoutine->ExplainForeignScan = MongoExplainForeignScan;
	fdwRoutine->BeginForeignScan = MongoBeginForeignScan;
	fdwRoutine->IterateForeignScan = MongoIterateForeignScan;
	fdwRoutine->ReScanForeignScan = MongoReScanForeignScan;
	fdwRoutine->EndForeignScan = MongoEndForeignScan;
	fdwRoutine->AnalyzeForeignTable = MongoAnalyzeForeignTable;

	PG_RETURN_POINTER(fdwRoutine);
}


/*
 * mongo_fdw_validator validates options given to one of the following commands:
 * foreign data wrapper, server, user mapping, or foreign table. This function
 * errors out if the given option name or its value is considered invalid.
 */
Datum
mongo_fdw_validator(PG_FUNCTION_ARGS)
{
	Datum optionArray = PG_GETARG_DATUM(0);
	Oid optionContextId = PG_GETARG_OID(1);
	List *optionList = untransformRelOptions(optionArray);
	ListCell *optionCell = NULL;

	foreach(optionCell, optionList)
	{
		DefElem *optionDef = (DefElem *) lfirst(optionCell);
		char *optionName = optionDef->defname;
		bool optionValid = false;

		int32 optionIndex = 0;
		for (optionIndex = 0; optionIndex < ValidOptionCount; optionIndex++)
		{
			const MongoValidOption *validOption = &(ValidOptionArray[optionIndex]);

			if ((optionContextId == validOption->optionContextId) &&
				(strncmp(optionName, validOption->optionName, NAMEDATALEN) == 0))
			{
				optionValid = true;
				break;
			}
		}

		/* if invalid option, display an informative error message */
		if (!optionValid)
		{
			StringInfo optionNamesString = OptionNamesString(optionContextId);

			ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
							errmsg("invalid option \"%s\"", optionName),
							errhint("Valid options in this context are: %s",
									optionNamesString->data)));
		}

		/* if port option is given, error out if its value isn't an integer */
		if (strncmp(optionName, OPTION_NAME_PORT, NAMEDATALEN) == 0)
		{
			char *optionValue = defGetString(optionDef);
			int32 portNumber = pg_atoi(optionValue, sizeof(int32), 0);
			(void) portNumber;
		}
	}

	PG_RETURN_VOID();
}


/*
 * OptionNamesString finds all options that are valid for the current context,
 * and concatenates these option names in a comma separated string.
 */
static StringInfo
OptionNamesString(Oid currentContextId)
{
	StringInfo optionNamesString = makeStringInfo();
	bool firstOptionPrinted = false;

	int32 optionIndex = 0;
	for (optionIndex = 0; optionIndex < ValidOptionCount; optionIndex++)
	{
		const MongoValidOption *validOption = &(ValidOptionArray[optionIndex]);

		/* if option belongs to current context, append option name */
		if (currentContextId == validOption->optionContextId)
		{
			if (firstOptionPrinted)
			{
				appendStringInfoString(optionNamesString, ", ");
			}

			appendStringInfoString(optionNamesString, validOption->optionName);
			firstOptionPrinted = true;
		}
	}

	return optionNamesString;
}


/*
 * MongoGetForeignRelSize obtains relation size estimates for mongo foreign table.
 */
static void
MongoGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreignTableId)
{
	double documentCount = ForeignTableDocumentCount(foreignTableId);
	if (documentCount > 0.0)
	{
		/*
		 * We estimate the number of rows returned after restriction qualifiers
		 * are applied. This will be more accurate if analyze is run on this
		 * relation.
		 */
		List *rowClauseList = baserel->baserestrictinfo;
		double rowSelectivity = clauselist_selectivity(root, rowClauseList,
													   0, JOIN_INNER, NULL);

		double outputRowCount = clamp_row_est(documentCount * rowSelectivity);
		baserel->rows = outputRowCount;
	}
	else
	{
		ereport(DEBUG1, (errmsg("could not retrieve document count for collection"),
						 errhint("Falling back to default estimates in planning")));
	}
}


/*
 * MongoGetForeignPaths creates the only scan path used to execute the query.
 * Note that MongoDB may decide to use an underlying index for this scan, but
 * that decision isn't deterministic or visible to us. We therefore create a
 * single table scan path.
 */
static void
MongoGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreignTableId)
{
	double tupleFilterCost = baserel->baserestrictcost.per_tuple;
	double inputRowCount = 0.0;
	double documentSelectivity = 0.0;
	double foreignTableSize = 0;
	int32 documentWidth = 0;
	BlockNumber pageCount = 0;
	double totalDiskAccessCost = 0.0;
	double cpuCostPerDoc = 0.0;
	double cpuCostPerRow = 0.0;
	double totalCpuCost = 0.0;
	double connectionCost = 0.0;
	double documentCount = 0.0;
	List *opExpressionList = NIL;
	Cost startupCost = 0.0;
	Cost totalCost = 0.0;
	Path *foreignPath = NULL;

	documentCount = ForeignTableDocumentCount(foreignTableId);
	if (documentCount > 0.0)
	{
		/*
		 * We estimate the number of rows returned after restriction qualifiers
		 * are applied by MongoDB.
		 */
		opExpressionList = ApplicableOpExpressionList(baserel);
		documentSelectivity = clauselist_selectivity(root, opExpressionList,
													 0, JOIN_INNER, NULL);
		inputRowCount = clamp_row_est(documentCount * documentSelectivity);
		
		/*
		 * We estimate disk costs assuming a sequential scan over the data. This is
		 * an inaccurate assumption as Mongo scatters the data over disk pages, and
		 * may rely on an index to retrieve the data. Still, this should at least
		 * give us a relative cost.
		 */
		documentWidth = get_relation_data_width(foreignTableId, baserel->attr_widths);
		foreignTableSize = documentCount * documentWidth;

		pageCount = (BlockNumber) rint(foreignTableSize / BLCKSZ);
		totalDiskAccessCost = seq_page_cost * pageCount;

		/*
		 * The cost of processing a document returned by Mongo (input row) is 5x the
		 * cost of processing a regular row.
		 */
		cpuCostPerDoc = cpu_tuple_cost;
		cpuCostPerRow = (cpu_tuple_cost * MONGO_TUPLE_COST_MULTIPLIER) + tupleFilterCost;
		totalCpuCost = (cpuCostPerDoc * documentCount) + (cpuCostPerRow * inputRowCount);

		connectionCost = MONGO_CONNECTION_COST_MULTIPLIER * seq_page_cost;
		startupCost = baserel->baserestrictcost.startup + connectionCost;
		totalCost = startupCost + totalDiskAccessCost + totalCpuCost;
	}
	else
	{
		ereport(DEBUG1, (errmsg("could not retrieve document count for collection"),
						 errhint("Falling back to default estimates in planning")));
	}

	/* create a foreign path node */
	foreignPath = (Path *) create_foreignscan_path(root, baserel, baserel->rows,
												   startupCost, totalCost,
												   NIL,	 /* no pathkeys */
												   NULL, /* no outer rel either */
												   NIL); /* no fdw_private data */

    /* add foreign path as the only possible path */
	add_path(baserel, foreignPath);	
}


/*
 * MongoGetForeignPlan creates a foreign scan plan node for scanning the MongoDB
 * collection. Note that MongoDB may decide to use an underlying index for this
 * scan, but that decision isn't deterministic or visible to us.
 */
static ForeignScan *
MongoGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel,	Oid foreignTableId,
					ForeignPath *bestPath, List *targetList, List *restrictionClauses)
{
	Index scanRangeTableIndex = baserel->relid;
	ForeignScan *foreignScan = NULL;
	List *foreignPrivateList = NIL;
	List *opExpressionList = NIL;
	bson *queryDocument = NULL;
	Const *queryBuffer = NULL;
	List *columnList = NIL;

	/*
	 * We push down applicable restriction clauses to MongoDB, but for simplicity
	 * we currently put all the restrictionClauses into the plan node's qual
	 * list for the executor to re-check. So all we have to do here is strip
	 * RestrictInfo nodes from the clauses and ignore pseudoconstants (which
	 * will be handled elsewhere).
	 */
	restrictionClauses = extract_actual_clauses(restrictionClauses, false);

	/*
	 * We construct the query document to have MongoDB filter its rows. We could
	 * also construct a column name document here to retrieve only the needed
	 * columns. However, we found this optimization to degrade performance on
	 * the MongoDB server-side, so we instead filter out columns on our side.
	 */
	opExpressionList = ApplicableOpExpressionList(baserel);
	queryDocument = QueryDocument(foreignTableId, opExpressionList);
	queryBuffer = SerializeDocument(queryDocument);

	/* only clean up the query struct, but not its data */
	bson_dealloc(queryDocument);

	/* we don't need to serialize column list as lists are copiable */
	columnList = ColumnList(baserel);

	/* construct foreign plan with query document and column list */
	foreignPrivateList = list_make2(queryBuffer, columnList);

	/* create the foreign scan node */
	foreignScan =  make_foreignscan(targetList, restrictionClauses,
									scanRangeTableIndex,
									NIL, /* no expressions to evaluate */
									foreignPrivateList);
	return foreignScan;
}


/*
 * MongoExplainForeignScan produces extra output for the Explain command.
 */
static void
MongoExplainForeignScan(ForeignScanState *scanState, ExplainState *explainState)
{
	MongoFdwOptions *mongoFdwOptions = NULL;
	StringInfo namespaceName = NULL;
	Oid foreignTableId = InvalidOid;

	foreignTableId = RelationGetRelid(scanState->ss.ss_currentRelation);
	mongoFdwOptions = MongoGetOptions(foreignTableId);

	/* construct fully qualified collection name */
	namespaceName = makeStringInfo();
	appendStringInfo(namespaceName, "%s.%s", mongoFdwOptions->databaseName,
					 mongoFdwOptions->collectionName);

	ExplainPropertyText("Foreign Namespace", namespaceName->data, explainState);
}


/*
 * MongoBeginForeignScan connects to the MongoDB server, and opens a cursor that
 * uses the database name, collection name, and the remote query to send to the
 * server. The function also creates a hash table that maps referenced column
 * names to column index and type information.
 */
static void
MongoBeginForeignScan(ForeignScanState *scanState, int executorFlags)
{
	mongo *mongoConnection = NULL;
	mongo_cursor *mongoCursor = NULL;
	int32 connectStatus = MONGO_ERROR;
	Oid foreignTableId = InvalidOid;
	List *columnList = NIL;
	HTAB *columnMappingHash = NULL;
	char *addressName = NULL;
	int32 portNumber = 0;
	int32 errorCode = 0;
	StringInfo namespaceName = NULL;
	ForeignScan *foreignScan = NULL;
	List *foreignPrivateList = NIL;
	Const *queryBuffer = NULL;
	bson *queryDocument = NULL;
	MongoFdwOptions *mongoFdwOptions = NULL;
	MongoFdwExecState *executionState = NULL;

	/* if Explain with no Analyze, do nothing */
	if (executorFlags & EXEC_FLAG_EXPLAIN_ONLY)
	{
		return;
	}

	foreignTableId = RelationGetRelid(scanState->ss.ss_currentRelation);
	mongoFdwOptions = MongoGetOptions(foreignTableId);

	/* resolve hostname and port number; and connect to mongo server */
	addressName = mongoFdwOptions->addressName;
	portNumber = mongoFdwOptions->portNumber;

	mongoConnection = mongo_alloc();
	mongo_init(mongoConnection);

	connectStatus = mongo_connect(mongoConnection, addressName, portNumber);
	if (connectStatus != MONGO_OK)
	{
		errorCode = (int32) mongoConnection->err;

		mongo_destroy(mongoConnection);
		mongo_dealloc(mongoConnection);

		ereport(ERROR, (errmsg("could not connect to %s:%d", addressName, portNumber),
						errhint("Mongo driver connection error: %d", errorCode)));
	}

	/* deserialize query document; and create column info hash */
	foreignScan = (ForeignScan *) scanState->ss.ps.plan;
	foreignPrivateList = foreignScan->fdw_private;
	Assert(list_length(foreignPrivateList) == 2);

	queryBuffer = (Const *) linitial(foreignPrivateList);
	queryDocument = DeserializeDocument(queryBuffer);

	columnList = (List *) lsecond(foreignPrivateList);
	columnMappingHash = ColumnMappingHash(foreignTableId, columnList);

	namespaceName = makeStringInfo();
	appendStringInfo(namespaceName, "%s.%s", mongoFdwOptions->databaseName,
					 mongoFdwOptions->collectionName);

	/* create cursor for collection name and set query */
	mongoCursor = mongo_cursor_alloc();
	mongo_cursor_init(mongoCursor, mongoConnection, namespaceName->data);
	mongo_cursor_set_query(mongoCursor, queryDocument);

	/* create and set foreign execution state */
	executionState = (MongoFdwExecState *) palloc0(sizeof(MongoFdwExecState));
	executionState->columnMappingHash = columnMappingHash;
	executionState->mongoConnection = mongoConnection;
	executionState->mongoCursor = mongoCursor;
	executionState->queryDocument = queryDocument;

	scanState->fdw_state = (void *) executionState;
}


/*
 * MongoIterateForeignScan reads the next document from MongoDB, converts it to
 * a PostgreSQL tuple, and stores the converted tuple into the ScanTupleSlot as
 * a virtual tuple.
 */
static TupleTableSlot *
MongoIterateForeignScan(ForeignScanState *scanState)
{
	MongoFdwExecState *executionState = (MongoFdwExecState *) scanState->fdw_state;
	TupleTableSlot *tupleSlot = scanState->ss.ss_ScanTupleSlot;
	mongo_cursor *mongoCursor = executionState->mongoCursor;
	HTAB *columnMappingHash = executionState->columnMappingHash;
	int32 cursorStatus = MONGO_ERROR;

	TupleDesc tupleDescriptor = tupleSlot->tts_tupleDescriptor;
	Datum *columnValues = tupleSlot->tts_values;
	bool *columnNulls = tupleSlot->tts_isnull;
	int32 columnCount = tupleDescriptor->natts;

	/*
	 * We execute the protocol to load a virtual tuple into a slot. We first
	 * call ExecClearTuple, then fill in values / isnull arrays, and last call
	 * ExecStoreVirtualTuple. If we are done fetching documents from Mongo, we
	 * just return an empty slot as required.
	 */
	ExecClearTuple(tupleSlot);

	/* initialize all values for this row to null */
	memset(columnValues, 0, columnCount * sizeof(Datum));
	memset(columnNulls, true, columnCount * sizeof(bool));

	cursorStatus = mongo_cursor_next(mongoCursor);
	if (cursorStatus == MONGO_OK)
	{
		const bson *bsonDocument = mongo_cursor_bson(mongoCursor);
		const char *bsonDocumentKey = NULL; /* top level document */

		FillTupleSlot(bsonDocument, bsonDocumentKey,
					  columnMappingHash, columnValues, columnNulls);

		ExecStoreVirtualTuple(tupleSlot);
	}
	else
	{
		/*
		 * The following is a courtesy check. In practice when Mongo shuts down,
		 * mongo_cursor_next() could possibly crash. This function first frees
		 * cursor->reply, and then references reply in mongo_cursor_destroy().
		 */
		mongo_cursor_error_t errorCode = mongoCursor->err;
		if (errorCode != MONGO_CURSOR_EXHAUSTED)
		{
			MongoFreeScanState(executionState);

			ereport(ERROR, (errmsg("could not iterate over mongo collection"),
							errhint("Mongo driver cursor error code: %d", errorCode)));
		}
	}

	return tupleSlot;
}


/*
 * MongoEndForeignScan finishes scanning the foreign table, closes the cursor
 * and the connection to MongoDB, and reclaims scan related resources.
 */
static void
MongoEndForeignScan(ForeignScanState *scanState)
{
	MongoFdwExecState *executionState = (MongoFdwExecState *) scanState->fdw_state;

	/* if we executed a query, reclaim mongo related resources */
	if (executionState != NULL)
	{
		MongoFreeScanState(executionState);
	}
}


/*
 * MongoReScanForeignScan rescans the foreign table. Note that rescans in Mongo
 * end up being notably more expensive than what the planner expects them to be,
 * since MongoDB cursors don't provide reset/rewind functionality.
 */
static void
MongoReScanForeignScan(ForeignScanState *scanState)
{
	MongoFdwExecState *executionState = (MongoFdwExecState *) scanState->fdw_state;
	mongo *mongoConnection = executionState->mongoConnection;
	MongoFdwOptions *mongoFdwOptions = NULL;
	mongo_cursor *mongoCursor = NULL;
	StringInfo namespaceName = NULL;
	Oid foreignTableId = InvalidOid;

	/* close down the old cursor */
	mongo_cursor_destroy(executionState->mongoCursor);
	mongo_cursor_dealloc(executionState->mongoCursor);

	/* reconstruct full collection name */
	foreignTableId = RelationGetRelid(scanState->ss.ss_currentRelation);
	mongoFdwOptions = MongoGetOptions(foreignTableId);

	namespaceName = makeStringInfo();
	appendStringInfo(namespaceName, "%s.%s", mongoFdwOptions->databaseName,
					 mongoFdwOptions->collectionName);

	/* reconstruct cursor for collection name and set query */
	mongoCursor = mongo_cursor_alloc();
	mongo_cursor_init(mongoCursor, mongoConnection, namespaceName->data);
	mongo_cursor_set_query(mongoCursor, executionState->queryDocument);

	executionState->mongoCursor = mongoCursor;
}


/*
 * SerializeDocument serializes the document's data to a constant, as advised in
 * foreign/fdwapi.h. Note that this function shallow-copies the document's data;
 * and the caller should therefore not free it.
 */
static Const *
SerializeDocument(bson *document)
{
	Const *serializedDocument = NULL;
	Datum documentDatum = 0;

	/*
	 * We access document data and wrap a datum around it. Note that even when
	 * we have an empty document, the document size can't be zero according to
	 * bson apis.
	 */
	const char *documentData = bson_data(document);
	int32 documentSize = bson_buffer_size(document);
	Assert(documentSize != 0);

	documentDatum = CStringGetDatum(documentData);
	serializedDocument = makeConst(CSTRINGOID, -1, InvalidOid, documentSize,
								   documentDatum, false, false);

	return serializedDocument;
}


/*
 * DeserializeDocument deserializes the constant to a bson document. For this,
 * the function creates a document, and explicitly sets the document's data.
 */
static bson *
DeserializeDocument(Const *constant)
{
	bson *document = NULL;
	Datum documentDatum = constant->constvalue;
	char *documentData = DatumGetCString(documentDatum);

	Assert(constant->constlen > 0);
	Assert(constant->constisnull == false);

	document = bson_alloc();
	bson_init_size(document, 0);
	bson_init_finished_data_with_copy(document, documentData);

	return document;
}


/*
 * ForeignTableDocumentCount connects to the MongoDB server, and queries it for
 * the number of documents in the foreign collection. On success, the function
 * returns the document count. On failure, the function returns -1.0.
 */
static double
ForeignTableDocumentCount(Oid foreignTableId)
{
	MongoFdwOptions *options = NULL;
	mongo *mongoConnection = NULL;
	const bson *emptyQuery = NULL;
	int32 status = MONGO_ERROR;
	double documentCount = 0.0;

	/* resolve foreign table options; and connect to mongo server */
	options = MongoGetOptions(foreignTableId);

	mongoConnection = mongo_alloc();
	mongo_init(mongoConnection);

	status = mongo_connect(mongoConnection, options->addressName, options->portNumber);
	if (status == MONGO_OK)
	{
		documentCount = mongo_count(mongoConnection, options->databaseName,
									options->collectionName, emptyQuery);
	}
	else
	{
		documentCount = -1.0;
	}

	mongo_destroy(mongoConnection);
	mongo_dealloc(mongoConnection);

	return documentCount;
}


/*
 * MongoGetOptions returns the option values to be used when connecting to and
 * querying MongoDB. To resolve these values, the function checks the foreign
 * table's options, and if not present, falls back to default values.
 */
static MongoFdwOptions *
MongoGetOptions(Oid foreignTableId)
{
	MongoFdwOptions *mongoFdwOptions = NULL;
	char *addressName = NULL;
	char *portName = NULL;
	int32 portNumber = 0;
	char *databaseName = NULL;
	char *collectionName = NULL;

	addressName = MongoGetOptionValue(foreignTableId, OPTION_NAME_ADDRESS);
	if (addressName == NULL)
	{
		addressName = pstrdup(DEFAULT_IP_ADDRESS);
	}

	portName = MongoGetOptionValue(foreignTableId, OPTION_NAME_PORT);
	if (portName == NULL)
	{
		portNumber = DEFAULT_PORT_NUMBER;
	}
	else
	{
		portNumber = pg_atoi(portName, sizeof(int32), 0);
	}

	databaseName = MongoGetOptionValue(foreignTableId, OPTION_NAME_DATABASE);
	if (databaseName == NULL)
	{
		databaseName = pstrdup(DEFAULT_DATABASE_NAME);
	}

	collectionName = MongoGetOptionValue(foreignTableId, OPTION_NAME_COLLECTION);
	if (collectionName == NULL)
	{
		collectionName = get_rel_name(foreignTableId);
	}

	mongoFdwOptions = (MongoFdwOptions *) palloc0(sizeof(MongoFdwOptions));
	mongoFdwOptions->addressName = addressName;
	mongoFdwOptions->portNumber = portNumber;
	mongoFdwOptions->databaseName = databaseName;
	mongoFdwOptions->collectionName = collectionName;

	return mongoFdwOptions;
}


/*
 * MongoGetOptionValue walks over foreign table and foreign server options, and
 * looks for the option with the given name. If found, the function returns the
 * option's value.
 */
static char *
MongoGetOptionValue(Oid foreignTableId, const char *optionName)
{
	ForeignTable *foreignTable = NULL;
	ForeignServer *foreignServer = NULL;
	List *optionList = NIL;
	ListCell *optionCell = NULL;
	char *optionValue = NULL;

	foreignTable = GetForeignTable(foreignTableId);
	foreignServer = GetForeignServer(foreignTable->serverid);

	optionList = list_concat(optionList, foreignTable->options);
	optionList = list_concat(optionList, foreignServer->options);

	foreach(optionCell, optionList)
	{
		DefElem *optionDef = (DefElem *) lfirst(optionCell);
		char *optionDefName = optionDef->defname;

		if (strncmp(optionDefName, optionName, NAMEDATALEN) == 0)
		{
			optionValue = defGetString(optionDef);
			break;
		}
	}

	return optionValue;
}


/*
 * ColumnMappingHash creates a hash table that maps column names to column index
 * and types. This table helps us quickly translate BSON document key/values to
 * the corresponding PostgreSQL columns.
 */
static HTAB *
ColumnMappingHash(Oid foreignTableId, List *columnList)
{
	ListCell *columnCell = NULL;
	const long hashTableSize = 2048;
	HTAB *columnMappingHash = NULL;

	/* create hash table */
	HASHCTL hashInfo;
	memset(&hashInfo, 0, sizeof(hashInfo));
	hashInfo.keysize = NAMEDATALEN;
	hashInfo.entrysize = sizeof(ColumnMapping);
	hashInfo.hash = string_hash;
	hashInfo.hcxt = CurrentMemoryContext;

	columnMappingHash = hash_create("Column Mapping Hash", hashTableSize, &hashInfo,
									(HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT));
	Assert(columnMappingHash != NULL);

	foreach(columnCell, columnList)
	{
		Var *column = (Var *) lfirst(columnCell);
		AttrNumber columnId = column->varattno;

		ColumnMapping *columnMapping = NULL;
		char *columnName = NULL;
		bool handleFound = false;
		void *hashKey = NULL;

		columnName = get_relid_attribute_name(foreignTableId, columnId);
		hashKey = (void *) columnName;

		columnMapping = (ColumnMapping *) hash_search(columnMappingHash, hashKey,
													  HASH_ENTER, &handleFound);
		Assert(columnMapping != NULL);

		columnMapping->columnIndex = columnId - 1;
		columnMapping->columnTypeId = column->vartype;
		columnMapping->columnTypeMod = column->vartypmod;
		columnMapping->columnArrayTypeId = get_element_type(column->vartype);
	}

	return columnMappingHash;
}


/*
 * FillTupleSlot walks over all key/value pairs in the given document. For each
 * pair, the function checks if the key appears in the column mapping hash, and
 * if the value type is compatible with the one specified for the column. If so,
 * the function converts the value and fills the corresponding tuple position.
 * The bsonDocumentKey parameter is used for recursion, and should always be 
 * passed as NULL.
 */
static void
FillTupleSlot(const bson *bsonDocument, const char *bsonDocumentKey,
			  HTAB *columnMappingHash, Datum *columnValues, bool *columnNulls)
{
	bson_iterator bsonIterator = { NULL, 0 };
	bson_iterator_init(&bsonIterator, bsonDocument);

	while (bson_iterator_next(&bsonIterator))
	{
		const char *bsonKey = bson_iterator_key(&bsonIterator);
		bson_type bsonType = bson_iterator_type(&bsonIterator);

		ColumnMapping *columnMapping = NULL;
		Oid columnTypeId = InvalidOid;
		Oid columnArrayTypeId = InvalidOid;
		bool compatibleTypes = false;
		bool handleFound = false;
		const char *bsonFullKey = NULL;
		void *hashKey = NULL;

		if (bsonDocumentKey != NULL)
		{
			/*
			 * For fields in nested BSON objects, we use fully qualified field
			 * name to check the column mapping.
			 */
			StringInfo bsonFullKeyString = makeStringInfo();
			appendStringInfo(bsonFullKeyString, "%s.%s", bsonDocumentKey, bsonKey);
			bsonFullKey = bsonFullKeyString->data;
		}
		else
		{
			bsonFullKey = bsonKey;
		}

		/* look up the corresponding column for this bson key */
		hashKey = (void *) bsonFullKey;
		columnMapping = (ColumnMapping *) hash_search(columnMappingHash, hashKey,
													  HASH_FIND, &handleFound);
		if (columnMapping != NULL) {
			columnTypeId = columnMapping->columnTypeId;
			columnArrayTypeId = columnMapping->columnArrayTypeId;
		}

		/* recurse into nested objects */
		if (bsonType == BSON_OBJECT && columnTypeId != JSONOID)
		{
			bson subObject;
			bson_iterator_subobject_init(&bsonIterator, &subObject, false);
			FillTupleSlot(&subObject, bsonFullKey,
						  columnMappingHash, columnValues, columnNulls);
			continue;
		}

		/* if no corresponding column or null bson value, continue */
		if (columnMapping == NULL || bsonType == BSON_NULL)
		{
			continue;
		}

		/* check if columns have compatible types */
		if (OidIsValid(columnArrayTypeId) && bsonType == BSON_ARRAY)
		{
			compatibleTypes = true;
		}
		else
		{
			compatibleTypes = ColumnTypesCompatible(bsonType, columnTypeId);
		}

		/* if types are incompatible, leave this column null */
		if (!compatibleTypes)
		{
			continue;
		}

		/* fill in corresponding column value and null flag */
		if (OidIsValid(columnArrayTypeId))
		{
			int32 columnIndex = columnMapping->columnIndex;

			columnValues[columnIndex] = ColumnValueArray(&bsonIterator,
														 columnArrayTypeId);
			columnNulls[columnIndex] = false;
		}
		else
		{
			int32 columnIndex = columnMapping->columnIndex;
			Oid columnTypeMod = columnMapping->columnTypeMod;

			columnValues[columnIndex] = ColumnValue(&bsonIterator,
													columnTypeId, columnTypeMod);
			columnNulls[columnIndex] = false;
		}
	}
}


/*
 * ColumnTypesCompatible checks if the given BSON type can be converted to the
 * given PostgreSQL type. In this check, the function also uses its knowledge of
 * internal conversions applied by BSON APIs.
 */
static bool
ColumnTypesCompatible(bson_type bsonType, Oid columnTypeId)
{
	bool compatibleTypes = false;

	/* we consider the PostgreSQL column type as authoritative */
	switch(columnTypeId)
	{
		case INT2OID: case INT4OID:
		case INT8OID: case FLOAT4OID:
		case FLOAT8OID: case NUMERICOID:
		{
			if (bsonType == BSON_INT || bsonType == BSON_LONG ||
				bsonType == BSON_DOUBLE)
			{
				compatibleTypes = true;
			}
			break;
		}
		case BOOLOID:
		{
			if (bsonType == BSON_INT || bsonType == BSON_LONG ||
				bsonType == BSON_DOUBLE || bsonType == BSON_BOOL)
			{
				compatibleTypes = true;
			}
			break;
		}
		case BPCHAROID:
		case VARCHAROID:
		case TEXTOID:
		{
			if (bsonType == BSON_STRING)
			{
				compatibleTypes = true;
			}
			break;
		}
		case NAMEOID:
		{
			/*
			 * We currently overload the NAMEOID type to represent the BSON
			 * object identifier. We can safely overload this 64-byte data type
			 * since it's reserved for internal use in PostgreSQL.
			 */
			if (bsonType == BSON_OID)
			{
				compatibleTypes = true;
			}
			break;
		}
		case DATEOID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
		{
			if (bsonType == BSON_DATE)
			{
				compatibleTypes = true;
			}
			break;
		}
		case JSONOID:
		{
			if (bsonType == BSON_OBJECT || bsonType == BSON_ARRAY)
			{
				compatibleTypes = true;
			}
			break;
		}
		default:
		{
			/*
			 * We currently error out on other data types. Some types such as
			 * byte arrays are easy to add, but they need testing. Other types
			 * such as money or inet, do not have equivalents in MongoDB.
			 */
			ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
							errmsg("cannot convert bson type to column type"),
							errhint("Column type: %u", (uint32) columnTypeId)));
			break;
		}
	}

	return compatibleTypes;
}


/*
 * ColumnValueArray uses array element type id to read the current array pointed
 * to by the BSON iterator, and converts each array element (with matching type)
 * to the corresponding PostgreSQL datum. Then, the function constructs an array
 * datum from element datums, and returns the array datum.
 */
static Datum
ColumnValueArray(bson_iterator *bsonIterator, Oid valueTypeId)
{
	Datum *columnValueArray = palloc0(INITIAL_ARRAY_CAPACITY * sizeof(Datum));
	uint32 arrayCapacity = INITIAL_ARRAY_CAPACITY;
	uint32 arrayGrowthFactor = 2;
	uint32 arrayIndex = 0;

	ArrayType *columnValueObject = NULL;
	Datum columnValueDatum = 0;
	bool typeByValue = false;
	char typeAlignment = 0;
	int16 typeLength = 0;

	bson_iterator bsonSubIterator = { NULL, 0 };
	bson_iterator_subiterator(bsonIterator, &bsonSubIterator);

	while (bson_iterator_next(&bsonSubIterator))
	{
		bson_type bsonType = bson_iterator_type(&bsonSubIterator);
		bool compatibleTypes = false;

		compatibleTypes = ColumnTypesCompatible(bsonType, valueTypeId);
		if (bsonType == BSON_NULL || !compatibleTypes)
		{
			continue;
		}

		if (arrayIndex >= arrayCapacity)
		{
			arrayCapacity *= arrayGrowthFactor;
			columnValueArray = repalloc(columnValueArray, arrayCapacity * sizeof(Datum));
		}

		/* use default type modifier (0) to convert column value */
		columnValueArray[arrayIndex] = ColumnValue(&bsonSubIterator, valueTypeId, 0);
		arrayIndex++;
	}

	get_typlenbyvalalign(valueTypeId, &typeLength, &typeByValue, &typeAlignment);
	columnValueObject = construct_array(columnValueArray, arrayIndex, valueTypeId,
										typeLength, typeByValue, typeAlignment);

	columnValueDatum = PointerGetDatum(columnValueObject);
	return columnValueDatum;
}


int append(char **dest, char *src, int length) {
	length += strlen(src);
	strcpy(*dest, src);
	*dest = *dest + strlen(src);
	return length;
}


int print_json(char **buffer, int length, const char *data , int depth,
			   bool is_array ) {
	bson_iterator i;
    const char *key;
    int temp;
    bson_timestamp_t ts;
    char oidhex[25];
    bson scope;
    bson_iterator_from_buffer( &i, data );

	char * buff = malloc(4096);

	bool first_elem = true;

    while ( bson_iterator_next( &i ) ) {
		if (!first_elem) {
			length = append(buffer, ",", length);
		}

        bson_type t = bson_iterator_type( &i );
        if ( t == 0 )
            break;
        key = bson_iterator_key( &i );

		/*
        for ( temp=0; temp<=depth; temp++ )
            elog(NOTICE,  "\t" );
        elog(NOTICE,  "%s : %d \t " , key , t );
		*/
		if (!is_array) {
			sprintf(buff, "\"%s\":", key);
			length = append(buffer, buff, length);
			//elog(NOTICE, "%s", buff);
		}

        switch ( t ) {
        case BSON_DOUBLE:
            //elog(NOTICE,  "%f" , bson_iterator_double( &i ) );
            sprintf(buff, "%f", bson_iterator_double( &i ) );
			length = append(buffer, buff, length);
            break;
        case BSON_STRING:
            //elog(NOTICE,  "\"%s\"" , bson_iterator_string( &i ) );
			sprintf(buff, "\"%s\"" , bson_iterator_string( &i ) );
			length = append(buffer, buff, length);
            break;
        case BSON_SYMBOL:
            elog(NOTICE,  "SYMBOL: %s" , bson_iterator_string( &i ) );
            break;
        case BSON_OID:
            bson_oid_to_string( bson_iterator_oid( &i ), oidhex );
            //elog(NOTICE,  "\"%s\"" , oidhex );
			sprintf(buff,   "\"%s\"" , oidhex );
			length = append(buffer, buff, length);
            break;
        case BSON_BOOL:
            elog(NOTICE,  "%s" , bson_iterator_bool( &i ) ? "true" : "false" );
            break;
        case BSON_DATE:
            elog(NOTICE,  "%ld" , ( long int )bson_iterator_date( &i ) );
            break;
        case BSON_BINDATA:
            elog(NOTICE,  "BSON_BINDATA" );
            break;
        case BSON_UNDEFINED:
            elog(NOTICE,  "BSON_UNDEFINED" );
            break;
        case BSON_NULL:
            elog(NOTICE,  "BSON_NULL" );
            break;
        case BSON_REGEX:
            elog(NOTICE,  "BSON_REGEX: %s", bson_iterator_regex( &i ) );
            break;
        case BSON_CODE:
            elog(NOTICE,  "BSON_CODE: %s", bson_iterator_code( &i ) );
            break;
        /*case BSON_CODEWSCOPE:
            elog(NOTICE,  "BSON_CODE_W_SCOPE: %s", bson_iterator_code( &i ) );
            bson_init( &scope );
            bson_iterator_code_scope( &i, &scope );
            elog(NOTICE,  "\n\t SCOPE: " );
            my_bson_print( &scope );
            break;*/
        case BSON_INT:
            //elog(NOTICE,  "%d" , bson_iterator_int( &i ) );
            sprintf(buff,  "%d" , bson_iterator_int( &i ) );
			length = append(buffer, buff, length);
            break;
        case BSON_LONG:
            //elog(NOTICE,  "%lld" , ( uint64_t )bson_iterator_long( &i ) );
            sprintf(buff,  "%lld" , ( uint64_t )bson_iterator_long( &i ) );
			length = append(buffer, buff, length);
            break;
        case BSON_TIMESTAMP:
            ts = bson_iterator_timestamp( &i );
            elog(NOTICE,  "i: %d, t: %d", ts.i, ts.t );
            break;
        case BSON_OBJECT:
            //elog(NOTICE,  "{");
			length = append(buffer, "{", length);
			length = print_json(buffer, length,  bson_iterator_value( &i ) ,
					depth + 1, false );
            //elog(NOTICE,  "}");
			length = append(buffer, "}", length);
            break;
        case BSON_ARRAY:
            //elog(NOTICE,  "[");
			length = append(buffer, "[", length);
			length = print_json(buffer, length, bson_iterator_value( &i ) ,
					depth + 1, true );
            //elog(NOTICE,  "]");
			length = append(buffer, "]", length);
            break;
        default:
            bson_errprintf( "can't print type : %d\n" , t );
        }
		//elog(NOTICE,  "," );
		first_elem = false;
    }
	//free(buff);
	return length;
}


/*
 * ColumnValue uses column type information to read the current value pointed to
 * by the BSON iterator, and converts this value to the corresponding PostgreSQL
 * datum. The function then returns this datum.
 */
static Datum
ColumnValue(bson_iterator *bsonIterator, Oid columnTypeId, int32 columnTypeMod)
{
	Datum columnValue = 0;

	switch(columnTypeId)
	{
		case INT2OID:
		{
			int16 value = (int16) bson_iterator_int(bsonIterator);
			columnValue = Int16GetDatum(value);
			break;
		}
		case INT4OID:
		{
			int32 value = bson_iterator_int(bsonIterator);
			columnValue = Int32GetDatum(value);
			break;
		}
		case INT8OID:
		{
			int64 value = bson_iterator_long(bsonIterator);
			columnValue = Int64GetDatum(value);
			break;
		}
		case FLOAT4OID:
		{
			float4 value = (float4) bson_iterator_double(bsonIterator);
			columnValue = Float4GetDatum(value);
			break;
		}
		case FLOAT8OID:
		{
			float8 value = bson_iterator_double(bsonIterator);
			columnValue = Float8GetDatum(value);
			break;
		}
		case NUMERICOID:
		{
			float8 value = bson_iterator_double(bsonIterator);
			Datum valueDatum = Float8GetDatum(value);

			/* overlook type modifiers for numeric */
			columnValue = DirectFunctionCall1(float8_numeric, valueDatum);
			break;
		}
		case BOOLOID:
		{
			bool value = bson_iterator_bool(bsonIterator);
			columnValue = BoolGetDatum(value);
			break;
		}
		case BPCHAROID:
		{
			const char *value = bson_iterator_string(bsonIterator);
			Datum valueDatum = CStringGetDatum(value);

			columnValue = DirectFunctionCall3(bpcharin, valueDatum,
											  ObjectIdGetDatum(InvalidOid),
											  Int32GetDatum(columnTypeMod));
			break;
		}
		case VARCHAROID:
		{
			const char *value = bson_iterator_string(bsonIterator);
			Datum valueDatum = CStringGetDatum(value);

			columnValue = DirectFunctionCall3(varcharin, valueDatum,
											  ObjectIdGetDatum(InvalidOid),
											  Int32GetDatum(columnTypeMod));
			break;
		}
		case TEXTOID:
		{
			const char *value = bson_iterator_string(bsonIterator);
			columnValue = CStringGetTextDatum(value);
			break;
		}
		case NAMEOID:
		{
			char value[NAMEDATALEN];
			Datum valueDatum = 0;

			bson_oid_t *bsonObjectId = bson_iterator_oid(bsonIterator);
			bson_oid_to_string(bsonObjectId, value);

			valueDatum = CStringGetDatum(value);
			columnValue = DirectFunctionCall3(namein, valueDatum,
											  ObjectIdGetDatum(InvalidOid),
											  Int32GetDatum(columnTypeMod));
			break;
		}
		case DATEOID:
		{
			int64 valueMillis = bson_iterator_date(bsonIterator);
			int64 timestamp = (valueMillis * 1000L) - POSTGRES_TO_UNIX_EPOCH_USECS;
			Datum timestampDatum = TimestampGetDatum(timestamp);

			columnValue = DirectFunctionCall1(timestamp_date, timestampDatum);
			break;
		}
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
		{
			int64 valueMillis = bson_iterator_date(bsonIterator);
			int64 timestamp = (valueMillis * 1000L) - POSTGRES_TO_UNIX_EPOCH_USECS;

			/* overlook type modifiers for timestamp */
			columnValue = TimestampGetDatum(timestamp);
			break;
		}
		case JSONOID:
		{
			char *buffer = malloc(8184);
			char *bptr = buffer + 1;
			char start_symbol, end_symbol;
			bool is_array;

			bson_type type = bson_iterator_type(bsonIterator);

			if (type == BSON_ARRAY) {
				start_symbol = '[';
				end_symbol = ']';
				is_array = true;
			} else if (type == BSON_OBJECT) {
				start_symbol = '{';
				end_symbol = '}';
				is_array = false;
			} else {
				ereport(ERROR, (errmsg("cannot convert scolar to json")));
			}

			buffer[0] = start_symbol;

			int length = print_json(&bptr, 1,
					bson_iterator_value(bsonIterator), 0, is_array);
			buffer[length] = end_symbol;
			buffer[length + 1] = '\0';

			text	   *result = cstring_to_text(buffer);
			JsonLexContext *lex;

			/* validate it */
			lex = makeJsonLexContext(result, false);
			pg_parse_json(lex, &nullSemAction);
			columnValue = PointerGetDatum(result);
			//free(buffer);
			break;
		}
		default:
		{
			ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
							errmsg("cannot convert bson type to column type"),
							errhint("Column type: %u", (uint32) columnTypeId)));
			break;
		}
	}

	return columnValue;
}


/*
 * MongoFreeScanState closes the cursor and connection to MongoDB, and reclaims
 * all Mongo related resources allocated for the foreign scan.
 */
static void
MongoFreeScanState(MongoFdwExecState *executionState)
{
	if (executionState == NULL)
	{
		return;
	}

	bson_destroy(executionState->queryDocument);
	bson_dealloc(executionState->queryDocument);

	mongo_cursor_destroy(executionState->mongoCursor);
	mongo_cursor_dealloc(executionState->mongoCursor);

	/* also close the connection to mongo server */
	mongo_destroy(executionState->mongoConnection);
	mongo_dealloc(executionState->mongoConnection);
}


/*
 * MongoAnalyzeForeignTable collects statistics for the given foreign table.
 */
static bool
MongoAnalyzeForeignTable(Relation relation,
						 AcquireSampleRowsFunc *acquireSampleRowsFunc,
						 BlockNumber *totalPageCount)
{
	BlockNumber pageCount = 0;
	int attributeCount = 0;
	int32 *attributeWidths = NULL;
	Oid foreignTableId = InvalidOid;
	int32 documentWidth = 0;
	double documentCount = 0.0;
	double foreignTableSize = 0;

	foreignTableId = RelationGetRelid(relation);
	documentCount = ForeignTableDocumentCount(foreignTableId);

	if (documentCount > 0.0)
	{
		attributeCount = RelationGetNumberOfAttributes(relation);
		attributeWidths = (int32 *)	palloc0((attributeCount + 1) * sizeof(int32));

		/*
		 * We estimate disk costs assuming a sequential scan over the data. This is
		 * an inaccurate assumption as Mongo scatters the data over disk pages, and
		 * may rely on an index to retrieve the data. Still, this should at least
		 * give us a relative cost.
		 */
		documentWidth = get_relation_data_width(foreignTableId, attributeWidths);
		foreignTableSize = documentCount * documentWidth;

		pageCount = (BlockNumber) rint(foreignTableSize / BLCKSZ);
	}
	else
	{
		ereport(ERROR, (errmsg("could not retrieve document count for collection"),
						 errhint("could not	collect statistics about foreign table")));
	}

	(*totalPageCount) = pageCount;
	(*acquireSampleRowsFunc) = MongoAcquireSampleRows;

	return true;
}


/*
 * MongoAcquireSampleRows acquires a random sample of rows from the foreign
 * table. Selected rows are returned in the caller allocated sampleRows array,
 * which must have at least target row count entries. The actual number of rows
 * selected is returned as the function result. We also count the number of rows
 * in the collection and return it in total row count. We also always set dead
 * row count to zero.
 *
 * Note that the returned list of rows is not always in order by physical
 * position in the MongoDB collection. Therefore, correlation estimates
 * derived later may be meaningless, but it's OK because we don't use the
 * estimates currently (the planner only pays attention to correlation for
 * index scans).
 */
static int
MongoAcquireSampleRows(Relation relation, int errorLevel,
					   HeapTuple *sampleRows, int targetRowCount,
					   double *totalRowCount, double *totalDeadRowCount)
{
	int sampleRowCount = 0;
	double rowCount = 0;
	double rowCountToSkip = -1; /* -1 means not set yet */
	double randomState = 0;
	Datum *columnValues = NULL;
	bool *columnNulls = NULL;
	Oid foreignTableId = InvalidOid;
	TupleDesc tupleDescriptor = NULL;
	Form_pg_attribute *attributesPtr = NULL;
	AttrNumber columnCount = 0;
	AttrNumber columnId = 0;
	HTAB *columnMappingHash = NULL;
	mongo_cursor *mongoCursor = NULL;
	bson *queryDocument = NULL;
	Const *queryBuffer = NULL;
	List *columnList = NIL;
	ForeignScanState *scanState = NULL;
	List *foreignPrivateList = NIL;
	ForeignScan *foreignScan = NULL;
	MongoFdwExecState *executionState = NULL;
	char *relationName = NULL;
	int executorFlags = 0;
	MemoryContext oldContext = CurrentMemoryContext;
	MemoryContext tupleContext = NULL;

	/* create list of columns in the relation */
	tupleDescriptor = RelationGetDescr(relation);
	columnCount = tupleDescriptor->natts;
	attributesPtr = tupleDescriptor->attrs;

	for (columnId = 1; columnId <= columnCount; columnId++)
	{
		Var *column = (Var *) palloc0(sizeof(Var));

		/* only assign required fields for column mapping hash */
		column->varattno = columnId;
		column->vartype = attributesPtr[columnId-1]->atttypid;
		column->vartypmod = attributesPtr[columnId-1]->atttypmod;

		columnList = lappend(columnList, column);
	}

	/* create state structure */
	scanState = makeNode(ForeignScanState);
	scanState->ss.ss_currentRelation = relation;

	foreignTableId = RelationGetRelid(relation);
	queryDocument = QueryDocument(foreignTableId, NIL);
	queryBuffer = SerializeDocument(queryDocument);

	/* only clean up the query struct, but not its data */
	bson_dealloc(queryDocument);

	/* construct foreign plan with query document and column list */
	foreignPrivateList = list_make2(queryBuffer, columnList);

	foreignScan = makeNode(ForeignScan);
	foreignScan->fdw_private = foreignPrivateList;

	scanState->ss.ps.plan = (Plan *) foreignScan;

	MongoBeginForeignScan(scanState, executorFlags);

	executionState = (MongoFdwExecState *) scanState->fdw_state;
	mongoCursor = executionState->mongoCursor;
	columnMappingHash = executionState->columnMappingHash;

	/*
	 * Use per-tuple memory context to prevent leak of memory used to read
	 * rows from the file with copy routines.
	 */
	tupleContext = AllocSetContextCreate(CurrentMemoryContext,
										 "mongo_fdw temporary context",
										 ALLOCSET_DEFAULT_MINSIZE,
										 ALLOCSET_DEFAULT_INITSIZE,
										 ALLOCSET_DEFAULT_MAXSIZE);

	/* prepare for sampling rows */
	randomState = anl_init_selection_state(targetRowCount);

	columnValues = (Datum *) palloc0(columnCount * sizeof(Datum));
	columnNulls = (bool *) palloc0(columnCount * sizeof(bool));	

	for (;;)
	{
		int32 cursorStatus = MONGO_ERROR;

		/* check for user-requested abort or sleep */
		vacuum_delay_point();

		/* initialize all values for this row to null */
		memset(columnValues, 0, columnCount * sizeof(Datum));
		memset(columnNulls, true, columnCount * sizeof(bool));

		cursorStatus = mongo_cursor_next(mongoCursor);
		if (cursorStatus == MONGO_OK)
		{
			const bson *bsonDocument = mongo_cursor_bson(mongoCursor);
			const char *bsonDocumentKey = NULL; /* top level document */

			/* fetch next tuple */
			MemoryContextReset(tupleContext);
			MemoryContextSwitchTo(tupleContext);

			FillTupleSlot(bsonDocument, bsonDocumentKey,
						  columnMappingHash, columnValues, columnNulls);

			MemoryContextSwitchTo(oldContext);
		}
		else
		{
			/*
			 * The following is a courtesy check. In practice when Mongo shuts down,
			 * mongo_cursor_next() could possibly crash.
			 */
			mongo_cursor_error_t errorCode = mongoCursor->err;
			if (errorCode != MONGO_CURSOR_EXHAUSTED)
			{
				MongoFreeScanState(executionState);
				ereport(ERROR, (errmsg("could not iterate over mongo collection"),
								errhint("Mongo driver cursor error code: %d",
										errorCode)));
			}

			break;
		}

		/*
		 * The first targetRowCount sample rows are simply copied into the
		 * reservoir. Then we start replacing tuples in the sample until we
		 * reach the end of the relation. This algorithm is from Jeff Vitter's
		 * paper (see more info in commands/analyze.c).
		 */
		if (sampleRowCount < targetRowCount)
		{
			sampleRows[sampleRowCount++] = heap_form_tuple(tupleDescriptor,
														   columnValues,
														   columnNulls);
		}
		else
		{
			/*
			 * t in Vitter's paper is the number of records already processed.
			 * If we need to compute a new S value, we must use the "not yet
			 * incremented" value of rowCount as t.
			 */
			if (rowCountToSkip < 0)
			{
				rowCountToSkip = anl_get_next_S(rowCount, targetRowCount,
												&randomState);
			}

			if (rowCountToSkip <= 0)
			{
				/*
				 * Found a suitable tuple, so save it, replacing one old tuple
				 * at random.
				 */
				int rowIndex = (int) (targetRowCount * anl_random_fract());
				Assert(rowIndex >= 0);
				Assert(rowIndex < targetRowCount);

				heap_freetuple(sampleRows[rowIndex]);
				sampleRows[rowIndex] = heap_form_tuple(tupleDescriptor,
													   columnValues,
													   columnNulls);
			}

			rowCountToSkip -= 1;
		}

		rowCount += 1;
	}

	/* clean up */
	MemoryContextDelete(tupleContext);
	MongoFreeScanState(executionState);
	
	pfree(columnValues);
	pfree(columnNulls);

	/* emit some interesting relation info */
	relationName = RelationGetRelationName(relation);
	ereport(errorLevel, (errmsg("\"%s\": collection contains %.0f rows; %d rows	in sample",
								relationName, rowCount, sampleRowCount)));

	(*totalRowCount) = rowCount;
	(*totalDeadRowCount) = 0;

	return sampleRowCount;
}
