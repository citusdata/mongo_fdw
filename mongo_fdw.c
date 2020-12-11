/*-------------------------------------------------------------------------
 *
 * mongo_fdw.c
 * 		Foreign-data wrapper for remote MongoDB servers
 *
 * Portions Copyright (c) 2012-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 2004-2020, EnterpriseDB Corporation.
 * Portions Copyright (c) 2012–2014 Citus Data, Inc.
 *
 * IDENTIFICATION
 * 		mongo_fdw.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "mongo_wrapper.h"

#if PG_VERSION_NUM >= 90300
#include "access/htup_details.h"
#endif
#if PG_VERSION_NUM < 120000
#include "access/sysattr.h"
#endif
#if PG_VERSION_NUM >= 120000
#include "access/table.h"
#endif
#include "catalog/pg_type.h"
#if PG_VERSION_NUM >= 130000
#include "common/hashfn.h"
#include "common/jsonapi.h"
#endif
#include "miscadmin.h"
#include "mongo_fdw.h"
#include "mongo_query.h"
#if PG_VERSION_NUM >= 120000
#include "optimizer/optimizer.h"
#endif
#if PG_VERSION_NUM < 120000
#include "optimizer/var.h"
#endif
#include "parser/parsetree.h"
#include "storage/ipc.h"
#include "utils/jsonb.h"
#if PG_VERSION_NUM < 130000
#include "utils/jsonapi.h"
#else
#include "utils/jsonfuncs.h"
#endif
#include "utils/rel.h"

/* Declarations for dynamic loading */
PG_MODULE_MAGIC;

/*
 * In PG 9.5.1 the number will be 90501,
 * our version is 5.2.8 so number will be 50208
 */
#define CODE_VERSION   50208

extern PGDLLEXPORT void _PG_init(void);
const char *EscapeJsonString(const char *string);
void BsonToJsonString(StringInfo output, BSON_ITERATOR iter, bool isArray);

PG_FUNCTION_INFO_V1(mongo_fdw_handler);
PG_FUNCTION_INFO_V1(mongo_fdw_version);

/* FDW callback routines */
static void MongoGetForeignRelSize(PlannerInfo *root,
								   RelOptInfo *baserel,
								   Oid foreigntableid);
static void MongoGetForeignPaths(PlannerInfo *root,
								 RelOptInfo *baserel,
								 Oid foreigntableid);
static ForeignScan *MongoGetForeignPlan(PlannerInfo *root,
										RelOptInfo *foreignrel,
										Oid foreigntableid,
										ForeignPath *best_path,
										List *targetlist,
										List *restrictionClauses,
										Plan *outer_plan);
static void MongoExplainForeignScan(ForeignScanState *node, ExplainState *es);
static void MongoBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *MongoIterateForeignScan(ForeignScanState *node);
static void MongoEndForeignScan(ForeignScanState *node);
static void MongoReScanForeignScan(ForeignScanState *node);
static TupleTableSlot *MongoExecForeignUpdate(EState *estate,
											  ResultRelInfo *resultRelInfo,
											  TupleTableSlot *slot,
											  TupleTableSlot *planSlot);
static TupleTableSlot *MongoExecForeignDelete(EState *estate,
											  ResultRelInfo *resultRelInfo,
											  TupleTableSlot *slot,
											  TupleTableSlot *planSlot);
static void MongoEndForeignModify(EState *estate,
								  ResultRelInfo *resultRelInfo);
static void MongoAddForeignUpdateTargets(Query *parsetree,
										 RangeTblEntry *target_rte,
										 Relation target_relation);
static void MongoBeginForeignModify(ModifyTableState *mtstate,
									ResultRelInfo *resultRelInfo,
									List *fdw_private,
									int subplan_index,
									int eflags);
static TupleTableSlot *MongoExecForeignInsert(EState *estate,
											  ResultRelInfo *resultRelInfo,
											  TupleTableSlot *slot,
											  TupleTableSlot *planSlot);
static List *MongoPlanForeignModify(PlannerInfo *root,
									ModifyTable *plan,
									Index resultRelation,
									int subplan_index);
static void MongoExplainForeignModify(ModifyTableState *mtstate,
									  ResultRelInfo *rinfo,
									  List *fdw_private,
									  int subplan_index,
									  ExplainState *es);
static bool MongoAnalyzeForeignTable(Relation relation,
									 AcquireSampleRowsFunc *func,
									 BlockNumber *totalpages);
#if PG_VERSION_NUM >= 110000
static void MongoBeginForeignInsert(ModifyTableState *mtstate,
									ResultRelInfo *resultRelInfo);
static void MongoEndForeignInsert(EState *estate,
								  ResultRelInfo *resultRelInfo);
#endif

/*
 * Helper functions
 */
static double ForeignTableDocumentCount(Oid foreignTableId);
static HTAB *ColumnMappingHash(Oid foreignTableId, List *columnList);
static void FillTupleSlot(const BSON *bsonDocument,
						  const char *bsonDocumentKey,
						  HTAB *columnMappingHash,
						  Datum *columnValues,
						  bool *columnNulls);
static bool ColumnTypesCompatible(BSON_TYPE bsonType, Oid columnTypeId);
static Datum ColumnValueArray(BSON_ITERATOR *bsonIterator, Oid valueTypeId);
static Datum ColumnValue(BSON_ITERATOR *bsonIterator,
						 Oid columnTypeId,
						 int32 columnTypeMod);
static void MongoFreeScanState(MongoFdwModifyState *fmstate);
static int MongoAcquireSampleRows(Relation relation,
								  int errorLevel,
								  HeapTuple *sampleRows,
								  int targetRowCount,
								  double *totalRowCount,
								  double *totalDeadRowCount);
static void mongo_fdw_exit(int code, Datum arg);

/* The null action object used for pure validation */
#if PG_VERSION_NUM < 130000
static JsonSemAction nullSemAction =
{
	NULL, NULL, NULL, NULL, NULL,
	NULL, NULL, NULL, NULL, NULL
};
#else
JsonSemAction nullSemAction =
{
	NULL, NULL, NULL, NULL, NULL,
	NULL, NULL, NULL, NULL, NULL
};
#endif

/*
 * Library load-time initalization, sets on_proc_exit() callback for
 * backend shutdown.
 */
void
_PG_init(void)
{
#ifdef META_DRIVER
	/* Initialize MongoDB C driver */
	mongoc_init();
#endif

	on_proc_exit(&mongo_fdw_exit, PointerGetDatum(NULL));
}

/*
 * mongo_fdw_handler
 *		Creates and returns a struct with pointers to foreign table callback
 *		functions.
 */
Datum
mongo_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdwRoutine = makeNode(FdwRoutine);

	/* Functions for scanning foreign tables */
	fdwRoutine->GetForeignRelSize = MongoGetForeignRelSize;
	fdwRoutine->GetForeignPaths = MongoGetForeignPaths;
	fdwRoutine->GetForeignPlan = MongoGetForeignPlan;
	fdwRoutine->BeginForeignScan = MongoBeginForeignScan;
	fdwRoutine->IterateForeignScan = MongoIterateForeignScan;
	fdwRoutine->ReScanForeignScan = MongoReScanForeignScan;
	fdwRoutine->EndForeignScan = MongoEndForeignScan;

	/* Support for insert/update/delete */
	fdwRoutine->AddForeignUpdateTargets = MongoAddForeignUpdateTargets;
	fdwRoutine->PlanForeignModify = MongoPlanForeignModify;
	fdwRoutine->BeginForeignModify = MongoBeginForeignModify;
	fdwRoutine->ExecForeignInsert = MongoExecForeignInsert;
	fdwRoutine->ExecForeignUpdate = MongoExecForeignUpdate;
	fdwRoutine->ExecForeignDelete = MongoExecForeignDelete;
	fdwRoutine->EndForeignModify = MongoEndForeignModify;

	/* Support for EXPLAIN */
	fdwRoutine->ExplainForeignScan = MongoExplainForeignScan;
	fdwRoutine->ExplainForeignModify = MongoExplainForeignModify;

	/* Support for ANALYZE */
	fdwRoutine->AnalyzeForeignTable = MongoAnalyzeForeignTable;

#if PG_VERSION_NUM >= 110000
	/* Partition routing and/or COPY from */
	fdwRoutine->BeginForeignInsert = MongoBeginForeignInsert;
	fdwRoutine->EndForeignInsert = MongoEndForeignInsert;
#endif

	PG_RETURN_POINTER(fdwRoutine);
}

/*
 * mongo_fdw_exit
 *		Exit callback function.
 */
static void
mongo_fdw_exit(int code, Datum arg)
{
	mongo_cleanup_connection();
#ifdef META_DRIVER
	/* Release all memory and other resources allocated by the driver */
	mongoc_cleanup();
#endif
}

/*
 * MongoGetForeignRelSize
 * 		Obtains relation size estimates for mongo foreign table.
 */
static void
MongoGetForeignRelSize(PlannerInfo *root,
					   RelOptInfo *baserel,
					   Oid foreigntableid)
{
	double		documentCount = ForeignTableDocumentCount(foreigntableid);

	if (documentCount > 0.0)
	{
		double		rowSelectivity;

		/*
		 * We estimate the number of rows returned after restriction
		 * qualifiers are applied.  This will be more accurate if analyze is
		 * run on this relation.
		 */
		rowSelectivity = clauselist_selectivity(root,
												baserel->baserestrictinfo,
												0, JOIN_INNER, NULL);
		baserel->rows = clamp_row_est(documentCount * rowSelectivity);
	}
	else
		ereport(DEBUG1,
				(errmsg("could not retrieve document count for collection"),
				 errhint("Falling back to default estimates in planning.")));
}

/*
 * MongoGetForeignPaths
 *		Creates the only scan path used to execute the query.
 *
 * Note that MongoDB may decide to use an underlying index for this scan, but
 * that decision isn't deterministic or visible to us.  We therefore create a
 * single table scan path.
 */
static void
MongoGetForeignPaths(PlannerInfo *root,
					 RelOptInfo *baserel,
					 Oid foreigntableid)
{
	double		tupleFilterCost = baserel->baserestrictcost.per_tuple;
	double		inputRowCount;
	double		documentSelectivity;
	double		foreignTableSize;
	int32		documentWidth;
	BlockNumber pageCount;
	double		totalDiskAccessCost;
	double		cpuCostPerDoc;
	double		cpuCostPerRow;
	double		totalCpuCost;
	double		connectionCost;
	double 		documentCount;
	List	   *opExpressionList;
	Cost		startupCost = 0.0;
	Cost		totalCost = 0.0;
	Path	   *foreignPath;

	documentCount = ForeignTableDocumentCount(foreigntableid);

	if (documentCount > 0.0)
	{
		/*
		 * We estimate the number of rows returned after restriction
		 * qualifiers are applied by MongoDB.
		 */
		opExpressionList = ApplicableOpExpressionList(baserel);
		documentSelectivity = clauselist_selectivity(root, opExpressionList,
													 0, JOIN_INNER, NULL);
		inputRowCount = clamp_row_est(documentCount * documentSelectivity);

		/*
		 * We estimate disk costs assuming a sequential scan over the data.
		 * This is an inaccurate assumption as Mongo scatters the data over
		 * disk pages, and may rely on an index to retrieve the data.  Still,
		 * this should at least give us a relative cost.
		 */
		documentWidth = get_relation_data_width(foreigntableid,
												baserel->attr_widths);
		foreignTableSize = documentCount * documentWidth;

		pageCount = (BlockNumber) rint(foreignTableSize / BLCKSZ);
		totalDiskAccessCost = seq_page_cost * pageCount;

		/*
		 * The cost of processing a document returned by Mongo (input row) is
		 * 5x the cost of processing a regular row.
		 */
		cpuCostPerDoc = cpu_tuple_cost;
		cpuCostPerRow = (cpu_tuple_cost * MONGO_TUPLE_COST_MULTIPLIER) + tupleFilterCost;
		totalCpuCost = (cpuCostPerDoc * documentCount) +(cpuCostPerRow * inputRowCount);

		connectionCost = MONGO_CONNECTION_COST_MULTIPLIER * seq_page_cost;
		startupCost = baserel->baserestrictcost.startup + connectionCost;
		totalCost = startupCost + totalDiskAccessCost + totalCpuCost;
	}
	else
		ereport(DEBUG1,
				(errmsg("could not retrieve document count for collection"),
				 errhint("Falling back to default estimates in planning.")));

	/* Create a foreign path node */
	foreignPath = (Path *) create_foreignscan_path(root, baserel,
#if PG_VERSION_NUM >= 90600
												   NULL,	/* default pathtarget */
#endif
												   baserel->rows,
												   startupCost,
												   totalCost,
												   NIL, /* no pathkeys */
												   baserel->lateral_relids,
#if PG_VERSION_NUM >= 90500
												   NULL,	/* no extra plan */
#endif
												   NULL);	/* no fdw_private data */

	/* Add foreign path as the only possible path */
	add_path(baserel, foreignPath);
}

/*
 * MongoGetForeignPlan
 *		Creates a foreign scan plan node for scanning the MongoDB collection.
 *
 * Note that MongoDB may decide to use an underlying index for this
 * scan, but that decision isn't deterministic or visible to us.
 */
static ForeignScan *
MongoGetForeignPlan(PlannerInfo *root,
					RelOptInfo *foreignrel,
					Oid foreigntableid,
					ForeignPath *best_path,
					List *targetList,
					List *restrictionClauses,
					Plan *outer_plan)
{
	Index		scanRangeTableIndex = foreignrel->relid;
	ForeignScan *foreignScan;
	List	   *foreignPrivateList;
	List	   *opExpressionList;
	List	   *columnList;

	/*
	 * We push down applicable restriction clauses to MongoDB, but for
	 * simplicity we currently put all the restrictionClauses into the plan
	 * node's qual list for the executor to re-check.  So all we have to do
	 * here is strip RestrictInfo nodes from the clauses and ignore
	 * pseudoconstants (which will be handled elsewhere).
	 */
	restrictionClauses = extract_actual_clauses(restrictionClauses, false);

	/*
	 * Find the foreign relation's clauses, which can be transformed to
	 * equivalent MongoDB queries.
	 */
	opExpressionList = ApplicableOpExpressionList(foreignrel);

	/* We don't need to serialize column list as lists are copiable */
	columnList = ColumnList(foreignrel);

	/* Construct foreign plan with query document and column list */
	foreignPrivateList = list_make2(columnList, opExpressionList);

	/* Create the foreign scan node */
	foreignScan = make_foreignscan(targetList, restrictionClauses,
								   scanRangeTableIndex,
								   NIL, /* No expressions to evaluate */
								   foreignPrivateList
#if PG_VERSION_NUM >= 90500
								   ,NIL
								   ,NIL
								   ,NULL
#endif
		);

	return foreignScan;
}

/*
 * MongoExplainForeignScan
 *		Produces extra output for the Explain command.
 */
static void
MongoExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
	MongoFdwOptions *options;
	StringInfo	namespaceName;
	Oid			foreignTableId;

	foreignTableId = RelationGetRelid(node->ss.ss_currentRelation);
	options = mongo_get_options(foreignTableId);

	/* Construct fully qualified collection name */
	namespaceName = makeStringInfo();
	appendStringInfo(namespaceName, "%s.%s", options->svr_database,
					 options->collectionName);

	mongo_free_options(options);

	ExplainPropertyText("Foreign Namespace", namespaceName->data, es);
}

static void
MongoExplainForeignModify(ModifyTableState *mtstate,
						  ResultRelInfo *rinfo,
						  List *fdw_private,
						  int subplan_index,
						  ExplainState *es)
{
	MongoFdwOptions *options;
	StringInfo	namespaceName;
	Oid			foreignTableId;

	foreignTableId = RelationGetRelid(rinfo->ri_RelationDesc);
	options = mongo_get_options(foreignTableId);

	/* Construct fully qualified collection name */
	namespaceName = makeStringInfo();
	appendStringInfo(namespaceName, "%s.%s", options->svr_database,
					 options->collectionName);

	mongo_free_options(options);
	ExplainPropertyText("Foreign Namespace", namespaceName->data, es);
}

/*
 * MongoBeginForeignScan
 *		Connects to the MongoDB server, and opens a cursor that uses the
 *		database name, collection name, and the remote query to send to the
 *		server.
 *
 * The function also creates a hash table that maps referenced
 * column names to column index and type information.
 */
static void
MongoBeginForeignScan(ForeignScanState *node, int eflags)
{
	MONGO_CONN *mongoConnection;
	Oid			foreignTableId;
	List	   *columnList;
	HTAB	   *columnMappingHash;
	List	   *foreignPrivateList;
	MongoFdwOptions *options;
	MongoFdwModifyState *fmstate;
	RangeTblEntry *rte;
	EState	   *estate = node->ss.ps.state;
	ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
	Oid			userid;
	ForeignServer *server;
	UserMapping *user;
	ForeignTable *table;

	/* If Explain with no Analyze, do nothing */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	foreignTableId = RelationGetRelid(node->ss.ss_currentRelation);
	options = mongo_get_options(foreignTableId);

	fmstate = (MongoFdwModifyState *) palloc0(sizeof(MongoFdwModifyState));

	/*
	 * Identify which user to do the remote access as.  This should match what
	 * ExecCheckRTEPerms() does.
	 */
	rte = rt_fetch(fsplan->scan.scanrelid, estate->es_range_table);
	userid = rte->checkAsUser ? rte->checkAsUser : GetUserId();

	/* Get info about foreign table. */
	fmstate->rel = node->ss.ss_currentRelation;
	table = GetForeignTable(RelationGetRelid(fmstate->rel));
	server = GetForeignServer(table->serverid);
	user = GetUserMapping(userid, server->serverid);

	/*
	 * Get connection to the foreign server.  Connection manager will establish
	 * new connection if necessary.
	 */
	mongoConnection = mongo_get_connection(server, user, options);

	foreignPrivateList = fsplan->fdw_private;
	Assert(list_length(foreignPrivateList) == 2);

	columnList = list_nth(foreignPrivateList, 0);

	columnMappingHash = ColumnMappingHash(foreignTableId, columnList);

	/* Create and set foreign execution state */
	fmstate->columnMappingHash = columnMappingHash;
	fmstate->mongoConnection = mongoConnection;
	fmstate->options = options;

	node->fdw_state = (void *) fmstate;
}

/*
 * MongoIterateForeignScan
 *		Opens a Mongo cursor that uses the database name, collection name, and
 *		the remote query to send to the server.
 *
 *	Reads the next document from MongoDB, converts it to a PostgreSQL tuple,
 *	and stores the converted tuple into the ScanTupleSlot as a virtual tuple.
 */
static TupleTableSlot *
MongoIterateForeignScan(ForeignScanState *node)
{
	MongoFdwModifyState *fmstate = (MongoFdwModifyState *) node->fdw_state;
	TupleTableSlot *tupleSlot = node->ss.ss_ScanTupleSlot;
	MONGO_CURSOR *mongoCursor = fmstate->mongoCursor;
	HTAB	   *columnMappingHash = fmstate->columnMappingHash;
	ForeignScan *foreignScan = (ForeignScan *) node->ss.ps.plan;
	TupleDesc	tupleDescriptor = tupleSlot->tts_tupleDescriptor;
	Datum	   *columnValues = tupleSlot->tts_values;
	bool	   *columnNulls = tupleSlot->tts_isnull;
	int32		columnCount = tupleDescriptor->natts;

	/* Create cursor for collection name and set query */
	if (mongoCursor == NULL)
	{
		Oid			foreignTableId;
		List	   *foreignPrivateList;
		List	   *opExpressionList;
		BSON	   *queryDocument;

		foreignPrivateList = foreignScan->fdw_private;
		Assert(list_length(foreignPrivateList) == 2);

		opExpressionList = list_nth(foreignPrivateList, 1);
		foreignTableId = RelationGetRelid(node->ss.ss_currentRelation);

		/*
		 * We construct the query document to have MongoDB filter its rows.  We
		 * could also construct a column name document here to retrieve only
		 * the needed columns.  However, we found this optimization to degrade
		 * performance on the MongoDB server-side, so we instead filter out
		 * columns on our side.
		 */
		queryDocument = QueryDocument(foreignTableId, opExpressionList, node);

		mongoCursor = MongoCursorCreate(fmstate->mongoConnection,
										fmstate->options->svr_database,
										fmstate->options->collectionName,
										queryDocument);

		/* Save mongoCursor */
		fmstate->mongoCursor = mongoCursor;
	}

	/*
	 * We execute the protocol to load a virtual tuple into a slot. We first
	 * call ExecClearTuple, then fill in values / isnull arrays, and last call
	 * ExecStoreVirtualTuple.  If we are done fetching documents from Mongo,
	 * we just return an empty slot as required.
	 */
	ExecClearTuple(tupleSlot);

	/* Initialize all values for this row to null */
	memset(columnValues, 0, columnCount * sizeof(Datum));
	memset(columnNulls, true, columnCount * sizeof(bool));

	if (MongoCursorNext(mongoCursor, NULL))
	{
		const BSON *bsonDocument = MongoCursorBson(mongoCursor);
		const char *bsonDocumentKey = NULL; /* Top level document */

		FillTupleSlot(bsonDocument, bsonDocumentKey, columnMappingHash,
					  columnValues, columnNulls);

		ExecStoreVirtualTuple(tupleSlot);
	}

	return tupleSlot;
}

/*
 * MongoEndForeignScan
 *		Finishes scanning the foreign table, closes the cursor and the
 *		connection to MongoDB, and reclaims scan related resources.
 */
static void
MongoEndForeignScan(ForeignScanState *node)
{
	MongoFdwModifyState *fmstate;

	fmstate = (MongoFdwModifyState *) node->fdw_state;
	/* If we executed a query, reclaim mongo related resources */
	if (fmstate != NULL)
	{
		if (fmstate->options)
		{
			mongo_free_options(fmstate->options);
			fmstate->options = NULL;
		}
		MongoFreeScanState(fmstate);
	}
}

/*
 * MongoReScanForeignScan
 *		Rescans the foreign table.
 *
 * Note that rescans in Mongo end up being notably more expensive than what the
 * planner expects them to be, since MongoDB cursors don't provide reset/rewind
 * functionality.
 */
static void
MongoReScanForeignScan(ForeignScanState *node)
{
	MongoFdwModifyState *fmstate = (MongoFdwModifyState *) node->fdw_state;

	/* Close down the old cursor */
	if (fmstate->mongoCursor)
	{
		MongoCursorDestroy(fmstate->mongoCursor);
		fmstate->mongoCursor = NULL;
	}
}

static List *
MongoPlanForeignModify(PlannerInfo *root,
					   ModifyTable *plan,
					   Index resultRelation,
					   int subplan_index)
{
	CmdType		operation = plan->operation;
	RangeTblEntry *rte = planner_rt_fetch(resultRelation, root);
	Relation	rel;
	List	   *targetAttrs = NIL;

	/*
	 * Core code already has some lock on each rel being planned, so we can
	 * use NoLock here.
	 */
#if PG_VERSION_NUM < 130000
	rel = heap_open(rte->relid, NoLock);
#else
	rel = table_open(rte->relid, NoLock);
#endif
	if (operation == CMD_INSERT)
	{
		TupleDesc	tupdesc = RelationGetDescr(rel);
		int			attnum;

		for (attnum = 1; attnum <= tupdesc->natts; attnum++)
		{
#if PG_VERSION_NUM < 110000
			Form_pg_attribute attr = tupdesc->attrs[attnum - 1];
#else
			Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);
#endif

			if (!attr->attisdropped)
				targetAttrs = lappend_int(targetAttrs, attnum);
		}
	}
	else if (operation == CMD_UPDATE)
	{
#if PG_VERSION_NUM >= 90500
		Bitmapset  *tmpset = bms_copy(rte->updatedCols);
#else
		Bitmapset  *tmpset = bms_copy(rte->modifiedCols);
#endif
		AttrNumber	col;

		while ((col = bms_first_member(tmpset)) >= 0)
		{
			col += FirstLowInvalidHeapAttributeNumber;
			if (col <= InvalidAttrNumber)	/* Shouldn't happen */
				elog(ERROR, "system-column update is not supported");

			/*
			 * We also disallow updates to the first column which happens to
			 * be the row identifier in MongoDb (_id)
			 */
			if (col == 1)		/* Shouldn't happen */
				elog(ERROR, "row identifier column update is not supported");

			targetAttrs = lappend_int(targetAttrs, col);
		}
		/* We also want the rowid column to be available for the update */
		targetAttrs = lcons_int(1, targetAttrs);
	}
	else
		targetAttrs = lcons_int(1, targetAttrs);

	/*
	 * RETURNING list not supported
	 */
	if (plan->returningLists)
		elog(ERROR, "RETURNING is not supported by this FDW");

#if PG_VERSION_NUM < 130000
	heap_close(rel, NoLock);
#else
	table_close(rel, NoLock);
#endif

	return list_make1(targetAttrs);
}

/*
 * MongoBeginForeignModify
 *		Begin an insert/update/delete operation on a foreign table.
 */
static void
MongoBeginForeignModify(ModifyTableState *mtstate,
						ResultRelInfo *resultRelInfo,
						List *fdw_private,
						int subplan_index,
						int eflags)
{
	MongoFdwModifyState *fmstate;
	Relation	rel = resultRelInfo->ri_RelationDesc;
	AttrNumber	n_params;
	Oid			typefnoid = InvalidOid;
	bool		isvarlena = false;
	ListCell   *lc;
	Oid			foreignTableId;
	Oid			userid;
	ForeignServer *server;
	UserMapping *user;
	ForeignTable *table;

	/*
	 * Do nothing in EXPLAIN (no ANALYZE) case.  resultRelInfo->ri_FdwState
	 * stays NULL.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	foreignTableId = RelationGetRelid(rel);
	userid = GetUserId();

	/* Get info about foreign table. */
	table = GetForeignTable(foreignTableId);
	server = GetForeignServer(table->serverid);
	user = GetUserMapping(userid, server->serverid);

	/* Begin constructing MongoFdwModifyState. */
	fmstate = (MongoFdwModifyState *) palloc0(sizeof(MongoFdwModifyState));

	fmstate->rel = rel;
	fmstate->options = mongo_get_options(foreignTableId);

	/*
	 * Get connection to the foreign server.  Connection manager will
	 * establish new connection if necessary.
	 */
	fmstate->mongoConnection = mongo_get_connection(server, user,
													fmstate->options);

	fmstate->target_attrs = (List *) list_nth(fdw_private, 0);

	n_params = list_length(fmstate->target_attrs) + 1;
	fmstate->p_flinfo = (FmgrInfo *) palloc(sizeof(FmgrInfo) * n_params);
	fmstate->p_nums = 0;

	/* Set up for remaining transmittable parameters */
	foreach(lc, fmstate->target_attrs)
	{
		int			attnum = lfirst_int(lc);
#if PG_VERSION_NUM < 110000
		Form_pg_attribute attr = RelationGetDescr(rel)->attrs[attnum - 1];
#else
		Form_pg_attribute attr = TupleDescAttr(RelationGetDescr(rel),
											   attnum - 1);
#endif

		Assert(!attr->attisdropped);

		getTypeOutputInfo(attr->atttypid, &typefnoid, &isvarlena);
		fmgr_info(typefnoid, &fmstate->p_flinfo[fmstate->p_nums]);
		fmstate->p_nums++;
	}
	Assert(fmstate->p_nums <= n_params);

	resultRelInfo->ri_FdwState = fmstate;
}

/*
 * MongoExecForeignInsert
 *		Insert one row into a foreign table.
 */
static TupleTableSlot *
MongoExecForeignInsert(EState *estate,
					   ResultRelInfo *resultRelInfo,
					   TupleTableSlot *slot,
					   TupleTableSlot *planSlot)
{
	BSON	   *bsonDoc;
	Oid			typoid;
	Datum		value;
	bool		isnull = false;
	MongoFdwModifyState *fmstate;

	fmstate = (MongoFdwModifyState *) resultRelInfo->ri_FdwState;

	bsonDoc = BsonCreate();

	typoid = get_atttype(RelationGetRelid(resultRelInfo->ri_RelationDesc), 1);

	/* Get following parameters from slot */
	if (slot != NULL && fmstate->target_attrs != NIL)
	{
		ListCell   *lc;

		foreach(lc, fmstate->target_attrs)
		{
			int			attnum = lfirst_int(lc);

			value = slot_getattr(slot, attnum, &isnull);

			/* First column of MongoDB's foreign table must be _id */
#if PG_VERSION_NUM < 110000
			if (strcmp(slot->tts_tupleDescriptor->attrs[0]->attname.data, "_id") != 0)
#else
			if (strcmp(TupleDescAttr(slot->tts_tupleDescriptor, 0)->attname.data, "_id") != 0)
#endif
				elog(ERROR, "first column of MongoDB's foreign table must be \"_id\"");

			if (typoid != NAMEOID)
				elog(ERROR, "type of first column of MongoDB's foreign table must be \"NAME\"");
#if PG_VERSION_NUM < 110000
			if (strcmp(slot->tts_tupleDescriptor->attrs[0]->attname.data, "__doc") == 0)
#else
			if (strcmp(TupleDescAttr(slot->tts_tupleDescriptor, 0)->attname.data, "__doc") == 0)
#endif
				continue;

			/*
			 * Ignore the value of first column which is row identifier in
			 * MongoDb (_id) and let MongoDB to insert the unique value for
			 * that column.
			 */
			if (attnum == 1)
				continue;

#if PG_VERSION_NUM < 110000
			AppendMongoValue(bsonDoc,
							 slot->tts_tupleDescriptor->attrs[attnum - 1]->attname.data,
							 value,
							 isnull,
							 slot->tts_tupleDescriptor->attrs[attnum - 1]->atttypid);
#else
			AppendMongoValue(bsonDoc,
							 TupleDescAttr(slot->tts_tupleDescriptor, attnum - 1)->attname.data,
							 value,
							 isnull,
							 TupleDescAttr(slot->tts_tupleDescriptor, attnum - 1)->atttypid);
#endif
		}
	}
	BsonFinish(bsonDoc);

	/* Now we are ready to insert tuple/document into MongoDB */
	MongoInsert(fmstate->mongoConnection, fmstate->options->svr_database,
				fmstate->options->collectionName, bsonDoc);

	BsonDestroy(bsonDoc);

	return slot;
}

/*
 * MongoAddForeignUpdateTargets
 *		Add column(s) needed for update/delete on a foreign table, we are using
 *		first column as row identification column, so we are adding that into
 *		target list.
 */
static void
MongoAddForeignUpdateTargets(Query *parsetree,
							 RangeTblEntry *target_rte,
							 Relation target_relation)
{
	Var		   *var;
	const char *attrname;
	TargetEntry *tle;

	/*
	 * What we need is the rowid which is the first column
	 */
#if PG_VERSION_NUM < 110000
	Form_pg_attribute attr = RelationGetDescr(target_relation)->attrs[0];
#else
	Form_pg_attribute attr = TupleDescAttr(RelationGetDescr(target_relation),
										   0);
#endif

	/* Make a Var representing the desired value */
	var = makeVar(parsetree->resultRelation,
				  1,
				  attr->atttypid,
				  attr->atttypmod,
				  InvalidOid,
				  0);

	/* Wrap it in a TLE with the right name ... */
	attrname = NameStr(attr->attname);

	tle = makeTargetEntry((Expr *) var,
						  list_length(parsetree->targetList) + 1,
						  pstrdup(attrname),
						  true);

	/* ... And add it to the query's targetlist */
	parsetree->targetList = lappend(parsetree->targetList, tle);
}

static TupleTableSlot *
MongoExecForeignUpdate(EState *estate,
					   ResultRelInfo *resultRelInfo,
					   TupleTableSlot *slot,
					   TupleTableSlot *planSlot)
{
	Datum		datum;
	bool		isNull = false;
	Oid			foreignTableId;
	char	   *columnName;
	Oid			typoid;
	BSON	   *document;
	BSON	   *op = NULL;
	BSON		set;
	MongoFdwModifyState *fmstate;

	fmstate = (MongoFdwModifyState *) resultRelInfo->ri_FdwState;
	foreignTableId = RelationGetRelid(resultRelInfo->ri_RelationDesc);

	/* Get the id that was passed up as a resjunk column */
	datum = ExecGetJunkAttribute(planSlot, 1, &isNull);

#if PG_VERSION_NUM < 110000
	columnName = get_relid_attribute_name(foreignTableId, 1);
#else
	columnName = get_attname(foreignTableId, 1, false);
#endif

	typoid = get_atttype(foreignTableId, 1);

	document = BsonCreate();
	BsonAppendStartObject(document, "$set", &set);

	/* Get following parameters from slot */
	if (slot != NULL && fmstate->target_attrs != NIL)
	{
		ListCell   *lc;

		foreach(lc, fmstate->target_attrs)
		{
			int			attnum = lfirst_int(lc);
#if PG_VERSION_NUM < 110000
			Form_pg_attribute attr = slot->tts_tupleDescriptor->attrs[attnum - 1];
#else
			Form_pg_attribute attr = TupleDescAttr(slot->tts_tupleDescriptor,
												   attnum - 1);
#endif
			Datum		value;
			bool		isnull;

			if (strcmp("_id", attr->attname.data) == 0)
				continue;

			if (strcmp("__doc", attr->attname.data) == 0)
				elog(ERROR, "system column '__doc' update is not supported");

			value = slot_getattr(slot, attnum, &isnull);
#ifdef META_DRIVER
			AppendMongoValue(&set, attr->attname.data, value,
							 isnull ? true : false, attr->atttypid);
#else
			AppendMongoValue(document, attr->attname.data, value,
							 isnull ? true : false, attr->atttypid);
#endif
		}
	}
	BsonAppendFinishObject(document, &set);
	BsonFinish(document);

	op = BsonCreate();
	if (!AppendMongoValue(op, columnName, datum, false, typoid))
	{
		BsonDestroy(document);
		return NULL;
	}
	BsonFinish(op);

	/* We are ready to update the row into MongoDB */
	MongoUpdate(fmstate->mongoConnection, fmstate->options->svr_database,
				fmstate->options->collectionName, op, document);

	BsonDestroy(op);
	BsonDestroy(document);

	/* Return NULL if nothing was updated on the remote end */
	return slot;
}

/*
 * MongoExecForeignDelete
 *		Delete one row from a foreign table
 */
static TupleTableSlot *
MongoExecForeignDelete(EState *estate,
					   ResultRelInfo *resultRelInfo,
					   TupleTableSlot *slot,
					   TupleTableSlot *planSlot)
{
	Datum		datum;
	bool		isNull = false;
	Oid			foreignTableId;
	char	   *columnName = NULL;
	Oid			typoid;
	BSON	   *document;
	MongoFdwModifyState *fmstate;

	fmstate = (MongoFdwModifyState *) resultRelInfo->ri_FdwState;

	foreignTableId = RelationGetRelid(resultRelInfo->ri_RelationDesc);

	/* Get the id that was passed up as a resjunk column */
	datum = ExecGetJunkAttribute(planSlot, 1, &isNull);

#if PG_VERSION_NUM < 110000
	columnName = get_relid_attribute_name(foreignTableId, 1);
#else
	columnName = get_attname(foreignTableId, 1, false);
#endif

	typoid = get_atttype(foreignTableId, 1);

	document = BsonCreate();
	if (!AppendMongoValue(document, columnName, datum, false, typoid))
	{
		BsonDestroy(document);
		return NULL;
	}
	BsonFinish(document);

	/* Now we are ready to delete a single document from MongoDB */
	MongoDelete(fmstate->mongoConnection, fmstate->options->svr_database,
				fmstate->options->collectionName, document);

	BsonDestroy(document);

	/* Return NULL if nothing was updated on the remote end */
	return slot;
}

/*
 * MongoEndForeignModify
 *		Finish an insert/update/delete operation on a foreign table
 */
static void
MongoEndForeignModify(EState *estate, ResultRelInfo *resultRelInfo)
{
	MongoFdwModifyState *fmstate;

	fmstate = (MongoFdwModifyState *) resultRelInfo->ri_FdwState;
	if (fmstate)
	{
		if (fmstate->options)
		{
			mongo_free_options(fmstate->options);
			fmstate->options = NULL;
		}
		MongoFreeScanState(fmstate);
		pfree(fmstate);
	}
}

/*
 * ForeignTableDocumentCount
 * 		Connects to the MongoDB server, and queries it for the number of
 * 		documents in the foreign collection. On success, the function returns
 * 		the document count.  On failure, the function returns -1.0.
 */
static double
ForeignTableDocumentCount(Oid foreignTableId)
{
	MongoFdwOptions *options;
	MONGO_CONN *mongoConnection;
	const BSON *emptyQuery = NULL;
	double 		documentCount;
	Oid			userid = GetUserId();
	ForeignServer *server;
	UserMapping *user;
	ForeignTable *table;

	/* Get info about foreign table. */
	table = GetForeignTable(foreignTableId);
	server = GetForeignServer(table->serverid);
	user = GetUserMapping(userid, server->serverid);

	/* Resolve foreign table options; and connect to mongo server */
	options = mongo_get_options(foreignTableId);

	/*
	 * Get connection to the foreign server.  Connection manager will
	 * establish new connection if necessary.
	 */
	mongoConnection = mongo_get_connection(server, user, options);

	documentCount = MongoAggregateCount(mongoConnection, options->svr_database,
										options->collectionName, emptyQuery);

	mongo_free_options(options);

	return documentCount;
}

/*
 * ColumnMappingHash
 *		Creates a hash table that maps column names to column index and types.
 *
 * This table helps us quickly translate BSON document key/values to the
 * corresponding PostgreSQL columns.
 */
static HTAB *
ColumnMappingHash(Oid foreignTableId, List *columnList)
{
	ListCell   *columnCell;
	const long	hashTableSize = 2048;
	HTAB	   *columnMappingHash;

	/* Create hash table */
	HASHCTL		hashInfo;

	memset(&hashInfo, 0, sizeof(hashInfo));
	hashInfo.keysize = NAMEDATALEN;
	hashInfo.entrysize = sizeof(ColumnMapping);
	hashInfo.hash = string_hash;
	hashInfo.hcxt = CurrentMemoryContext;

	columnMappingHash = hash_create("Column Mapping Hash", hashTableSize,
									&hashInfo,
									(HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT));
	Assert(columnMappingHash != NULL);

	foreach(columnCell, columnList)
	{
		Var		   *column = (Var *) lfirst(columnCell);
		AttrNumber	columnId = column->varattno;
		ColumnMapping *columnMapping;
		char	   *columnName = NULL;
		bool		handleFound = false;
		void	   *hashKey;

#if PG_VERSION_NUM < 110000
		columnName = get_relid_attribute_name(foreignTableId, columnId);
#else
		columnName = get_attname(foreignTableId, columnId, false);
#endif
		hashKey = (void *) columnName;

		columnMapping = (ColumnMapping *) hash_search(columnMappingHash,
													  hashKey,
													  HASH_ENTER,
													  &handleFound);
		Assert(columnMapping != NULL);

		columnMapping->columnIndex = columnId - 1;
		columnMapping->columnTypeId = column->vartype;
		columnMapping->columnTypeMod = column->vartypmod;
		columnMapping->columnArrayTypeId = get_element_type(column->vartype);
	}

	return columnMappingHash;
}

/*
 * FillTupleSlot
 *		Walks over all key/value pairs in the given document.
 *
 * For each pair, the function checks if the key appears in the column mapping
 * hash, and if the value type is compatible with the one specified for the
 * column.  If so, the function converts the value and fills the corresponding
 * tuple position.  The bsonDocumentKey parameter is used for recursion, and
 * should always be passed as NULL.
 */
static void
FillTupleSlot(const BSON *bsonDocument,
			  const char *bsonDocumentKey,
			  HTAB *columnMappingHash,
			  Datum *columnValues,
			  bool *columnNulls)
{
	ColumnMapping *columnMapping;
	bool		handleFound = false;
	void	   *hashKey;
	BSON_ITERATOR bsonIterator = {NULL, 0};

	if (BsonIterInit(&bsonIterator, (BSON *) bsonDocument) == false)
		elog(ERROR, "failed to initialize BSON iterator");

	hashKey = "__doc";
	columnMapping = (ColumnMapping *) hash_search(columnMappingHash, hashKey,
												  HASH_FIND, &handleFound);

	if (columnMapping != NULL && handleFound == true &&
		columnValues[columnMapping->columnIndex] == 0)
	{
		JsonLexContext *lex;
		text	   *result;
		Datum		columnValue;
		char	   *str;

		str = BsonAsJson(bsonDocument);
		result = cstring_to_text_with_len(str, strlen(str));
		lex = makeJsonLexContext(result, false);
		pg_parse_json(lex, &nullSemAction);
		columnValue = PointerGetDatum(result);

		switch (columnMapping->columnTypeId)
		{
			case BOOLOID:
			case INT2OID:
			case INT4OID:
			case INT8OID:
			case BOXOID:
			case BYTEAOID:
			case CHAROID:
			case VARCHAROID:
			case NAMEOID:
			case JSONOID:
			case XMLOID:
			case POINTOID:
			case LSEGOID:
			case LINEOID:
			case UUIDOID:
			case LSNOID:
			case TEXTOID:
			case CASHOID:
			case DATEOID:
			case MACADDROID:
			case TIMESTAMPOID:
			case TIMESTAMPTZOID:
			case BPCHAROID:
				columnValue = PointerGetDatum(result);
				break;
			case JSONBOID:
				columnValue = DirectFunctionCall1(jsonb_in,
												  PointerGetDatum(str));
				break;
			default:
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
						 errmsg("unsupported type for column __doc"),
						 errhint("Column type: %u",
								 (uint32) columnMapping->columnTypeId)));
				break;
		}

		columnValues[columnMapping->columnIndex] = columnValue;
		columnNulls[columnMapping->columnIndex] = false;

		return;
	}

	while (BsonIterNext(&bsonIterator))
	{
		const char *bsonKey = BsonIterKey(&bsonIterator);
		BSON_TYPE	bsonType = BsonIterType(&bsonIterator);
		Oid			columnTypeId = InvalidOid;
		Oid			columnArrayTypeId = InvalidOid;
		bool		compatibleTypes = false;
		bool		handleFound = false;
		const char *bsonFullKey;
		void	   *hashKey;
		int32		columnIndex;

		columnMapping = NULL;
		if (bsonDocumentKey != NULL)
		{
			/*
			 * For fields in nested BSON objects, we use fully qualified field
			 * name to check the column mapping.
			 */
			StringInfo	bsonFullKeyString = makeStringInfo();

			appendStringInfo(bsonFullKeyString, "%s.%s", bsonDocumentKey,
							 bsonKey);
			bsonFullKey = bsonFullKeyString->data;
		}
		else
			bsonFullKey = bsonKey;

		/* Look up the corresponding column for this bson key */
		hashKey = (void *) bsonFullKey;
		columnMapping = (ColumnMapping *) hash_search(columnMappingHash,
													  hashKey,
													  HASH_FIND,
													  &handleFound);
		if (columnMapping != NULL)
		{
			columnTypeId = columnMapping->columnTypeId;
			columnArrayTypeId = columnMapping->columnArrayTypeId;
		}

		/* Recurse into nested objects */
		if (bsonType == BSON_TYPE_DOCUMENT)
		{
			if (columnTypeId != JSONOID)
			{
				BSON		subObject;

				BsonIterSubObject(&bsonIterator, &subObject);
				FillTupleSlot(&subObject, bsonFullKey, columnMappingHash,
							  columnValues, columnNulls);
				continue;
			}
		}

		/* If no corresponding column or null BSON value, continue */
		if (columnMapping == NULL || bsonType == BSON_TYPE_NULL)
			continue;

		/* Check if columns have compatible types */
		if (OidIsValid(columnArrayTypeId) && bsonType == BSON_TYPE_ARRAY)
			compatibleTypes = true;
		else
			compatibleTypes = ColumnTypesCompatible(bsonType, columnTypeId);

		/* If types are incompatible, leave this column null */
		if (!compatibleTypes)
			continue;

		columnIndex = columnMapping->columnIndex;
		/* Fill in corresponding column value and null flag */
		if (OidIsValid(columnArrayTypeId))
			columnValues[columnIndex] = ColumnValueArray(&bsonIterator,
														 columnArrayTypeId);
		else
			columnValues[columnIndex] = ColumnValue(&bsonIterator,
													columnTypeId,
													columnMapping->columnTypeMod);
		columnNulls[columnIndex] = false;
	}
}

/*
 * ColumnTypesCompatible
 * 		Checks if the given BSON type can be converted to the given PostgreSQL
 * 		type.
 *
 * In this check, the function also uses its knowledge of internal conversions
 * applied by BSON APIs.
 */
static bool
ColumnTypesCompatible(BSON_TYPE bsonType, Oid columnTypeId)
{
	bool		compatibleTypes = false;

	/* We consider the PostgreSQL column type as authoritative */
	switch (columnTypeId)
	{
		case INT2OID:
		case INT4OID:
		case INT8OID:
		case FLOAT4OID:
		case FLOAT8OID:
		case NUMERICOID:
			if (bsonType == BSON_TYPE_INT32 || bsonType == BSON_TYPE_INT64 ||
				bsonType == BSON_TYPE_DOUBLE)
				compatibleTypes = true;
			break;
		case BOOLOID:
			if (bsonType == BSON_TYPE_INT32 || bsonType == BSON_TYPE_INT64 ||
				bsonType == BSON_TYPE_DOUBLE || bsonType == BSON_TYPE_BOOL)
				compatibleTypes = true;
			break;
		case BPCHAROID:
		case VARCHAROID:
		case TEXTOID:
			if (bsonType == BSON_TYPE_UTF8)
				compatibleTypes = true;
			break;
		case BYTEAOID:
			if (bsonType == BSON_TYPE_BINDATA)
				compatibleTypes = true;
#ifdef META_DRIVER
			if (bsonType == BSON_TYPE_OID)
				compatibleTypes = true;
#endif
			break;
		case NAMEOID:

			/*
			 * We currently overload the NAMEOID type to represent the BSON
			 * object identifier.  We can safely overload this 64-byte data
			 * type since it's reserved for internal use in PostgreSQL.
			 */
			if (bsonType == BSON_TYPE_OID)
				compatibleTypes = true;
			break;
		case DATEOID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			if (bsonType == BSON_TYPE_DATE_TIME)
				compatibleTypes = true;
			break;
		case NUMERICARRAY_OID:
			if (bsonType == BSON_TYPE_ARRAY)
				compatibleTypes = true;
			break;
		case JSONOID:
			if (bsonType == BSON_TYPE_DOCUMENT ||
				bsonType == BSON_TYPE_ARRAY)
				compatibleTypes = true;
			break;
		default:
			/*
			 * We currently error out on other data types. Some types such as
			 * byte arrays are easy to add, but they need testing.
			 *
			 * Other types such as money or inet, do not have equivalents in
			 * MongoDB.
			 */
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
					 errmsg("cannot convert BSON type to column type"),
					 errhint("Column type: %u", (uint32) columnTypeId)));
			break;
	}

	return compatibleTypes;
}

/*
 * ColumnValueArray
 * 		Uses array element type id to read the current array pointed to by the
 * 		BSON iterator, and converts each array element (with matching type) to
 * 		the corresponding PostgreSQL datum.
 *
 * Then, the function constructs an array datum from element datums, and
 * returns the array datum.
 */
static Datum
ColumnValueArray(BSON_ITERATOR *bsonIterator, Oid valueTypeId)
{
	Datum	   *columnValueArray = palloc(INITIAL_ARRAY_CAPACITY * sizeof(Datum));
	uint32		arrayCapacity = INITIAL_ARRAY_CAPACITY;
	uint32		arrayIndex = 0;
	ArrayType  *columnValueObject;
	Datum		columnValueDatum;
	bool		typeByValue;
	char		typeAlignment;
	int16		typeLength;

	BSON_ITERATOR bsonSubIterator = {NULL, 0};

	BsonIterSubIter(bsonIterator, &bsonSubIterator);
	while (BsonIterNext(&bsonSubIterator))
	{
		BSON_TYPE	bsonType = BsonIterType(&bsonSubIterator);
		bool		compatibleTypes = false;

		compatibleTypes = ColumnTypesCompatible(bsonType, valueTypeId);
		if (bsonType == BSON_TYPE_NULL || !compatibleTypes)
			continue;

		if (arrayIndex >= arrayCapacity)
		{
			/* Double the array capacity. */
			arrayCapacity *= 2;
			columnValueArray = repalloc(columnValueArray,
										arrayCapacity * sizeof(Datum));
		}

		/* Use default type modifier (0) to convert column value */
		columnValueArray[arrayIndex] = ColumnValue(&bsonSubIterator,
												   valueTypeId, 0);
		arrayIndex++;
	}

	get_typlenbyvalalign(valueTypeId, &typeLength, &typeByValue,
						 &typeAlignment);
	columnValueObject = construct_array(columnValueArray,
										arrayIndex,
										valueTypeId,
										typeLength,
										typeByValue,
										typeAlignment);

	columnValueDatum = PointerGetDatum(columnValueObject);

	pfree(columnValueArray);

	return columnValueDatum;
}

/*
 * ColumnValue
 * 		Uses column type information to read the current value pointed to by
 * 		the BSON iterator, and converts this value to the corresponding
 * 		PostgreSQL datum.  The function then returns this datum.
 */
static Datum
ColumnValue(BSON_ITERATOR *bsonIterator, Oid columnTypeId, int32 columnTypeMod)
{
	Datum		columnValue;

	switch (columnTypeId)
	{
		case INT2OID:
			{
				int16		value = (int16) BsonIterInt32(bsonIterator);

				columnValue = Int16GetDatum(value);
			}
			break;
		case INT4OID:
			{
				int32		value = BsonIterInt32(bsonIterator);

				columnValue = Int32GetDatum(value);
			}
			break;
		case INT8OID:
			{
				int64		value = BsonIterInt64(bsonIterator);

				columnValue = Int64GetDatum(value);
			}
			break;
		case FLOAT4OID:
			{
				float4		value = (float4) BsonIterDouble(bsonIterator);

				columnValue = Float4GetDatum(value);
			}
			break;
		case FLOAT8OID:
			{
				float8		value = BsonIterDouble(bsonIterator);

				columnValue = Float8GetDatum(value);
			}
			break;
		case NUMERICOID:
			{
				float8		value = BsonIterDouble(bsonIterator);
				Datum		valueDatum = Float8GetDatum(value);

				/* Overlook type modifiers for numeric */
				columnValue = DirectFunctionCall1(float8_numeric, valueDatum);
			}
			break;
		case BOOLOID:
			{
				bool		value = BsonIterBool(bsonIterator);

				columnValue = BoolGetDatum(value);
			}
			break;
		case BPCHAROID:
			{
				const char *value = BsonIterString(bsonIterator);
				Datum		valueDatum = CStringGetDatum(value);

				columnValue = DirectFunctionCall3(bpcharin, valueDatum,
												  ObjectIdGetDatum(InvalidOid),
												  Int32GetDatum(columnTypeMod));
			}
			break;
		case VARCHAROID:
			{
				const char *value = BsonIterString(bsonIterator);
				Datum		valueDatum = CStringGetDatum(value);

				columnValue = DirectFunctionCall3(varcharin, valueDatum,
												  ObjectIdGetDatum(InvalidOid),
												  Int32GetDatum(columnTypeMod));
			}
			break;
		case TEXTOID:
			{
				const char *value = BsonIterString(bsonIterator);

				columnValue = CStringGetTextDatum(value);
			}
			break;
		case NAMEOID:
			{
				char		value[NAMEDATALEN];
				Datum		valueDatum = 0;

				bson_oid_t *bsonObjectId = (bson_oid_t *) BsonIterOid(bsonIterator);

				bson_oid_to_string(bsonObjectId, value);

				valueDatum = CStringGetDatum(value);
				columnValue = DirectFunctionCall3(namein, valueDatum,
												  ObjectIdGetDatum(InvalidOid),
												  Int32GetDatum(columnTypeMod));
			}
			break;
		case BYTEAOID:
			{
				int			value_len;
				char	   *value;
				bytea	   *result;
#ifdef META_DRIVER
				switch (BsonIterType(bsonIterator))
				{
					case BSON_TYPE_OID:
						value = (char *) BsonIterOid(bsonIterator);
						value_len = 12;
						break;
					default:
						value = (char *) BsonIterBinData(bsonIterator,
														 (uint32_t *) &value_len);
						break;
				}
#else
				value_len = BsonIterBinLen(bsonIterator);
				value = (char *) BsonIterBinData(bsonIterator);
#endif
				result = (bytea *) palloc(value_len + VARHDRSZ);
				memcpy(VARDATA(result), value, value_len);
				SET_VARSIZE(result, value_len + VARHDRSZ);
				columnValue = PointerGetDatum(result);
			}
			break;
		case DATEOID:
			{
				int64		valueMillis = BsonIterDate(bsonIterator);
				int64		timestamp = (valueMillis * 1000L) - POSTGRES_TO_UNIX_EPOCH_USECS;
				Datum		timestampDatum = TimestampGetDatum(timestamp);

				columnValue = DirectFunctionCall1(timestamp_date,
												  timestampDatum);
			}
			break;
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			{
				int64		valueMillis = BsonIterDate(bsonIterator);
				int64		timestamp = (valueMillis * 1000L) - POSTGRES_TO_UNIX_EPOCH_USECS;

				/* Overlook type modifiers for timestamp */
				columnValue = TimestampGetDatum(timestamp);
			}
			break;
		case JSONOID:
			{
				JsonLexContext *lex;
				text	   *result;
				StringInfo	buffer = makeStringInfo();

				BSON_TYPE	type = BSON_ITER_TYPE(bsonIterator);

				if (type != BSON_TYPE_ARRAY && type != BSON_TYPE_DOCUMENT)
					ereport(ERROR,
							(errmsg("cannot convert to json")));

#ifdef META_DRIVER
				/* Convert BSON to JSON value */
				BsonToJsonStringValue(buffer, bsonIterator,
									  BSON_TYPE_ARRAY == type);
#else
				/* Convert BSON to JSON value */
				BsonToJsonString(buffer, *bsonIterator,
								 BSON_TYPE_ARRAY == type);
#endif
				result = cstring_to_text_with_len(buffer->data, buffer->len);
				lex = makeJsonLexContext(result, false);
				pg_parse_json(lex, &nullSemAction);
				columnValue = PointerGetDatum(result);
			}
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
					 errmsg("cannot convert BSON type to column type"),
					 errhint("Column type: %u", (uint32) columnTypeId)));
			break;
	}

	return columnValue;
}

void
BsonToJsonString(StringInfo output, BSON_ITERATOR i, bool isArray)
{
	const char *key;
	bool		isFirstElement;
	char		beginSymbol = '{';
	char		endSymbol = '}';
	BSON_TYPE	bsonType;

	if (isArray)
	{
		beginSymbol = '[';
		endSymbol = ']';
	}

#ifndef META_DRIVER
	{
		const char *bsonData = bson_iterator_value(&i);

		bson_iterator_from_buffer(&i, bsonData);
	}
#endif

	appendStringInfoChar(output, beginSymbol);

	isFirstElement = true;
	while (BsonIterNext(&i))
	{
		if (!isFirstElement)
			appendStringInfoChar(output, ',');

		bsonType = BsonIterType(&i);
		if (bsonType == 0)
			break;

		key = BsonIterKey(&i);

		if (!isArray)
			appendStringInfo(output, "\"%s\":", key);

		switch (bsonType)
		{
			case BSON_TYPE_DOUBLE:
				appendStringInfo(output, "%f", BsonIterDouble(&i));
				break;
			case BSON_TYPE_UTF8:
				appendStringInfo(output, "\"%s\"",
								 EscapeJsonString(BsonIterString(&i)));
				break;
			case BSON_TYPE_SYMBOL:
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
						 errmsg("\"symbol\" BSON type is deprecated and unsupported"),
						 errhint("Symbol: %s", BsonIterString(&i))));
				break;
			case BSON_TYPE_OID:
				{
					char		oidhex[25];

					BsonOidToString(BsonIterOid(&i), oidhex);
					appendStringInfo(output, "{\"$oid\":\"%s\"}", oidhex);
					break;
				}
			case BSON_TYPE_BOOL:
				appendStringInfoString(output,
									   BsonIterBool(&i) ? "true" : "false");
				break;
			case BSON_TYPE_DATE_TIME:
				appendStringInfo(output, "{\"$date\":%ld}",
								 (long int) BsonIterDate(&i));
				break;
			case BSON_TYPE_BINDATA:
				/* It's possible to encode the data with base64 here. */
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
						 errmsg("support for \"binary data\" BSON type is not implemented")));
				break;
			case BSON_TYPE_UNDEFINED:
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
						 errmsg("\"undefined\" BSON type is deprecated and unsupported")));
				break;
			case BSON_TYPE_NULL:
				appendStringInfoString(output, "null");
				break;
			case BSON_TYPE_REGEX:
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
						 errmsg("support for \"regex\" BSON type is not implemented"),
						 errhint("Regex: %s", BsonIterRegex(&i))));
				break;
			case BSON_TYPE_CODE:
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
						 errmsg("support for \"code\" BSON type is not implemented"),
						 errhint("Code: %s", BsonIterCode(&i))));
				break;
			case BSON_TYPE_CODEWSCOPE:
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
						 errmsg("support for \"code\" with scope` BSON type is not implemented")));
				break;
			case BSON_TYPE_INT32:
				appendStringInfo(output, "%d", BsonIterInt32(&i));
				break;
			case BSON_TYPE_INT64:
				appendStringInfo(output, "%lu", (uint64_t) BsonIterInt64(&i));
				break;
			case BSON_TYPE_TIMESTAMP:
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
						 errmsg("internal `timestamp` BSON type is and unsupported")));
				break;
			case BSON_TYPE_DOCUMENT:
				BsonToJsonString(output, i, false);
				break;
			case BSON_TYPE_ARRAY:
				BsonToJsonString(output, i, true);
				break;
			default:
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
						 errmsg("unsupported BSON type: %x", bsonType)));
		}
		isFirstElement = false;
	}
	appendStringInfoChar(output, endSymbol);
}

/*
 * EscapeJsonString
 *		Escapes a string for safe inclusion in JSON.
 */
const char *
EscapeJsonString(const char *string)
{
	StringInfo	buffer;
	const char *ptr;
	int			i;
	int			segmentStartIdx;
	int			len;
	bool		needsEscaping = false;

	for (ptr = string; *ptr; ++ptr)
	{
		if (*ptr == '"' || *ptr == '\r' || *ptr == '\n' || *ptr == '\t' ||
			*ptr == '\\')
		{
			needsEscaping = true;
			break;
		}
	}

	if (!needsEscaping)
		return string;

	buffer = makeStringInfo();
	len = strlen(string);
	segmentStartIdx = 0;
	for (i = 0; i < len; ++i)
	{
		if (string[i] == '"' || string[i] == '\r' || string[i] == '\n' ||
			string[i] == '\t' || string[i] == '\\')
		{
			if (segmentStartIdx < i)
				appendBinaryStringInfo(buffer, string + segmentStartIdx,
									   i - segmentStartIdx);

			appendStringInfoChar(buffer, '\\');
			if (string[i] == '"')
				appendStringInfoChar(buffer, '"');
			else if (string[i] == '\r')
				appendStringInfoChar(buffer, 'r');
			else if (string[i] == '\n')
				appendStringInfoChar(buffer, 'n');
			else if (string[i] == '\t')
				appendStringInfoChar(buffer, 't');
			else if (string[i] == '\\')
				appendStringInfoChar(buffer, '\\');

			segmentStartIdx = i + 1;
		}
	}
	if (segmentStartIdx < len)
		appendBinaryStringInfo(buffer, string + segmentStartIdx,
							   len - segmentStartIdx);
	return buffer->data;
}

/*
 * MongoFreeScanState
 *		Closes the cursor and connection to MongoDB, and reclaims all Mongo
 *		related resources allocated for the foreign scan.
 */
static void
MongoFreeScanState(MongoFdwModifyState *fmstate)
{
	if (fmstate == NULL)
		return;

	if (fmstate->queryDocument)
	{
		BsonDestroy(fmstate->queryDocument);
		fmstate->queryDocument = NULL;
	}

	if (fmstate->mongoCursor)
	{
		MongoCursorDestroy(fmstate->mongoCursor);
		fmstate->mongoCursor = NULL;
	}

	/* Release remote connection */
	mongo_release_connection(fmstate->mongoConnection);
}

/*
 * MongoAnalyzeForeignTable
 *		Collects statistics for the given foreign table.
 */
static bool
MongoAnalyzeForeignTable(Relation relation,
						 AcquireSampleRowsFunc *func,
						 BlockNumber *totalpages)
{
	BlockNumber pageCount = 0;
	int			attributeCount;
	int32	   *attributeWidths;
	Oid			foreignTableId;
	int32		documentWidth;
	double 		documentCount;
	double		foreignTableSize;

	foreignTableId = RelationGetRelid(relation);
	documentCount = ForeignTableDocumentCount(foreignTableId);

	if (documentCount > 0.0)
	{
		attributeCount = RelationGetNumberOfAttributes(relation);
		attributeWidths = (int32 *) palloc0((attributeCount + 1) * sizeof(int32));

		/*
		 * We estimate disk costs assuming a sequential scan over the data.
		 * This is an inaccurate assumption as Mongo scatters the data over
		 * disk pages, and may rely on an index to retrieve the data.  Still,
		 * this should at least give us a relative cost.
		 */
		documentWidth = get_relation_data_width(foreignTableId,
												attributeWidths);
		foreignTableSize = documentCount * documentWidth;

		pageCount = (BlockNumber) rint(foreignTableSize / BLCKSZ);
	}
	else
		ereport(DEBUG1,
				(errmsg("could not retrieve document count for collection"),
				 errhint("Could not	collect statistics about foreign table.")));

	(*totalpages) = pageCount;
	(*func) = MongoAcquireSampleRows;

	return true;
}

/*
 * MongoAcquireSampleRows
 *		Acquires a random sample of rows from the foreign table.
 *
 * Selected rows are returned in the caller allocated sampleRows array,
 * which must have at least target row count entries.  The actual number of
 * rows selected is returned as the function result.  We also count the number
 * of rows in the collection and return it in total row count.  We also always
 * set dead row count to zero.
 *
 * Note that the returned list of rows is not always in order by physical
 * position in the MongoDB collection.  Therefore, correlation estimates
 * derived later may be meaningless, but it's OK because we don't use the
 * estimates currently (the planner only pays attention to correlation for
 * index scans).
 */
static int
MongoAcquireSampleRows(Relation relation,
					   int errorLevel,
					   HeapTuple *sampleRows,
					   int targetRowCount,
					   double *totalRowCount,
					   double *totalDeadRowCount)
{
	MONGO_CONN *mongoConnection;
	int			sampleRowCount = 0;
	double		rowCount = 0;
	double		rowCountToSkip = -1;	/* -1 means not set yet */
	double		randomState;
	Datum	   *columnValues;
	bool	   *columnNulls;
	Oid			foreignTableId;
	TupleDesc	tupleDescriptor;
	AttrNumber	columnCount;
	AttrNumber	columnId;
	HTAB	   *columnMappingHash;
	MONGO_CURSOR *mongoCursor;
	BSON	   *queryDocument;
	List	   *columnList = NIL;
	char	   *relationName;
	MemoryContext oldContext = CurrentMemoryContext;
	MemoryContext tupleContext;
	MongoFdwOptions *options;
	ForeignServer *server;
	UserMapping *user;
	ForeignTable *table;

	/* Create list of columns in the relation */
	tupleDescriptor = RelationGetDescr(relation);
	columnCount = tupleDescriptor->natts;

	for (columnId = 1; columnId <= columnCount; columnId++)
	{
		Var		   *column = (Var *) palloc0(sizeof(Var));
#if PG_VERSION_NUM >= 110000
		Form_pg_attribute attr = TupleDescAttr(tupleDescriptor, columnId - 1);

		column->varattno = columnId;
		column->vartype = attr->atttypid;
		column->vartypmod = attr->atttypmod;
#else
		/* Only assign required fields for column mapping hash */
		column->varattno = columnId;
		column->vartype = tupleDescriptor->attrs[columnId - 1]->atttypid;
		column->vartypmod = tupleDescriptor->attrs[columnId - 1]->atttypmod;
#endif

		columnList = lappend(columnList, column);
	}

	foreignTableId = RelationGetRelid(relation);
	table = GetForeignTable(foreignTableId);
	server = GetForeignServer(table->serverid);
	user = GetUserMapping(GetUserId(), server->serverid);
	options = mongo_get_options(foreignTableId);

	/*
	 * Get connection to the foreign server.  Connection manager will establish
	 * new connection if necessary.
	 */
	mongoConnection = mongo_get_connection(server, user, options);

	queryDocument = QueryDocument(foreignTableId, NIL, NULL);
	/* Create cursor for collection name and set query */
	mongoCursor = MongoCursorCreate(mongoConnection, options->svr_database,
									options->collectionName, queryDocument);
	columnMappingHash = ColumnMappingHash(foreignTableId, columnList);

	/*
	 * Use per-tuple memory context to prevent leak of memory used to read
	 * rows from the file with copy routines.
	 */
#if PG_VERSION_NUM < 110000
	tupleContext = AllocSetContextCreate(CurrentMemoryContext,
										 "mongo_fdw temporary context",
										 ALLOCSET_DEFAULT_MINSIZE,
										 ALLOCSET_DEFAULT_INITSIZE,
										 ALLOCSET_DEFAULT_MAXSIZE);
#else
	tupleContext = AllocSetContextCreate(CurrentMemoryContext,
										 "mongo_fdw temporary context",
										 ALLOCSET_DEFAULT_SIZES);
#endif

	/* Prepare for sampling rows */
	randomState = anl_init_selection_state(targetRowCount);

	columnValues = (Datum *) palloc(columnCount * sizeof(Datum));
	columnNulls = (bool *) palloc(columnCount * sizeof(bool));

	for (;;)
	{
		/* Check for user-requested abort or sleep */
		vacuum_delay_point();

		/* Initialize all values for this row to null */
		memset(columnValues, 0, columnCount * sizeof(Datum));
		memset(columnNulls, true, columnCount * sizeof(bool));

		if (MongoCursorNext(mongoCursor, NULL))
		{
			const BSON *bsonDocument = MongoCursorBson(mongoCursor);
			const char *bsonDocumentKey = NULL; /* Top level document */

			/* Fetch next tuple */
			MemoryContextReset(tupleContext);
			MemoryContextSwitchTo(tupleContext);

			FillTupleSlot(bsonDocument, bsonDocumentKey,
						  columnMappingHash, columnValues, columnNulls);

			MemoryContextSwitchTo(oldContext);
		}
		else
		{
#ifdef META_DRIVER
			bson_error_t error;

			if (mongoc_cursor_error(mongoCursor, &error))
				ereport(ERROR,
						(errmsg("could not iterate over mongo collection"),
						 errhint("Mongo driver error: %s", error.message)));
#else
			mongo_cursor_error_t errorCode = mongoCursor->err;

			if (errorCode != MONGO_CURSOR_EXHAUSTED)
				ereport(ERROR,
						(errmsg("could not iterate over mongo collection"),
						 errhint("Mongo driver cursor error code: %d",
								 errorCode)));
#endif
			break;
		}

		/*
		 * The first targetRowCount sample rows are simply copied into the
		 * reservoir.  Then we start replacing tuples in the sample until we
		 * reach the end of the relation.  This algorithm is from Jeff
		 * Vitter's paper (see more info in commands/analyze.c).
		 */
		if (sampleRowCount < targetRowCount)
			sampleRows[sampleRowCount++] = heap_form_tuple(tupleDescriptor,
														   columnValues,
														   columnNulls);
		else
		{
			/*
			 * t in Vitter's paper is the number of records already processed.
			 * If we need to compute a new S value, we must use the "not yet
			 * incremented" value of rowCount as t.
			 */
			if (rowCountToSkip < 0)
				rowCountToSkip = anl_get_next_S(rowCount, targetRowCount,
												&randomState);

			if (rowCountToSkip <= 0)
			{
				/*
				 * Found a suitable tuple, so save it, replacing one old tuple
				 * at random.
				 */
				int			rowIndex = (int) (targetRowCount * anl_random_fract());

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

	/* Only clean up the query struct, but not its data */
	BsonDestroy(queryDocument);

	/* Clean up */
	MemoryContextDelete(tupleContext);

	pfree(columnValues);
	pfree(columnNulls);

	/* Emit some interesting relation info */
	relationName = RelationGetRelationName(relation);
	ereport(errorLevel,
			(errmsg("\"%s\": collection contains %.0f rows; %d rows in sample",
					relationName, rowCount, sampleRowCount)));

	(*totalRowCount) = rowCount;
	(*totalDeadRowCount) = 0;

	return sampleRowCount;
}

Datum
mongo_fdw_version(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT32(CODE_VERSION);
}

#if PG_VERSION_NUM >= 110000
/*
 * MongoBeginForeignInsert
 * 		Prepare for an insert operation triggered by partition routing
 * 		or COPY FROM.
 *
 * This is not yet supported, so raise an error.
 */
static void
MongoBeginForeignInsert(ModifyTableState *mtstate,
						ResultRelInfo *resultRelInfo)
{
	ereport(ERROR,
			(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
			 errmsg("COPY and foreign partition routing not supported in mongo_fdw")));
}

/*
 * MongoEndForeignInsert
 * 		BeginForeignInsert() is not yet implemented, hence we do not
 * 		have anything to cleanup as of now. We throw an error here just
 * 		to make sure when we do that we do not forget to cleanup
 * 		resources.
 */
static void
MongoEndForeignInsert(EState *estate, ResultRelInfo *resultRelInfo)
{
	ereport(ERROR,
			(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
			 errmsg("COPY and foreign partition routing not supported in mongo_fdw")));
}
#endif
