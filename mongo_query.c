/*-------------------------------------------------------------------------
 *
 * mongo_query.c
 * 		FDW query handling for mongo_fdw
 *
 * Portions Copyright (c) 2012-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 2004-2021, EnterpriseDB Corporation.
 * Portions Copyright (c) 2012–2014 Citus Data, Inc.
 *
 * IDENTIFICATION
 * 		mongo_query.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "mongo_wrapper.h"

#include <bson.h>
#include <json.h>

#if PG_VERSION_NUM < 120000
#include "access/sysattr.h"
#endif
#include "access/htup_details.h"
#if PG_VERSION_NUM >= 120000
#include "access/table.h"
#endif
#include "catalog/heap.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_operator.h"
#if PG_VERSION_NUM >= 130000
#include "common/hashfn.h"
#endif
#ifdef META_DRIVER
#include "mongoc.h"
#else
#include "mongo.h"
#endif
#include "mongo_query.h"
#if PG_VERSION_NUM < 120000
#include "nodes/relation.h"
#include "optimizer/var.h"
#endif
#if PG_VERSION_NUM >= 120000
#include "optimizer/optimizer.h"
#endif
#include "parser/parsetree.h"
#include "utils/rel.h"
#include "utils/syscache.h"

/*
 * Global context for foreign_expr_walker's search of an expression tree.
 */
typedef struct foreign_glob_cxt
{
	PlannerInfo *root;			/* global planner state */
	RelOptInfo *foreignrel;		/* the foreign relation we are planning for */
	unsigned short varcount;	/* Var count */
	unsigned short opexprcount;
	Relids		relids;			/* relids of base relations in the underlying
								 * scan */
	bool		is_join_cond;	/* "true" for join relations */
} foreign_glob_cxt;

/*
 * Local (per-tree-level) context for foreign_expr_walker's search.
 * This is concerned with identifying collations used in the expression.
 */
typedef enum
{
	FDW_COLLATE_NONE,			/* expression is of a noncollatable type */
	FDW_COLLATE_SAFE,			/* collation derives from a foreign Var */
	FDW_COLLATE_UNSAFE			/* collation derives from something else */
} FDWCollateState;

typedef struct foreign_loc_cxt
{
	Oid			collation;		/* OID of current collation, if any */
	FDWCollateState state;		/* state of current collation choice */
} foreign_loc_cxt;

/* Local functions forward declarations */
static Expr *FindArgumentOfType(List *argumentList, NodeTag argumentType);
static List *EqualityOperatorList(List *operatorList);
static List *UniqueColumnList(List *operatorList);
static List *ColumnOperatorList(Var *column, List *operatorList);
static void AppendParamValue(BSON *queryDocument, const char *keyName,
							 Param *paramNode,
							 ForeignScanState *scanStateNode);
static bool foreign_expr_walker(Node *node,
								foreign_glob_cxt *glob_cxt,
								foreign_loc_cxt *outer_cxt);
static List *prepare_var_list_for_baserel(Oid relid, Index varno,
										  Bitmapset *attrs_used);
static HTAB *ColumnInfoHash(List *colname_list, List *colnum_list,
							List *rti_list, List *isouter_list);
static void mongo_prepare_inner_pipeline(List *joinclause,
										 BSON *inner_pipeline,
										 List *colname_list,
										 List *isouter_list,
										 pipeline_cxt *context);
static void mongo_append_joinclauses_to_inner_pipeline(List *joinclause,
													   BSON *child_doc,
													   pipeline_cxt *context);

/*
 * FindArgumentOfType
 *		Walks over the given argument list, looks for an argument with the
 *		given type, and returns the argument if it is found.
 */
static Expr *
FindArgumentOfType(List *argumentList, NodeTag argumentType)
{
	Expr	   *foundArgument = NULL;
	ListCell   *argumentCell;

	foreach(argumentCell, argumentList)
	{
		Expr	   *argument = (Expr *) lfirst(argumentCell);

		/* For RelabelType type, examine the inner node */
		if (IsA(argument, RelabelType))
			argument = ((RelabelType *) argument)->arg;

		if (nodeTag(argument) == argumentType)
		{
			foundArgument = argument;
			break;
		}
	}

	return foundArgument;
}

/*
 * QueryDocument
 *		Takes in the applicable operator expressions for relation and also the
 *		join clauses for join relation and converts these expressions and join
 *		clauses into equivalent queries in MongoDB.
 *
 * For join clauses, transforms simple comparison expressions along with a
 * comparison between two vars and nested operator expressions as well.
 *
 * Example: Consider the following two foreign tables:
 *    t1(_id NAME, age INT, name VARCHAR)
 *    t2(_id NAME, old INT, alias VARCHAR)
 *
 * SQL query:
 *    SELECT * FROM t1 LEFT JOIN t2 ON(t1.age = t2.old) WHERE name = 'xyz';
 *
 * Equivalent MongoDB query:
 *
 *    db.t1.aggregate([
 *      {
 *        "$lookup":
 *        {
 *          "from": "t2",
 *          "let": { "v_age": "$age" },
 *          "pipeline": [
 *            {
 *              "$match":
 *              {
 *                "$expr":
 *                {
 *                  "$and": [
 *                    { "$eq": [ "$$v_age", "$old" ] }
 *                    { "$ne": [ "$$v_age", null ] },
 *                    { "$ne": [ "$old", null ] },
 *                  ]
 *                }
 *              }
 *            }
 *          ],
 *          "as": "Join_Result"
 *        }
 *      },
 *      { "$match": { "name" : "xyz" } },
 *      {
 *        "$unwind":
 *        {
 *          "path": "$Join_Result",
 *          "preserveNullAndEmptyArrays": true
 *        }
 *      }
 *    ])
 *
 * Any MongoDB query would have the following three main arrays:
 *  1. Root pipeline array (first square bracket):
 *  	This has three elements called $lookup, $unwind, and $match stages.
 *  2. Inner pipeline array (starting with "pipeline" keyword above):
 *  	It has one element that is $match.
 *  3. "$and" expression inside inner pipeline:
 *  	These elements depend on the join clauses available.
 *
 * The outer $match stage (2nd element of root pipeline array) represents
 * remote_exprs, and $match inside $lookup stage represents the join clauses.
 *
 * For the conditions in WHERE i.e. remote_exprs, this function can only
 * transform simple comparison expressions and returns these transformed
 * expressions in a BSON document.  For example, simple expressions:
 * "l_shipdate >= date '1994-01-01' AND l_shipdate < date '1995-01-01'" becomes
 * "l_shipdate: { $gte: new Date(757382400000), $lt: new Date(788918400000) }".
 */
BSON *
QueryDocument(ForeignScanState *scanStateNode)
{
	MongoFdwModifyState *fmstate = (MongoFdwModifyState *) scanStateNode->fdw_state;
	ForeignScan *fsplan = (ForeignScan *) scanStateNode->ss.ps.plan;
	BSON	   *queryDocument = BsonCreate();
	BSON	   *filter = BsonCreate();
	List	   *PrivateList = fsplan->fdw_private;
	List	   *opExpressionList = list_nth(PrivateList,
											mongoFdwPrivateRemoteExprList);
	BSON		root_pipeline;
	int 		root_index = 0;
	List	   *joinclauses;
	List	   *colname_list = NIL;
	List	   *isouter_list = NIL;
	char	   *inner_relname;
	char	   *outer_relname;
	HTAB	   *columnInfoHash;
	int			jointype;

	/* Prepare array of stages */
	BsonAppendStartArray(queryDocument, "pipeline", &root_pipeline);

	if (fmstate->isJoinRel)
	{
		List	   *colnum_list;
		List	   *rti_list;
		List	   *innerouter_relname;
		int			natts;

		joinclauses = list_nth(PrivateList, mongoFdwPrivateJoinClauseList);
		if (joinclauses)
			jointype = intVal(list_nth(PrivateList, mongoFdwPrivateJoinType));

		innerouter_relname = list_nth(PrivateList,
									  mongoFdwPrivateJoinInnerOuterRelName);
		inner_relname = strVal(list_nth(innerouter_relname, 0));
		outer_relname = strVal(list_nth(innerouter_relname, 1));
		colname_list = list_nth(PrivateList,
								mongoFdwPrivateJoinClauseColNameList);
		colnum_list = list_nth(PrivateList,
							   mongoFdwPrivareJoinClauseColNumList);
		rti_list = list_nth(PrivateList, mongoFdwPrivateJoinClauseRtiList);
		isouter_list = list_nth(PrivateList,
								mongoFdwPrivateJoinClauseIsOuterList);

		/* Length should be same for all lists of column information */
		natts = list_length(colname_list);
		Assert(natts == list_length(colnum_list) &&
			   natts == list_length(rti_list) &&
			   natts == list_length(isouter_list));

		columnInfoHash = ColumnInfoHash(colname_list, colnum_list, rti_list,
										isouter_list);
	}

	/*
	 * Add filter into query pipeline if available.  These are remote_exprs
	 * i.e. clauses available in WHERE and those are push-able to the remote
	 * side.
	 */
	if (opExpressionList)
	{
		Oid			relationId;
		List	   *equalityOperatorList;
		List	   *comparisonOperatorList;
		List	   *columnList;
		ListCell   *equalityOperatorCell;
		ListCell   *columnCell;

		if (fsplan->scan.scanrelid > 0)
			relationId = RelationGetRelid(scanStateNode->ss.ss_currentRelation);
		else
			relationId = 0;

		/*
		 * We distinguish between equality expressions and others since we need
		 * to insert the latter (<, >, <=, >=, <>) as separate sub-documents
		 * into the BSON query object.
		 */
		equalityOperatorList = EqualityOperatorList(opExpressionList);
		comparisonOperatorList = list_difference(opExpressionList,
												 equalityOperatorList);

		/* Append equality expressions to the query */
		foreach(equalityOperatorCell, equalityOperatorList)
		{
			OpExpr	   *equalityOperator;
			Oid			columnId = InvalidOid;
			char	   *columnName;
			Const	   *constant;
			Param	   *paramNode;
			List	   *argumentList;
			Var		   *column;

			equalityOperator = (OpExpr *) lfirst(equalityOperatorCell);
			argumentList = equalityOperator->args;
			column = (Var *) FindArgumentOfType(argumentList, T_Var);
			constant = (Const *) FindArgumentOfType(argumentList, T_Const);
			paramNode = (Param *) FindArgumentOfType(argumentList, T_Param);

			if (relationId != 0)
			{
				columnId = column->varattno;
#if PG_VERSION_NUM < 110000
				columnName = get_relid_attribute_name(relationId, columnId);
#else
				columnName = get_attname(relationId, columnId, false);
#endif
			}
			/* For join rel, use columnInfoHash to get column name */
			else
			{
				bool		found = false;
				ColInfoHashKey key;
				ColInfoHashEntry *columnInfo;

				key.varNo = column->varno;
				key.varAttno = column->varattno;

				columnInfo = (ColInfoHashEntry *) hash_search(columnInfoHash,
															  (void *) &key,
															  HASH_FIND,
															  &found);
				if (found)
					columnName = columnInfo->colName;
			}

			if (constant != NULL)
				AppendConstantValue(filter, columnName, constant);
			else
				AppendParamValue(filter, columnName, paramNode,
								 scanStateNode);
		}

		/*
		 * For comparison expressions, we need to group them by their columns
		 * and then append all expressions that correspond to a column as one
		 * sub-document.  Otherwise, even when we have two expressions to
		 * define the upper and lower bound of a range, Mongo uses only one of
		 * these expressions during an index search.
		 */
		columnList = UniqueColumnList(comparisonOperatorList);

		/* Append comparison expressions, grouped by columns, to the query */
		foreach(columnCell, columnList)
		{
			Var		   *column = (Var *) lfirst(columnCell);
			Oid			columnId = InvalidOid;
			char	   *columnName;
			List	   *columnOperatorList;
			ListCell   *columnOperatorCell;
			BSON		childDocument;

			if (relationId != 0)
			{
				columnId = column->varattno;
#if PG_VERSION_NUM < 110000
				columnName = get_relid_attribute_name(relationId, columnId);
#else
				columnName = get_attname(relationId, columnId, false);
#endif
			}
			/* For join rel, use columnInfoHash to get column name */
			else
			{
				bool		found = false;
				ColInfoHashKey key;
				ColInfoHashEntry *columnInfo;

				key.varNo = column->varno;
				key.varAttno = column->varattno;

				columnInfo = (ColInfoHashEntry *) hash_search(columnInfoHash,
															  (void *) &key,
															  HASH_FIND,
															  &found);
				if (found)
					columnName = columnInfo->colName;
			}

			/* Find all expressions that correspond to the column */
			columnOperatorList = ColumnOperatorList(column,
													comparisonOperatorList);

			/* For comparison expressions, start a sub-document */
			BsonAppendStartObject(filter, columnName, &childDocument);

			foreach(columnOperatorCell, columnOperatorList)
			{
				OpExpr	   *columnOperator;
				char	   *operatorName;
				char	   *mongoOperatorName;
				List	   *argumentList;
				Const	   *constant;

				columnOperator = (OpExpr *) lfirst(columnOperatorCell);
				argumentList = columnOperator->args;
				constant = (Const *) FindArgumentOfType(argumentList, T_Const);
				operatorName = get_opname(columnOperator->opno);
				mongoOperatorName = MongoOperatorName(operatorName);
#ifdef META_DRIVER
				AppendConstantValue(&childDocument, mongoOperatorName,
									constant);
#else
				AppendConstantValue(filter, mongoOperatorName, constant);
#endif
			}
			BsonAppendFinishObject(filter, &childDocument);
		}
	}
	if (!BsonFinish(filter))
	{
#ifdef META_DRIVER
		ereport(ERROR,
				(errmsg("could not create document for query"),
				 errhint("BSON flags: %d", queryDocument->flags)));
#else
		ereport(ERROR,
				(errmsg("could not create document for query"),
				 errhint("BSON error: %d", queryDocument->err)));
#endif
	}

	if (fmstate->isJoinRel)
	{
		BSON		inner_pipeline;
		BSON		lookup_object;
		BSON		lookup;
		BSON		let_exprs;
		BSON		outer_match_stage;
		BSON		unwind_stage;
		BSON		unwind;
		BSON	   *inner_pipeline_doc = BsonCreate();
		ListCell   *cell1;
		ListCell   *cell2;

		/* $lookup stage. This is to perform JOIN */
		BsonAppendStartObject(&root_pipeline, psprintf("%d", root_index++),
							  &lookup_object);
		BsonAppendStartObject(&lookup_object, "$lookup", &lookup);
		BsonAppendUTF8(&lookup, "from", inner_relname);

		/*
		 * Start "let" operator: Specifies variables to use in the pipeline
		 * stages.  To access columns of outer relation, those need to be
		 * defined in terms of a variable using "let".
		 */
		BsonAppendStartObject(&lookup, "let", &let_exprs);
		forboth(cell1, colname_list, cell2, isouter_list)
		{
			char	*colname = strVal(lfirst(cell1));
			bool	 is_outer = lfirst_int(cell2);

			if (is_outer)
			{
				/*
				 * Add prefix "v_" to column name to form variable name.  Need
				 * to prefix with any lowercase letter because variable names
				 * must begin with only a lowercase ASCII letter or a non-ASCII
				 * character.
				 */
				char	*varname = psprintf("v_%s", colname);
				char	*field = psprintf("$%s", colname);

				BsonAppendUTF8(&let_exprs, varname, field);
			}
		}
		BsonAppendFinishObject(&lookup, &let_exprs); /* End "let" */

		/* Form inner pipeline required in $lookup stage to execute $match */
		BsonAppendStartArray(inner_pipeline_doc, "pipeline", &inner_pipeline);
		if (joinclauses)
		{
			pipeline_cxt context;

			context.colInfoHash = columnInfoHash;
			context.isBoolExpr = false;

			 /* Form equivalent join qual clauses in MongoDB */
			mongo_prepare_inner_pipeline(joinclauses, &inner_pipeline,
										 colname_list, isouter_list, &context);
			BsonAppendFinishArray(inner_pipeline_doc, &inner_pipeline);
		}

		/* Append inner pipeline to $lookup stage */
		bson_append_array(&lookup, "pipeline", (int) strlen ("pipeline"),
						  &inner_pipeline);

		BsonAppendUTF8(&lookup, "as", "Join_Result");
		BsonAppendFinishObject(&lookup_object, &lookup);
		BsonAppendFinishObject(&root_pipeline, &lookup_object);

		/* $match stage. This is to add a filter */
		BsonAppendStartObject(&root_pipeline, psprintf("%d", root_index++),
							  &outer_match_stage);
		BsonAppendBson(&outer_match_stage, "$match", filter);
		BsonAppendFinishObject(&root_pipeline, &outer_match_stage);

		/*
		 * $unwind stage. This deconstructs an array field from the input
		 * documents to output a document for each element.
		 */
		BsonAppendStartObject(&root_pipeline, psprintf("%d", root_index++),
							  &unwind_stage);
		BsonAppendStartObject(&unwind_stage, "$unwind", &unwind);
		BsonAppendUTF8(&unwind, "path", "$Join_Result");
		if (jointype == JOIN_INNER)
			BsonAppendBool(&unwind, "preserveNullAndEmptyArrays", false);
		else
			BsonAppendBool(&unwind, "preserveNullAndEmptyArrays", true);
		BsonAppendFinishObject(&unwind_stage, &unwind);
		BsonAppendFinishObject(&root_pipeline, &unwind_stage);

		if (!BsonFinish(queryDocument))
			ereport(ERROR,
					(errmsg("could not create document for query"),
					 errhint("BSON flags: %d", queryDocument->flags)));

		fmstate->outerRelName = outer_relname;
	}
	else
	{
		BSON	    match_stage;

		/* $match stage.  This is to add a filter for the WHERE clause */
		BsonAppendStartObject(&root_pipeline, psprintf("%d", root_index++),
							  &match_stage);
		BsonAppendBson(&match_stage, "$match", filter);
		BsonAppendFinishObject(&root_pipeline, &match_stage);
	}

	BsonAppendFinishArray(queryDocument, &root_pipeline);

	if (!BsonFinish(queryDocument))
	{
		ereport(ERROR,
				(errmsg("could not create document for query"),
				 errhint("BSON flags: %d", queryDocument->flags)));
	}

	return queryDocument;
}

/*
 * MongoOperatorName
 * 		Takes in the given PostgreSQL comparison operator name, and returns its
 * 		equivalent in MongoDB.
 */
char *
MongoOperatorName(const char *operatorName)
{
	const char *mongoOperatorName = NULL;
	const int32 nameCount = 14;
	static const char *nameMappings[][2] = {{"<", "$lt"},
	{">", "$gt"},
	{"<=", "$lte"},
	{">=", "$gte"},
	{"<>", "$ne"},
	{"=", "$eq"},
	{"+", "$add"},
	{"-", "$subtract"},
	{"*", "$multiply"},
	{"/", "$divide"},
	{"%", "$mod"},
	{"^", "$pow"},
	{"|/", "$sqrt"},
	{"@", "$abs"}};
	int32		nameIndex;

	for (nameIndex = 0; nameIndex < nameCount; nameIndex++)
	{
		const char *pgOperatorName = nameMappings[nameIndex][0];

		if (strncmp(pgOperatorName, operatorName, NAMEDATALEN) == 0)
		{
			mongoOperatorName = nameMappings[nameIndex][1];
			break;
		}
	}

	return (char *) mongoOperatorName;
}

/*
 * EqualityOperatorList
 *		Finds the equality (=) operators in the given list, and returns these
 *		operators in a new list.
 */
static List *
EqualityOperatorList(List *operatorList)
{
	List	   *equalityOperatorList = NIL;
	ListCell   *operatorCell;

	foreach(operatorCell, operatorList)
	{
		OpExpr	   *operator = (OpExpr *) lfirst(operatorCell);

		if (strncmp(get_opname(operator->opno), EQUALITY_OPERATOR_NAME,
					NAMEDATALEN) == 0)
			equalityOperatorList = lappend(equalityOperatorList, operator);
	}

	return equalityOperatorList;
}

/*
 * UniqueColumnList
 *		Walks over the given operator list, and extracts the column argument in
 *		each operator.
 *
 * The function then de-duplicates extracted columns, and returns them in a new
 * list.
 */
static List *
UniqueColumnList(List *operatorList)
{
	List	   *uniqueColumnList = NIL;
	ListCell   *operatorCell;

	foreach(operatorCell, operatorList)
	{
		OpExpr	   *operator = (OpExpr *) lfirst(operatorCell);
		List	   *argumentList = operator->args;
		Var		   *column = (Var *) FindArgumentOfType(argumentList, T_Var);

		/* List membership is determined via column's equal() function */
		uniqueColumnList = list_append_unique(uniqueColumnList, column);
	}

	return uniqueColumnList;
}

/*
 * ColumnOperatorList
 *		Finds all expressions that correspond to the given column, and returns
 *		them in a new list.
 */
static List *
ColumnOperatorList(Var *column, List *operatorList)
{
	List	   *columnOperatorList = NIL;
	ListCell   *operatorCell;

	foreach(operatorCell, operatorList)
	{
		OpExpr	   *operator = (OpExpr *) lfirst(operatorCell);
		List	   *argumentList = operator->args;
		Var		   *foundColumn = (Var *) FindArgumentOfType(argumentList,
															 T_Var);

		if (equal(column, foundColumn))
			columnOperatorList = lappend(columnOperatorList, operator);
	}

	return columnOperatorList;
}

static void
AppendParamValue(BSON *queryDocument, const char *keyName, Param *paramNode,
				 ForeignScanState *scanStateNode)
{
	ExprState  *param_expr;
	Datum		param_value;
	bool		isNull;
	ExprContext *econtext;

	if (scanStateNode == NULL)
		return;

	econtext = scanStateNode->ss.ps.ps_ExprContext;

	/* Prepare for parameter expression evaluation */
	param_expr = ExecInitExpr((Expr *) paramNode, (PlanState *) scanStateNode);

	/* Evaluate the parameter expression */
#if PG_VERSION_NUM >= 100000
	param_value = ExecEvalExpr(param_expr, econtext, &isNull);
#else
	param_value = ExecEvalExpr(param_expr, econtext, &isNull, NULL);
#endif

	AppendMongoValue(queryDocument, keyName, param_value, isNull,
					 paramNode->paramtype);
}

/*
 * AppendConstantValue
 *		Appends to the query document the key name and constant value.
 *
 * The function translates the constant value from its PostgreSQL type
 * to its MongoDB equivalent.
 */
void
AppendConstantValue(BSON *queryDocument, const char *keyName, Const *constant)
{
	if (constant->constisnull)
	{
		BsonAppendNull(queryDocument, keyName);
		return;
	}

	AppendMongoValue(queryDocument, keyName, constant->constvalue, false,
					 constant->consttype);
}

bool
AppendMongoValue(BSON *queryDocument, const char *keyName, Datum value,
				 bool isnull, Oid id)
{
	bool		status = false;

	if (isnull)
	{
		status = BsonAppendNull(queryDocument, keyName);
		return status;
	}

	switch (id)
	{
		case INT2OID:
			{
				int16		valueInt = DatumGetInt16(value);

				status = BsonAppendInt32(queryDocument, keyName,
										 (int) valueInt);
			}
			break;
		case INT4OID:
			{
				int32		valueInt = DatumGetInt32(value);

				status = BsonAppendInt32(queryDocument, keyName, valueInt);
			}
			break;
		case INT8OID:
			{
				int64		valueLong = DatumGetInt64(value);

				status = BsonAppendInt64(queryDocument, keyName, valueLong);
			}
			break;
		case FLOAT4OID:
			{
				float4		valueFloat = DatumGetFloat4(value);

				status = BsonAppendDouble(queryDocument, keyName,
										  (double) valueFloat);
			}
			break;
		case FLOAT8OID:
			{
				float8		valueFloat = DatumGetFloat8(value);

				status = BsonAppendDouble(queryDocument, keyName, valueFloat);
			}
			break;
		case NUMERICOID:
			{
				Datum		valueDatum = DirectFunctionCall1(numeric_float8,
															 value);
				float8		valueFloat = DatumGetFloat8(valueDatum);

				status = BsonAppendDouble(queryDocument, keyName, valueFloat);
			}
			break;
		case BOOLOID:
			{
				bool		valueBool = DatumGetBool(value);

				status = BsonAppendBool(queryDocument, keyName,
										(int) valueBool);
			}
			break;
		case BPCHAROID:
		case VARCHAROID:
		case TEXTOID:
			{
				char	   *outputString;
				Oid			outputFunctionId;
				bool		typeVarLength;

				getTypeOutputInfo(id, &outputFunctionId, &typeVarLength);
				outputString = OidOutputFunctionCall(outputFunctionId, value);
				status = BsonAppendUTF8(queryDocument, keyName, outputString);
			}
			break;
		case BYTEAOID:
			{
				int			len;
				char	   *data;
				char	   *result = DatumGetPointer(value);

				if (VARATT_IS_1B(result))
				{
					len = VARSIZE_1B(result) - VARHDRSZ_SHORT;
					data = VARDATA_1B(result);
				}
				else
				{
					len = VARSIZE_4B(result) - VARHDRSZ;
					data = VARDATA_4B(result);
				}
#ifdef META_DRIVER
				if (strcmp(keyName, "_id") == 0)
				{
					bson_oid_t	oid;

					bson_oid_init_from_data(&oid, (const uint8_t *) data);
					status = BsonAppendOid(queryDocument, keyName, &oid);
				}
				else
					status = BsonAppendBinary(queryDocument, keyName, data,
											  len);
#else
				status = BsonAppendBinary(queryDocument, keyName, data, len);
#endif
			}
			break;
		case NAMEOID:
			{
				char	   *outputString;
				Oid			outputFunctionId;
				bool		typeVarLength;
				bson_oid_t	bsonObjectId;

				memset(bsonObjectId.bytes, 0, sizeof(bsonObjectId.bytes));
				getTypeOutputInfo(id, &outputFunctionId, &typeVarLength);
				outputString = OidOutputFunctionCall(outputFunctionId, value);
				BsonOidFromString(&bsonObjectId, outputString);
				status = BsonAppendOid(queryDocument, keyName, &bsonObjectId);
			}
			break;
		case DATEOID:
			{
				Datum		valueDatum = DirectFunctionCall1(date_timestamp,
															 value);
				Timestamp	valueTimestamp = DatumGetTimestamp(valueDatum);
				int64		valueMicroSecs = valueTimestamp + POSTGRES_TO_UNIX_EPOCH_USECS;
				int64		valueMilliSecs = valueMicroSecs / 1000;

				status = BsonAppendDate(queryDocument, keyName,
										valueMilliSecs);
			}
			break;
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			{
				Timestamp	valueTimestamp = DatumGetTimestamp(value);
				int64		valueMicroSecs = valueTimestamp + POSTGRES_TO_UNIX_EPOCH_USECS;
				int64		valueMilliSecs = valueMicroSecs / 1000;

				status = BsonAppendDate(queryDocument, keyName,
										valueMilliSecs);
			}
			break;
		case NUMERICARRAY_OID:
			{
				ArrayType  *array;
				Oid			elmtype;
				int16		elmlen;
				bool		elmbyval;
				char		elmalign;
				int			num_elems;
				Datum	   *elem_values;
				bool	   *elem_nulls;
				int			i;
				BSON		childDocument;

				array = DatumGetArrayTypeP(value);
				elmtype = ARR_ELEMTYPE(array);
				get_typlenbyvalalign(elmtype, &elmlen, &elmbyval, &elmalign);

				deconstruct_array(array, elmtype, elmlen, elmbyval, elmalign,
								  &elem_values, &elem_nulls, &num_elems);

				BsonAppendStartArray(queryDocument, keyName, &childDocument);
				for (i = 0; i < num_elems; i++)
				{
					Datum		valueDatum;
					float8		valueFloat;

					if (elem_nulls[i])
						continue;

					valueDatum = DirectFunctionCall1(numeric_float8,
													 elem_values[i]);
					valueFloat = DatumGetFloat8(valueDatum);
#ifdef META_DRIVER
					status = BsonAppendDouble(&childDocument, keyName,
											  valueFloat);
#else
					status = BsonAppendDouble(queryDocument, keyName,
											  valueFloat);
#endif
				}
				BsonAppendFinishArray(queryDocument, &childDocument);
				pfree(elem_values);
				pfree(elem_nulls);
			}
			break;
		case TEXTARRAYOID:
			{
				ArrayType  *array;
				Oid			elmtype;
				int16		elmlen;
				bool		elmbyval;
				char		elmalign;
				int			num_elems;
				Datum	   *elem_values;
				bool	   *elem_nulls;
				int			i;
				BSON		childDocument;

				array = DatumGetArrayTypeP(value);
				elmtype = ARR_ELEMTYPE(array);
				get_typlenbyvalalign(elmtype, &elmlen, &elmbyval, &elmalign);

				deconstruct_array(array, elmtype, elmlen, elmbyval, elmalign,
								  &elem_values, &elem_nulls, &num_elems);

				BsonAppendStartArray(queryDocument, keyName, &childDocument);
				for (i = 0; i < num_elems; i++)
				{
					char	   *valueString;
					Oid			outputFunctionId;
					bool		typeVarLength;

					if (elem_nulls[i])
						continue;

					getTypeOutputInfo(TEXTOID, &outputFunctionId,
									  &typeVarLength);
					valueString = OidOutputFunctionCall(outputFunctionId,
														elem_values[i]);
					status = BsonAppendUTF8(queryDocument, keyName,
											valueString);
				}
				BsonAppendFinishArray(queryDocument, &childDocument);
				pfree(elem_values);
				pfree(elem_nulls);
			}
			break;
		case JSONOID:
			{
				char	   *outputString;
				Oid			outputFunctionId;
				struct json_object *o;
				bool		typeVarLength;

				getTypeOutputInfo(id, &outputFunctionId, &typeVarLength);
				outputString = OidOutputFunctionCall(outputFunctionId, value);
				o = JsonTokenerPrase(outputString);

				if (o == NULL)
				{
					elog(WARNING, "cannot parse the document");
					status = 0;
					break;
				}

				status = JsonToBsonAppendElement(queryDocument, keyName, o);
			}
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
					 errmsg("cannot convert constant value to BSON value"),
					 errhint("Constant value data type: %u", id)));
			break;
	}

	return status;
}

/*
 * mongo_get_column_list
 *		Process scan_var_list to find all columns needed for query execution
 *		and return them.
 *
 * Also, form two separate lists:
 * 1. column_name_list: column names of needed columns.
 * 2. is_inner_column_list: column is of inner relation or not.
 */
List *
mongo_get_column_list(PlannerInfo *root, RelOptInfo *foreignrel,
					  List *scan_var_list, List **column_name_list,
					  List **is_inner_column_list)
{
	List	   *columnList = NIL;
	ListCell   *lc;

	foreach(lc, scan_var_list)
	{
		Var		   *var = (Var *) lfirst(lc);
		RangeTblEntry *rte = planner_rt_fetch(var->varno, root);
		int		    is_innerrel = false;

		Assert(IsA(var, Var));

		/* Var belongs to foreign table? */
		if (!bms_is_member(var->varno, foreignrel->relids))
			continue;

		/* Is whole-row reference requested? */
		if (var->varattno == 0)
		{
			List	   *wr_var_list;
			RangeTblEntry *rte = rt_fetch(var->varno, root->parse->rtable);
			Bitmapset  *attrs_used;

			Assert(OidIsValid(rte->relid));

			/*
			 * Get list of Var nodes for all undropped attributes of the base
			 * relation.
			 */
			attrs_used = bms_make_singleton(0 -
											FirstLowInvalidHeapAttributeNumber);

			wr_var_list = prepare_var_list_for_baserel(rte->relid, var->varno,
													   attrs_used);
			columnList = list_concat_unique(columnList, wr_var_list);
			bms_free(attrs_used);
		}
		else
			columnList = list_append_unique(columnList, var);

#if PG_VERSION_NUM >= 100000
		if (IS_JOIN_REL(foreignrel))
#else
		if (foreignrel->reloptkind == RELOPT_JOINREL)
#endif
		{
			MongoFdwRelationInfo *fpinfo = (MongoFdwRelationInfo *) foreignrel->fdw_private;
			char	   *columnName;

#if PG_VERSION_NUM < 110000
			columnName = get_relid_attribute_name(rte->relid, var->varattno);
#else
			columnName = get_attname(rte->relid, var->varattno, false);
#endif
			*column_name_list = lappend(*column_name_list,
										makeString(columnName));
			if (bms_is_member(var->varno, fpinfo->innerrel->relids))
				is_innerrel = true;

			*is_inner_column_list = lappend_int(*is_inner_column_list,
												is_innerrel);
		}
	}

	return columnList;
}

/*
 * Check if expression is safe to execute remotely, and return true if so.
 *
 * In addition, *outer_cxt is updated with collation information.
 *
 * We must check that the expression contains only node types we can deparse,
 * that all types/operators are safe to send (which we approximate
 * as being built-in), and that all collations used in the expression derive
 * from Vars of the foreign table.
 *
 * For WHERE clauses, we only support simple binary operators that compare a
 * column against a constant.  If the expression is a tree, we don't recurse
 * into it.
 *
 * For JOIN clauses, in addition to the above support, in the case of operator
 * expression, we do support arithmetic (+, -, *, /, %, ^, @ and |/) operators.
 * Also, both operands of the binary operator can be a column.  If the
 * expression is a tree, we do recurse into it.  Supports Boolean expression as
 * well.
 */
static bool
foreign_expr_walker(Node *node, foreign_glob_cxt *glob_cxt,
					foreign_loc_cxt *outer_cxt)
{
	foreign_loc_cxt inner_cxt;
	Oid			collation;
	FDWCollateState state;

	/* Need do nothing for empty subexpressions */
	if (node == NULL)
		return true;

	/* Set up inner_cxt for possible recursion to child nodes */
	inner_cxt.collation = InvalidOid;
	inner_cxt.state = FDW_COLLATE_NONE;

	switch (nodeTag(node))
	{
		case T_Var:
			{
				Var		   *var = (Var *) node;

				/* Increment the Var count */
				glob_cxt->varcount++;

				/*
				 * If the Var is from the foreign table, we consider its
				 * collation (if any) safe to use.  If it is from another
				 * table, we treat its collation the same way as we would a
				 * Param's collation, i.e. it's not safe for it to have a
				 * non-default collation.
				 */
				if (bms_is_member(var->varno, glob_cxt->relids) &&
					var->varlevelsup == 0)
				{
					/* Var belongs to foreign table */
					collation = var->varcollid;
					state = OidIsValid(collation) ? FDW_COLLATE_SAFE : FDW_COLLATE_NONE;
				}
				else
				{
					/* Var belongs to some other table */
					collation = var->varcollid;
					if (var->varcollid != InvalidOid &&
						var->varcollid != DEFAULT_COLLATION_OID)
						return false;

					if (collation == InvalidOid ||
						collation == DEFAULT_COLLATION_OID)
					{
						/*
						 * It's noncollatable, or it's safe to combine with a
						 * collatable foreign Var, so set state to NONE.
						 */
						state = FDW_COLLATE_NONE;
					}
					else
					{
						/*
						 * Do not fail right away, since the Var might appear
						 * in a collation-insensitive context.
						 */
						state = FDW_COLLATE_UNSAFE;
					}
				}
			}
			break;
		case T_Const:
			{
				Const	   *c = (Const *) node;

				/*
				 * We don't push down operators where the constant is an array,
				 * since conditional operators for arrays in MongoDB aren't
				 * properly defined.
				 */
				if (OidIsValid(get_element_type(c->consttype)))
					return false;

				/*
				 * If the constant has nondefault collation, either it's of a
				 * non-builtin type, or it reflects folding of a CollateExpr.
				 * It's unsafe to send to the remote unless it's used in a
				 * non-collation-sensitive context.
				 */
				collation = c->constcollid;
				if (collation == InvalidOid ||
					collation == DEFAULT_COLLATION_OID)
					state = FDW_COLLATE_NONE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
		case T_Param:
			{
				Param	   *p = (Param *) node;

				/*
				 * Bail out on planner internal params. We could perhaps pass
				 * them to the remote server as regular params, but we don't
				 * have the machinery to do that at the moment.
				 */
				if (p->paramkind != PARAM_EXTERN)
					return false;

				/*
				 * Collation rule is same as for Consts and non-foreign Vars.
				 */
				collation = p->paramcollid;
				if (collation == InvalidOid ||
					collation == DEFAULT_COLLATION_OID)
					state = FDW_COLLATE_NONE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
		case T_OpExpr:
			{
				OpExpr	   *oe = (OpExpr *) node;
				char	   *oname = get_opname(oe->opno);

				/* Increment the operator expression count */
				glob_cxt->opexprcount++;

				/*
				 * We support =, <, >, <=, >=, <>, +, -, *, /, %, ^, |/, and @
				 * operators for joinclause of join relation.
				 */
				if (!(strncmp(oname, EQUALITY_OPERATOR_NAME, NAMEDATALEN) == 0) &&
					(MongoOperatorName(oname) == NULL))
					return false;

				/*
				 * Recurse to input subexpressions.
				 *
				 * We support only =, <, >, <=, >= and <> operators for WHERE
				 * conditions of simple as well as join relation.
				 */
				if (!foreign_expr_walker((Node *) oe->args, glob_cxt,
										 &inner_cxt) ||
					(!glob_cxt->is_join_cond && glob_cxt->opexprcount > 1))
					return false;

				/*
				 * If operator's input collation is not derived from a foreign
				 * Var, it can't be sent to remote.
				 */
				if (oe->inputcollid == InvalidOid)
					 /* OK, inputs are all noncollatable */ ;
				else if (inner_cxt.state != FDW_COLLATE_SAFE ||
						 oe->inputcollid != inner_cxt.collation)
					return false;

				/* Result-collation handling */
				collation = oe->opcollid;
				if (collation == InvalidOid)
					state = FDW_COLLATE_NONE;
				else if (inner_cxt.state == FDW_COLLATE_SAFE &&
						 collation == inner_cxt.collation)
					state = FDW_COLLATE_SAFE;
				else if (collation == DEFAULT_COLLATION_OID)
					state = FDW_COLLATE_NONE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
		case T_RelabelType:
			{
				RelabelType *r = (RelabelType *) node;

				/*
				 * Recurse to input subexpression.
				 */
				if (!foreign_expr_walker((Node *) r->arg,
										 glob_cxt, &inner_cxt))
					return false;

				/*
				 * RelabelType must not introduce a collation not derived from
				 * an input foreign Var (same logic as for a real function).
				 */
				collation = r->resultcollid;
				if (collation == InvalidOid)
					state = FDW_COLLATE_NONE;
				else if (inner_cxt.state == FDW_COLLATE_SAFE &&
						 collation == inner_cxt.collation)
					state = FDW_COLLATE_SAFE;
				else if (collation == DEFAULT_COLLATION_OID)
					state = FDW_COLLATE_NONE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
		case T_List:
			{
				List	   *l = (List *) node;
				ListCell   *lc;

				/*
				 * Recurse to component subexpressions.
				 *
				 * For simple relation, if the comparison is between two
				 * columns of the same table, then we don't push down because
				 * building corresponding MongoDB query is not possible with
				 * the cirrent MongoC driver.
				 */
				foreach(lc, l)
				{
					if ((!foreign_expr_walker((Node *) lfirst(lc),
											  glob_cxt, &inner_cxt)) ||
						(!(glob_cxt->is_join_cond) && glob_cxt->varcount > 1))
						return false;
				}

				/*
				 * When processing a list, collation state just bubbles up
				 * from the list elements.
				 */
				collation = inner_cxt.collation;
				state = inner_cxt.state;
			}
			break;
		case T_BoolExpr:
			{
				BoolExpr   *b = (BoolExpr *) node;

				/*
				 * Recurse to input sub-expressions.
				 */
				if (!foreign_expr_walker((Node *) b->args,
										 glob_cxt, &inner_cxt))
					return false;

				/* Output is always boolean and so noncollatable. */
				collation = InvalidOid;
				state = FDW_COLLATE_NONE;
			}
			break;
		default:

			/*
			 * If it's anything else, assume it's unsafe.  This list can be
			 * expanded later, but don't forget to add deparse support.
			 */
			return false;
	}

	/*
	 * Now, merge my collation information into my parent's state.
	 */
	if (state > outer_cxt->state)
	{
		/* Override previous parent state */
		outer_cxt->collation = collation;
		outer_cxt->state = state;
	}
	else if (state == outer_cxt->state)
	{
		/* Merge, or detect error if there's a collation conflict */
		switch (state)
		{
			case FDW_COLLATE_NONE:
				/* Nothing + nothing is still nothing */
				break;
			case FDW_COLLATE_SAFE:
				if (collation != outer_cxt->collation)
				{
					/*
					 * Non-default collation always beats default.
					 */
					if (outer_cxt->collation == DEFAULT_COLLATION_OID)
					{
						/* Override previous parent state */
						outer_cxt->collation = collation;
					}
					else if (collation != DEFAULT_COLLATION_OID)
					{
						/*
						 * Conflict; show state as indeterminate.  We don't
						 * want to "return false" right away, since parent
						 * node might not care about collation.
						 */
						outer_cxt->state = FDW_COLLATE_UNSAFE;
					}
				}
				break;
			case FDW_COLLATE_UNSAFE:
				/* We're still conflicted ... */
				break;
		}
	}

	/* It looks OK */
	return true;
}

/*
 * mongo_is_foreign_expr
 *		Returns true if given expr is safe to evaluate on the foreign server.
 */
bool
mongo_is_foreign_expr(PlannerInfo *root, RelOptInfo *baserel, Expr *expression,
					  bool is_join_cond)
{
	foreign_glob_cxt glob_cxt;
	foreign_loc_cxt loc_cxt;

	/*
	 * Check that the expression consists of nodes that are safe to execute
	 * remotely.
	 */
	glob_cxt.root = root;
	glob_cxt.foreignrel = baserel;
	glob_cxt.relids = baserel->relids;
	glob_cxt.varcount = 0;
	glob_cxt.opexprcount = 0;
	glob_cxt.is_join_cond = is_join_cond;
	loc_cxt.collation = InvalidOid;
	loc_cxt.state = FDW_COLLATE_NONE;
	if (!foreign_expr_walker((Node *) expression, &glob_cxt, &loc_cxt))
		return false;

	/* Expressions examined here should be boolean, i.e. noncollatable */
	Assert(loc_cxt.collation == InvalidOid);
	Assert(loc_cxt.state == FDW_COLLATE_NONE);

	/* OK to evaluate on the remote server */
	return true;
}

/*
 * prepare_var_list_for_baserel
 *		Build list of nodes corresponding to the attributes requested for given
 *		base relation.
 *
 * The list contains Var nodes corresponding to the attributes specified in
 * attrs_used. If whole-row reference is required, add Var nodes corresponding
 * to all the attributes in the relation.
 */
static List *
prepare_var_list_for_baserel(Oid relid, Index varno, Bitmapset *attrs_used)
{
	int			attno;
	List	   *tlist = NIL;
	Node	   *node;
	bool		wholerow_requested = false;
	Relation	relation;
	TupleDesc	tupdesc;

	Assert(OidIsValid(relid));

	/* Planner must have taken a lock, so request no lock here */
#if PG_VERSION_NUM < 130000
	relation = heap_open(relid, NoLock);
#else
	relation = table_open(relid, NoLock);
#endif

	tupdesc = RelationGetDescr(relation);

	/* Is whole-row reference requested? */
	wholerow_requested = bms_is_member(0 - FirstLowInvalidHeapAttributeNumber,
									   attrs_used);

	/* Handle user defined attributes first. */
	for (attno = 1; attno <= tupdesc->natts; attno++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, attno - 1);

		/* Ignore dropped attributes. */
		if (attr->attisdropped)
			continue;

		/* For a required attribute create a Var node */
		if (wholerow_requested ||
			bms_is_member(attno - FirstLowInvalidHeapAttributeNumber,
						  attrs_used))
		{
			node = (Node *) makeVar(varno, attno, attr->atttypid,
									attr->atttypmod, attr->attcollation, 0);
			tlist = lappend(tlist, node);

		}
	}

#if PG_VERSION_NUM < 130000
	heap_close(relation, NoLock);
#else
	table_close(relation, NoLock);
#endif

	return tlist;
}

/*
 * ColumnInfoHash
 *		Creates a hash table that maps varno and varattno to the column names,
 *		and also stores whether the column is part of outer relation or not.
 *
 * This table helps us to form the pipeline quickly.
 */
static HTAB *
ColumnInfoHash(List *colname_list, List *colnum_list, List *rti_list,
			   List *isouter_list)
{
	HTAB	   *columnInfoHash;
	ColInfoHashKey key;
	HASHCTL		hashInfo;
	ListCell   *l1;
	ListCell   *l2;
	ListCell   *l3;
	ListCell   *l4;

	memset(&hashInfo, 0, sizeof(hashInfo));
	hashInfo.keysize = sizeof(ColInfoHashKey );
	hashInfo.entrysize = sizeof(ColInfoHashEntry);
	hashInfo.hcxt = CurrentMemoryContext;

	columnInfoHash = hash_create("Column Information Hash", MaxHashTableSize,
								 &hashInfo,
								 (HASH_ELEM | HASH_BLOBS | HASH_CONTEXT));
	Assert(columnInfoHash != NULL);

	/*
	 * There's no forfour() in version 11 and below, so need to traverse one
	 * list the hard way.
	 */
	l4 = list_head(isouter_list);
	forthree(l1, colname_list, l2, colnum_list, l3, rti_list)
	{
		ColInfoHashEntry *columnInfo;
		char	   *columnName = strVal(lfirst(l1));
		int		    columnNum = lfirst_int(l2);
		int		    varNo = lfirst_int(l3);
		bool		isOuter = lfirst_int(l4);

		key.varNo = varNo;
		key.varAttno = columnNum;

		columnInfo = (ColInfoHashEntry *) hash_search(columnInfoHash,
													  (void *)&key,
													  HASH_ENTER,
													  NULL);
		Assert(columnInfo != NULL);

		columnInfo->colName = columnName;
		columnInfo->isOuter = isOuter;

#if PG_VERSION_NUM >= 130000
		l4 = lnext(isouter_list, l4);
#else
		l4 = lnext(l4);
#endif
	}

	return columnInfoHash;
}

/*
 * mongo_prepare_inner_pipeline
 *		Form inner query pipeline syntax equivalent to postgresql join clauses.
 *
 * From the example given on QueryDocument, the following part of MongoDB query
 * formed by this function:
 *
 *          "pipeline": [
 *            {
 *              "$match":
 *              {
 *                "$expr":
 *                {
 *                  "$and": [
 *                    { "$eq": [ "$$v_age", "$old" ] }
 *                    { "$ne": [ "$$v_age", null ] },
 *                    { "$ne": [ "$old", null ] },
 *                  ]
 *                }
 *              }
 *            }
 *          ]
 */
static void
mongo_prepare_inner_pipeline(List *joinclause, BSON *inner_pipeline,
							 List *colname_list, List *isouter_list,
							 pipeline_cxt *context)
{
	BSON	   *and_query_doc = BsonCreate();
	BSON	    match_object;
	BSON	    match_stage;
	BSON	    expr;
	BSON	    and_op;
	int 	    inner_pipeline_index = 0;

	BsonAppendStartObject(inner_pipeline,
						  psprintf("%d", inner_pipeline_index++),
						  &match_object);
	BsonAppendStartObject(&match_object, "$match", &match_stage);
	BsonAppendStartObject(&match_stage, "$expr", &expr);

	BsonAppendStartArray(and_query_doc, "$and", &and_op);

	context->arrayIndex = 0;

	/* Append join clause expression */
	mongo_append_joinclauses_to_inner_pipeline(joinclause, &and_op, context);

	/* Append $and array to $expr */
	bson_append_array(&expr, "$and", (int) strlen ("$and"), &and_op);

	BsonAppendFinishArray(and_query_doc, &and_op);
	BsonAppendFinishObject(&match_stage, &expr);
	BsonAppendFinishObject(&match_object, &match_stage);
	BsonAppendFinishObject(inner_pipeline, &match_object);
}

/*
 * mongo_append_joinclauses_to_inner_pipeline
 *		Append all join expressions to mongoDB's $and array.
 */
static void
mongo_append_joinclauses_to_inner_pipeline(List *joinclause, BSON *child_doc,
										   pipeline_cxt *context)
{
	ListCell   *lc;

	/* loop through all join-clauses */
	foreach(lc, joinclause)
	{
		Expr	   *expr = (Expr *) lfirst(lc);

		/* Extract clause from RestrictInfo */
		if (IsA(expr, RestrictInfo))
		{
			RestrictInfo *ri = (RestrictInfo *) expr;

			expr = ri->clause;
		}

		mongo_append_expr(expr, child_doc, context);
		context->arrayIndex++;
	}
}
