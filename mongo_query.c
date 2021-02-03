/*-------------------------------------------------------------------------
 *
 * mongo_query.c
 * 		FDW query handling for mongo_fdw
 *
 * Portions Copyright (c) 2012-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 2004-2020, EnterpriseDB Corporation.
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

#include "catalog/pg_collation.h"
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

/*
 * Global context for foreign_expr_walker's search of an expression tree.
 */
typedef struct foreign_glob_cxt
{
	PlannerInfo *root;			/* global planner state */
	RelOptInfo *foreignrel;		/* the foreign relation we are planning for */
	unsigned short varcount;	/* Var count */
	unsigned short opexprcount;
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
static void AppendConstantValue(BSON *queryDocument, const char *keyName,
								Const *constant);
static void AppendParamValue(BSON *queryDocument, const char *keyName,
							 Param *paramNode,
							 ForeignScanState *scanStateNode);
static bool foreign_expr_walker(Node *node,
								foreign_glob_cxt *glob_cxt,
								foreign_loc_cxt *outer_cxt);

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
 *		Takes in the applicable operator expressions for a relation and
 *		converts these expressions into equivalent queries in MongoDB.
 *
 * For now, this function can only transform simple comparison expressions, and
 * returns these transformed expressions in a BSON document.  For example,
 * simple expressions:
 * "l_shipdate >= date '1994-01-01' AND l_shipdate < date '1995-01-01'" become
 * "l_shipdate: { $gte: new Date(757382400000), $lt: new Date(788918400000) }".
 */
BSON *
QueryDocument(Oid relationId, List *opExpressionList,
			  ForeignScanState *scanStateNode)
{
	List	   *equalityOperatorList;
	List	   *comparisonOperatorList;
	List	   *columnList;
	ListCell   *equalityOperatorCell;
	ListCell   *columnCell;
	BSON	   *queryDocument = BsonCreate();

	/*
	 * We distinguish between equality expressions and others since we need to
	 * insert the latter (<, >, <=, >=, <>) as separate sub-documents into the
	 * BSON query object.
	 */
	equalityOperatorList = EqualityOperatorList(opExpressionList);
	comparisonOperatorList = list_difference(opExpressionList,
											 equalityOperatorList);

	/* Append equality expressions to the query */
	foreach(equalityOperatorCell, equalityOperatorList)
	{
		OpExpr	   *equalityOperator = (OpExpr *) lfirst(equalityOperatorCell);
		Oid			columnId = InvalidOid;
		char	   *columnName;
		Const	   *constant;
		Param	   *paramNode;
		List	   *argumentList = equalityOperator->args;
		Var		   *column = (Var *) FindArgumentOfType(argumentList, T_Var);

		constant = (Const *) FindArgumentOfType(argumentList, T_Const);
		paramNode = (Param *) FindArgumentOfType(argumentList, T_Param);

		columnId = column->varattno;
#if PG_VERSION_NUM < 110000
		columnName = get_relid_attribute_name(relationId, columnId);
#else
		columnName = get_attname(relationId, columnId, false);
#endif

		if (constant != NULL)
			AppendConstantValue(queryDocument, columnName, constant);
		else
			AppendParamValue(queryDocument, columnName, paramNode,
							 scanStateNode);
	}

	/*
	 * For comparison expressions, we need to group them by their columns and
	 * append all expressions that correspond to a column as one sub-document.
	 *
	 * Otherwise, even when we have two expressions to define the upper- and
	 * lower-bound of a range, Mongo uses only one of these expressions during
	 * an index search.
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

		columnId = column->varattno;
#if PG_VERSION_NUM < 110000
		columnName = get_relid_attribute_name(relationId, columnId);
#else
		columnName = get_attname(relationId, columnId, false);
#endif

		/* Find all expressions that correspond to the column */
		columnOperatorList = ColumnOperatorList(column,
												comparisonOperatorList);

		/* For comparison expressions, start a sub-document */
		BsonAppendStartObject(queryDocument, columnName, &childDocument);

		foreach(columnOperatorCell, columnOperatorList)
		{
			OpExpr	   *columnOperator = (OpExpr *) lfirst(columnOperatorCell);
			char	   *operatorName;
			char	   *mongoOperatorName;
			List	   *argumentList = columnOperator->args;
			Const	   *constant = (Const *) FindArgumentOfType(argumentList,
																T_Const);

			operatorName = get_opname(columnOperator->opno);
			mongoOperatorName = MongoOperatorName(operatorName);
#ifdef META_DRIVER
			AppendConstantValue(&childDocument, mongoOperatorName, constant);
#else
			AppendConstantValue(queryDocument, mongoOperatorName, constant);
#endif
		}
		BsonAppendFinishObject(queryDocument, &childDocument);
	}

	if (!BsonFinish(queryDocument))
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
	const int32 nameCount = 5;
	static const char *nameMappings[][2] = {{"<", "$lt"},
	{">", "$gt"},
	{"<=", "$lte"},
	{">=", "$gte"},
	{"<>", "$ne"}};
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
static void
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
 * ColumnList
 *		Takes in the planner's information about this foreign table.  The
 *		function then finds all columns needed for query execution, including
 *		those used in projections, joins, and filter clauses, de-duplicates
 *		these columns, and returns them in a new list.
 */
List *
ColumnList(RelOptInfo *baserel)
{
	List	   *columnList = NIL;
	List	   *neededColumnList;
	AttrNumber	columnIndex;
	AttrNumber	columnCount = baserel->max_attr;

#if PG_VERSION_NUM >= 90600
	List	   *targetColumnList = baserel->reltarget->exprs;
#else
	List	   *targetColumnList = baserel->reltargetlist;
#endif
	List	   *restrictInfoList = baserel->baserestrictinfo;
	ListCell   *restrictInfoCell;

	/* First add the columns used in joins and projections */
	neededColumnList = pull_var_clause((Node *)targetColumnList,
#if PG_VERSION_NUM < 90600
									   PVC_RECURSE_AGGREGATES,
#endif
									   PVC_RECURSE_PLACEHOLDERS);

	/* Then walk over all restriction clauses, and pull up any used columns */
	foreach(restrictInfoCell, restrictInfoList)
	{
		RestrictInfo *restrictInfo = (RestrictInfo *) lfirst(restrictInfoCell);
		Node	   *restrictClause = (Node *) restrictInfo->clause;
		List	   *clauseColumnList = NIL;

		/* Recursively pull up any columns used in the restriction clause */
		clauseColumnList = pull_var_clause(restrictClause,
#if PG_VERSION_NUM < 90600
										   PVC_RECURSE_AGGREGATES,
#endif
										   PVC_RECURSE_PLACEHOLDERS);

		neededColumnList = list_union(neededColumnList, clauseColumnList);
	}

	/* Walk over all column definitions, and de-duplicate column list */
	for (columnIndex = 1; columnIndex <= columnCount; columnIndex++)
	{
		ListCell   *neededColumnCell;
		Var		   *column = NULL;

		/* Look for this column in the needed column list */
		foreach(neededColumnCell, neededColumnList)
		{
			Var		   *neededColumn = (Var *) lfirst(neededColumnCell);

			if (neededColumn->varattno == columnIndex)
			{
				column = neededColumn;
				break;
			}
		}

		if (column != NULL)
			columnList = lappend(columnList, column);
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
 * We only support simple binary operators that compare a column against a
 * constant.  If the expression is a tree, we don't recurse into it.
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
				if (var->varno == glob_cxt->foreignrel->relid &&
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

				/* We only support =, <, >, <=, >=, and <> operators */
				if (!(strncmp(oname, EQUALITY_OPERATOR_NAME, NAMEDATALEN) == 0) &&
					(MongoOperatorName(oname) == NULL))
					return false;

				/*
				 * Recurse to input subexpressions.
				 */
				if (glob_cxt->opexprcount > 1 ||
					!foreign_expr_walker((Node *) oe->args,
										 glob_cxt, &inner_cxt))
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
				 * If comparison is between two columns of same table then we
				 * don't push down because currently building corresponding
				 * MongoDB query not possible with the help of MongoC driver.
				 */
				foreach(lc, l)
				{
					if ((!foreign_expr_walker((Node *) lfirst(lc),
											  glob_cxt, &inner_cxt)) ||
						glob_cxt->varcount > 1)
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
mongo_is_foreign_expr(PlannerInfo *root, RelOptInfo *baserel, Expr *expression)
{
	foreign_glob_cxt glob_cxt;
	foreign_loc_cxt loc_cxt;

	/*
	 * Check that the expression consists of nodes that are safe to execute
	 * remotely.
	 */
	glob_cxt.root = root;
	glob_cxt.foreignrel = baserel;
	glob_cxt.varcount = 0;
	glob_cxt.opexprcount = 0;
	loc_cxt.collation = InvalidOid;
	loc_cxt.state = FDW_COLLATE_NONE;
	if (!foreign_expr_walker((Node *) expression, &glob_cxt, &loc_cxt))
		return false;

	/* Expressions examined here should be boolean, ie noncollatable */
	Assert(loc_cxt.collation == InvalidOid);
	Assert(loc_cxt.state == FDW_COLLATE_NONE);

	/* OK to evaluate on the remote server */
	return true;
}
