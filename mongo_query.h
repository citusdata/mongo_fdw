/*-------------------------------------------------------------------------
 *
 * mongo_query.h
 * 		Foreign-data wrapper for remote MongoDB servers
 *
 * Portions Copyright (c) 2012-2014, PostgreSQL Global Development Group
 *
 * Portions Copyright (c) 2004-2014, EnterpriseDB Corporation.
 *
 * Portions Copyright (c) 2012–2014 Citus Data, Inc.
 *
 * IDENTIFICATION
 * 		mongo_query.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef MONGO_QUERY_H
#define MONGO_QUERY_H


#define NUMERICARRAY_OID 1231

bool AppenMongoValue(BSON *queryDocument, const char *keyName, Datum	value, bool isnull, Oid id);

#endif /* MONGO_QUERY_H */
