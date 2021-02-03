/*-------------------------------------------------------------------------
 *
 * mongo_query.h
 * 		FDW query handling for mongo_fdw
 *
 * Portions Copyright (c) 2012-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 2004-2020, EnterpriseDB Corporation.
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

bool AppendMongoValue(BSON *queryDocument,
					  const char *keyName,
					  Datum value,
					  bool isnull,
					  Oid id);

char *MongoOperatorName(const char *operatorName);

#endif							/* MONGO_QUERY_H */
