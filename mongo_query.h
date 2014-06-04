/*-------------------------------------------------------------------------
 *
 * mongo_query.h
 *
 * Type and function declarations for constructing queries to send to MongoDB.
 *
 * Portions Copyright © 2004-2014, EnterpriseDB Corporation.
 *
 * Portions Copyright © 2012–2014 Citus Data, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */

#ifndef MONGO_QUERY_H
#define MONGO_QUERY_H


#define NUMERICARRAY_OID 1231

bool AppenMongoValue(BSON *queryDocument, const char *keyName, Datum	value, bool isnull, Oid id);

#endif /* MONGO_QUERY_H */
