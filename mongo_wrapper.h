/*-------------------------------------------------------------------------
 *
 * mongo_wrapper.h
 * 		Wrapper functions for remote MongoDB servers
 *
 * Portions Copyright (c) 2012-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 2004-2020, EnterpriseDB Corporation.
 * Portions Copyright (c) 2012–2014 Citus Data, Inc.
 *
 * IDENTIFICATION
 * 		mongo_wrapper.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MONGO_WRAPPER_H
#define MONGO_WRAPPER_H

#include "mongo_fdw.h"

#ifdef META_DRIVER
#include "mongoc.h"
#else
#include "mongo.h"
#endif
#define json_object json_object_tmp

#include <json.h>
#include <bits.h>

#ifdef META_DRIVER
MONGO_CONN *MongoConnect(MongoFdwOptions *opt);
#else
MONGO_CONN *MongoConnect(MongoFdwOptions *opt);
#endif
void MongoDisconnect(MONGO_CONN *conn);
bool MongoInsert(MONGO_CONN *conn, char *database, char *collection, BSON *b);
bool MongoUpdate(MONGO_CONN *conn, char *database, char *collection, BSON *b,
				 BSON *op);
bool MongoDelete(MONGO_CONN *conn, char *database, char *collection,
				 BSON *b);
MONGO_CURSOR *MongoCursorCreate(MONGO_CONN *conn, char *database,
								char *collection, BSON *q);
const BSON *MongoCursorBson(MONGO_CURSOR *c);
bool MongoCursorNext(MONGO_CURSOR *c, BSON *b);
void MongoCursorDestroy(MONGO_CURSOR *c);
double MongoAggregateCount(MONGO_CONN *conn, const char *database,
						   const char *collection, const BSON *b);

BSON *BsonCreate(void);
void BsonDestroy(BSON *b);

bool BsonIterInit(BSON_ITERATOR *it, BSON *b);
bool BsonIterSubObject(BSON_ITERATOR *it, BSON *b);
int32_t	BsonIterInt32(BSON_ITERATOR *it);
int64_t	BsonIterInt64(BSON_ITERATOR *it);
double BsonIterDouble(BSON_ITERATOR *it);
bool BsonIterBool(BSON_ITERATOR *it);
const char *BsonIterString(BSON_ITERATOR *it);
#ifdef META_DRIVER
const char *BsonIterBinData(BSON_ITERATOR *it, uint32_t *len);
#else
const char *BsonIterBinData(BSON_ITERATOR *it);
int	BsonIterBinLen(BSON_ITERATOR *it);
#endif
#ifdef META_DRIVER
const bson_oid_t *BsonIterOid(BSON_ITERATOR *it);
#else
bson_oid_t *BsonIterOid(BSON_ITERATOR *it);
#endif
time_t BsonIterDate(BSON_ITERATOR *it);
int	BsonIterType(BSON_ITERATOR *it);
int	BsonIterNext(BSON_ITERATOR *it);
bool BsonIterSubIter(BSON_ITERATOR *it, BSON_ITERATOR *sub);
void BsonOidFromString(bson_oid_t *o, char *str);
void BsonOidToString(const bson_oid_t *o, char str[25]);
const char *BsonIterCode(BSON_ITERATOR *i);
const char *BsonIterRegex(BSON_ITERATOR *i);
const char *BsonIterKey(BSON_ITERATOR *i);
#ifdef META_DRIVER
const bson_value_t *BsonIterValue(BSON_ITERATOR *i);
#else
const char *BsonIterValue(BSON_ITERATOR *i);
#endif

void BsonIteratorFromBuffer(BSON_ITERATOR *i, const char *buffer);

BSON *BsonCreate();
bool BsonAppendOid(BSON *b, const char *key, bson_oid_t *v);
bool BsonAppendBool(BSON *b, const char *key, bool v);
bool BsonAppendNull(BSON *b, const char *key);
bool BsonAppendInt32(BSON *b, const char *key, int v);
bool BsonAppendInt64(BSON *b, const char *key, int64_t v);
bool BsonAppendDouble(BSON *b, const char *key, double v);
bool BsonAppendUTF8(BSON *b, const char *key, char *v);
bool BsonAppendBinary(BSON *b, const char *key, char *v, size_t len);
bool BsonAppendDate(BSON *b, const char *key, time_t v);
bool BsonAppendStartArray(BSON *b, const char *key, BSON *c);
bool BsonAppendFinishArray(BSON *b, BSON *c);
bool BsonAppendStartObject(BSON *b, char *key, BSON *r);
bool BsonAppendFinishObject(BSON *b, BSON *r);
bool BsonAppendBson(BSON *b, char *key, BSON *c);
bool BsonFinish(BSON *b);
bool JsonToBsonAppendElement(BSON *bb, const char *k, struct json_object *v);
json_object *JsonTokenerPrase(char *s);

char *BsonAsJson(const BSON *bsonDocument);

void BsonToJsonStringValue(StringInfo output, BSON_ITERATOR *iter,
						   bool isArray);
void DumpJsonObject(StringInfo output, BSON_ITERATOR *iter);
void DumpJsonArray(StringInfo output, BSON_ITERATOR *iter);


#endif					/* MONGO_QUERY_H */
