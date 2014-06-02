/*-------------------------------------------------------------------------
 *
 * mongo_wrapper.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef MONGO_WRAPPER_H
#define MONGO_WRAPPER_H

#include "mongo_fdw.h"
#include "bson.h"

#ifdef META_DRIVER
	#include "mongoc.h"
#else
	#include "mongo.h"
#endif

MONGO_CONN* MongoConnect(const char* host, const unsigned short port);
void MongoDisconnect(MONGO_CONN* conn);
bool MongoInsert(MONGO_CONN* conn, char* database, char *collection, BSON* b);
bool MongoUpdate(MONGO_CONN* conn, char* database, char *collection, BSON* b, BSON* op);
bool MongoDelete(MONGO_CONN* conn, char* database, char *collection, BSON* b);
MONGO_CURSOR* MongoCursorCreate(MONGO_CONN* conn, char* database, char *collection, BSON* q);
const BSON* MongoCursorBson(MONGO_CURSOR* c);
bool MongoCursorNext(MONGO_CURSOR* c, BSON* b);
void MongoCursorDestroy(MONGO_CURSOR* c);
int MongoAggregateCount(MONGO_CONN* conn, const char* database, const char* collection, const BSON* b);

BSON* BsonCreate(void);
void BsonDestroy(BSON *b);

bool BsonIterInit(BSON_ITERATOR *it, BSON *b);
bool BsonIterSubObject(BSON_ITERATOR *it, BSON *b);
int32_t BsonIterInt32(BSON_ITERATOR *it);
int64_t BsonIterInt64(BSON_ITERATOR *it);
double BsonIterDouble(BSON_ITERATOR *it);
bool BsonIterBool(BSON_ITERATOR *it);
const char* BsonIterString(BSON_ITERATOR *it);
const bson_oid_t * BsonIterOid(BSON_ITERATOR *it);
time_t BsonIterDate(BSON_ITERATOR *it);
const char* BsonIterKey(BSON_ITERATOR *it);
int BsonIterType(BSON_ITERATOR *it);
int BsonIterNext(BSON_ITERATOR *it);
bool BsonIterSubIter(BSON_ITERATOR *it, BSON_ITERATOR* sub);
void BsonOidFromString(bson_oid_t *o, char* str);

BSON *BsonCreate();
bool BsonAppendOid(BSON *b, const char* key, bson_oid_t *v);
bool BsonAppendBool(BSON *b, const char* key, bool v);
bool BsonAppendNull(BSON *b, const char* key);
bool BsonAppendInt32(BSON *b, const char* key, int v);
bool BsonAppendInt64(BSON *b, const char* key, int64_t v);
bool BsonAppendDouble(BSON *b, const char* key, double v);
bool BsonAppendUTF8(BSON *b, const char* key, char *v);
bool BsonAppendDate(BSON *b, const char* key, time_t v);
bool BsonAppendStartObject(BSON* b, char *key, BSON *r);
bool BsonAppendFinishObject(BSON* b, BSON* r);
bool BsonAppendBson(BSON* b, char *key, BSON* c);
bool BsonFinish(BSON* b);
#endif
