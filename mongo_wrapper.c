/*-------------------------------------------------------------------------
 *
 * mongo_wrapper.c
 *
 * Wrapper functions for MongoDB's old legacy Driver.
 *
 * Portions Copyright © 2004-2014, EnterpriseDB Corporation.
 *
 * Portions Copyright © 2012–2014 Citus Data, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "mongo_wrapper.h"

#ifdef META_DRIVER
	#include "mongoc.h"
#else
	#include "mongo.h"
#endif

#include "mongo_fdw.h"

#define QUAL_STRING_LEN 512

MONGO_CONN*
MongoConnect(const char* host, const unsigned short port, char* databaseName, char *user, char *password)
{
	MONGO_CONN *conn;
	conn = mongo_alloc();
	mongo_init(conn);

	if (mongo_connect(conn, host, port) != MONGO_OK)
	{
		int err = conn->err;
		mongo_destroy(conn);
		mongo_dealloc(conn);
		ereport(ERROR, (errmsg("could not connect to %s:%d", host, port),
						errhint("Mongo driver connection error: %d", err)));
	}
	if (user && password)
	{
		if (mongo_cmd_authenticate(conn, databaseName, user, password) != MONGO_OK)
		{
			int err = conn->err;
			mongo_destroy(conn);
			mongo_dealloc(conn);
			ereport(ERROR, (errmsg("could not connect to %s:%d", host, port),
							errhint("Mongo driver connection error: %d", err)));
		}
	}
	return conn;
}

void
MongoDisconnect(MONGO_CONN* conn)
{
	mongo_destroy(conn);
	mongo_dealloc(conn);
}

bool
MongoInsert(MONGO_CONN* conn, char* database, char *collection, bson* b)
{
	char qual[QUAL_STRING_LEN];

	snprintf (qual, QUAL_STRING_LEN, "%s.%s", database, collection);
	if (mongo_insert(conn, qual, b, NULL) != MONGO_OK)
		ereport(ERROR, (errmsg("failed to insert row"),
						errhint("Mongo driver insert error: %d", conn->err)));
	return true;
}


bool
MongoUpdate(MONGO_CONN* conn, char* database, char *collection, BSON* b, BSON* op)
{
	char qual[QUAL_STRING_LEN];

	snprintf (qual, QUAL_STRING_LEN, "%s.%s", database, collection);
	if (mongo_update(conn, qual, b, op, MONGO_UPDATE_BASIC, 0) != MONGO_OK)
		ereport(ERROR, (errmsg("failed to update row"),
						errhint("Mongo driver update error: %d", conn->err)));
	return true;
}


bool
MongoDelete(MONGO_CONN* conn, char* database, char *collection, BSON* b)
{
	char qual[QUAL_STRING_LEN];

	snprintf (qual, QUAL_STRING_LEN, "%s.%s", database, collection);
	if (mongo_remove(conn, qual, b , NULL) != MONGO_OK)
		ereport(ERROR, (errmsg("failed to delete row"),
						errhint("Mongo driver delete error: %d", conn->err)));

	return true;
}


MONGO_CURSOR*
MongoCursorCreate(MONGO_CONN* conn, char* database, char *collection, BSON* q)
{
	MONGO_CURSOR* c;
	char qual[QUAL_STRING_LEN];

	snprintf (qual, QUAL_STRING_LEN, "%s.%s", database, collection);
	c = mongo_cursor_alloc();
	mongo_cursor_init(c, conn , qual);
	mongo_cursor_set_query(c, q);
	return c;
}


const bson*
MongoCursorBson(MONGO_CURSOR* c)
{
	return mongo_cursor_bson(c);
}


bool
MongoCursorNext(MONGO_CURSOR* c, BSON* b)
{
	return (mongo_cursor_next(c) == MONGO_OK);
}

void
MongoCursorDestroy(MONGO_CURSOR* c)
{
	mongo_cursor_destroy(c);
	mongo_cursor_dealloc(c);
}

BSON*
BsonCreate()
{
	BSON *b = NULL;
	b = bson_alloc();
	bson_init(b);
	return b;
}

void
BsonDestroy(BSON *b)
{
	bson_destroy(b);
	bson_dealloc(b);
}

bool
BsonIterInit(BSON_ITERATOR *it, BSON *b)
{
	bson_iterator_init(it, b);
	return true;
}

bool
BsonIterSubObject(BSON_ITERATOR *it, BSON *b)
{
	bson_iterator_subobject_init(it, b, 0);
	return true;
}

int32_t
BsonIterInt32(BSON_ITERATOR *it)
{
	return bson_iterator_int(it);
}


int64_t
BsonIterInt64(BSON_ITERATOR *it)
{
	return bson_iterator_long(it);
}


double
BsonIterDouble(BSON_ITERATOR *it)
{
	return bson_iterator_double(it);
}


bool
BsonIterBool(BSON_ITERATOR *it)
{
	return bson_iterator_bool(it);
}


const char*
BsonIterString(BSON_ITERATOR *it)
{
	return bson_iterator_string(it);
}

const bson_oid_t *
BsonIterOid(BSON_ITERATOR *it)
{
	return bson_iterator_oid(it);
}


time_t
BsonIterDate(BSON_ITERATOR *it)
{
	return bson_iterator_date(it);
}


const char*
BsonIterKey(BSON_ITERATOR *it)
{
	return bson_iterator_key(it);
}

int
BsonIterType(BSON_ITERATOR *it)
{
	return bson_iterator_type(it);
}

int
BsonIterNext(BSON_ITERATOR *it)
{
	return bson_iterator_next(it);
}


bool
BsonIterSubIter(BSON_ITERATOR *it, BSON_ITERATOR* sub)
{
	bson_iterator_subiterator(it, sub);
	return true;
}


void
BsonOidFromString(bson_oid_t *o, char* str)
{
	bson_oid_from_string(o, str);
}


bool
BsonAppendOid(BSON *b, const char* key, bson_oid_t *v)
{
	return (bson_append_oid(b, key, v) == MONGO_OK);
}

bool
BsonAppendBool(BSON *b, const char* key, bool v)
{
	return (bson_append_int(b, key, v) == MONGO_OK);
}

bool
BsonAppendNull(BSON *b, const char* key)
{
	return (bson_append_null(b, key) == MONGO_OK);
}


bool
BsonAppendInt32(BSON *b, const char* key, int v)
{
	return (bson_append_int(b, key, v) == MONGO_OK);
}


bool
BsonAppendInt64(BSON *b, const char* key, int64_t v)
{
	return (bson_append_long(b, key, v) == MONGO_OK);
}

bool
BsonAppendDouble(BSON *b, const char* key, double v)
{
	return (bson_append_double(b, key, v) == MONGO_OK);
}

bool
BsonAppendUTF8(BSON *b, const char* key, char *v)
{
	return (bson_append_string(b, key, v) == MONGO_OK);
}

bool
BsonAppendDate(BSON *b, const char* key, time_t v)
{
	return (bson_append_date(b, key, v) == MONGO_OK);
}


bool BsonAppendStartArray(BSON *b, const char* key, BSON* c)
{
    return (bson_append_start_array(b, key) == MONGO_OK);
}


bool BsonAppendFinishArray(BSON *b, BSON *c)
{
    return (bson_append_finish_array(b) == MONGO_OK);
}

bool
BsonAppendStartObject(BSON* b, char *key, BSON* r)
{
	return (bson_append_start_object(b, key) == MONGO_OK);
}

bool
BsonAppendFinishObject(BSON* b, BSON* r)
{
	return (bson_append_finish_object(b) == MONGO_OK);
}


bool
BsonAppendBson(BSON* b, char *key, BSON* c)
{
	return (bson_append_bson(b, key, c) == MONGO_OK);
}


bool
BsonFinish(BSON* b)
{
	return (bson_finish(b) == MONGO_OK);
}

double
MongoAggregateCount(MONGO_CONN* conn, const char* database, const char* collection, const BSON* b)
{
	return mongo_count(conn, database, collection, b);
}

