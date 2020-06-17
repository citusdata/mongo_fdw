/*-------------------------------------------------------------------------
 *
 * mongo_wrapper.c
 * 		Foreign-data wrapper for remote MongoDB servers
 *
 * Portions Copyright (c) 2012-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 2004-2020, EnterpriseDB Corporation.
 * Portions Copyright (c) 2012–2014 Citus Data, Inc.
 *
 * IDENTIFICATION
 * 		mongo_wrapper.c
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

#include <bson.h>
#include <json.h>
#include <bits.h>

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
			char *str = pstrdup(conn->errstr);
			mongo_destroy(conn);
			mongo_dealloc(conn);
			ereport(ERROR, (errmsg("could not connect to %s:%d", host, port),
							errhint("Mongo driver connection error: %s", str)));
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

const char* BsonIterBinData(BSON_ITERATOR *it)
{
	return bson_iterator_bin_data(it);
}

int BsonIterBinLen(BSON_ITERATOR *it)
{
	return bson_iterator_bin_len(it);
}


bson_oid_t *
BsonIterOid(BSON_ITERATOR *it)
{
	return bson_iterator_oid(it);
}


time_t
BsonIterDate(BSON_ITERATOR *it)
{
	return bson_iterator_date(it);
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

bool BsonAppendBinary(BSON *b, const char* key, char *v, size_t len)
{
	return (bson_append_binary(b, key, BSON_BIN_BINARY, v, len) == MONGO_OK);
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

json_object*
JsonTokenerPrase(char * s)
{
	return json_tokener_parse(s);
}

bool
JsonToBsonAppendElement(BSON *bb , const char *k , struct json_object *v )
{
	bool status;

	status = true;
	if (!v)
	{
		bson_append_null(bb, k);
		return status;
	}

	switch (json_object_get_type(v))
	{
		case json_type_int:
		bson_append_int(bb, k, json_object_get_int(v));
		break;

		case json_type_boolean:
		bson_append_bool(bb , k, json_object_get_boolean(v));
		break;

		case json_type_double:
		bson_append_double(bb, k, json_object_get_double(v));
		break;

		case json_type_string:
		bson_append_string(bb, k, json_object_get_string(v));
		break;

		case json_type_object:
		{
			struct json_object *joj = NULL;
			joj = json_object_object_get(v, "$oid");

			if (joj != NULL)
			{
				bson_oid_t bsonObjectId;
				memset(bsonObjectId.bytes, 0, sizeof(bsonObjectId.bytes));
				BsonOidFromString(&bsonObjectId,
								  (char *)json_object_get_string(joj));
				status = BsonAppendOid(bb, k , &bsonObjectId);
				break;
			}
			joj = json_object_object_get( v, "$date" );
			if (joj != NULL)
			{
				status = BsonAppendDate(bb, k, json_object_get_int64(joj));
				break;
			}

			bson_append_start_object(bb , k);

			{
				json_object_object_foreach(v, kk, vv)
				{
					JsonToBsonAppendElement(bb, kk, vv);
				}
			}
			bson_append_finish_object(bb);
			break;
		}
		case json_type_array:
		{
			int i;
			char buf[10];
			bson_append_start_array(bb ,k);
			for (i = 0; i<json_object_array_length(v); i++)
			{
			sprintf(buf , "%d" , i);
			JsonToBsonAppendElement(bb ,buf ,json_object_array_get_idx(v, i));
			}
			bson_append_finish_object( bb );
			break;
		}
		default:
			ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
			errmsg("can't handle type for : %s", json_object_to_json_string(v))));
	}
	return status;
}


double
MongoAggregateCount(MONGO_CONN* conn, const char* database, const char* collection, const BSON* b)
{
	return mongo_count(conn, database, collection, b);
}


void BsonIteratorFromBuffer(BSON_ITERATOR * i, const char * buffer)
{
	bson_iterator_from_buffer(i, buffer);
}

void BsonOidToString(const bson_oid_t *o, char str[25])
{
	bson_oid_to_string (o, str);
}

const char*
BsonIterCode(BSON_ITERATOR *i)
{
	return bson_iterator_code (i);
}

const char*
BsonIterRegex(BSON_ITERATOR *i)
{
	return bson_iterator_regex(i);
}

const char*
BsonIterKey(BSON_ITERATOR *i)
{
	return bson_iterator_key(i);
}

const char*
BsonIterValue(BSON_ITERATOR *i)
{
	return bson_iterator_value(i);
}

void
BsonToJsonStringValue(StringInfo output, BSON_ITERATOR *iter, bool isArray)
{
	if (isArray)
		DumpJsonArray(output, iter);
	else
		DumpJsonObject(output, iter);
}

void
DumpJsonObject(StringInfo output, BSON_ITERATOR *iter)
{

}

void
DumpJsonArray(StringInfo output, BSON_ITERATOR *iter)
{

}

char*
BsonAsJson(const BSON* bsonDocument)
{
	elog (ERROR, "Full document retrival only available in MongoC meta driver");
}


