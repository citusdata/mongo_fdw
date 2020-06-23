/*-------------------------------------------------------------------------
 *
 * option.c
 * 		FDW option handling for mongo_fdw
 *
 * Portions Copyright (c) 2012-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 2004-2020, EnterpriseDB Corporation.
 * Portions Copyright (c) 2012–2014 Citus Data, Inc.
 *
 * IDENTIFICATION
 * 		option.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"
#include "mongo_wrapper.h"

static char *mongo_get_option_value(List *optionList, const char *optionName);

/*
 * Validate the generic options given to a FOREIGN DATA WRAPPER, SERVER,
 * USER MAPPING or FOREIGN TABLE that uses postgres_fdw.
 *
 * Raise an ERROR if the option or its value is considered invalid.
 */
extern Datum mongo_fdw_validator(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(mongo_fdw_validator);

/*
 * mongo_fdw_validator
 *		Validates options given to one of the following commands:
 *		foreign data wrapper, server, user mapping, or foreign table.
 *
 * This function errors out if the given option name or its value is considered
 * invalid.
 */
Datum
mongo_fdw_validator(PG_FUNCTION_ARGS)
{
	Datum		optionArray = PG_GETARG_DATUM(0);
	Oid			optionContextId = PG_GETARG_OID(1);
	List	   *optionList = untransformRelOptions(optionArray);
	ListCell   *optionCell;

	foreach(optionCell, optionList)
	{
		DefElem    *optionDef = (DefElem *) lfirst(optionCell);
		char	   *optionName = optionDef->defname;
		bool		optionValid = false;
		int32		optionIndex;

		for (optionIndex = 0; optionIndex < ValidOptionCount; optionIndex++)
		{
			const MongoValidOption *validOption;

			validOption = &(ValidOptionArray[optionIndex]);

			if ((optionContextId == validOption->optionContextId) &&
				(strncmp(optionName, validOption->optionName, NAMEDATALEN) == 0))
			{
				optionValid = true;
				break;
			}
		}

		/* If invalid option, display an informative error message */
		if (!optionValid)
		{
			StringInfo	optionNamesString;

			optionNamesString = mongo_option_names_string(optionContextId);
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("invalid option \"%s\"", optionName),
					 errhint("Valid options in this context are: %s.",
							 optionNamesString->data)));
		}

		/* If port option is given, error out if its value isn't an integer */
		if (strncmp(optionName, OPTION_NAME_PORT, NAMEDATALEN) == 0)
			(void) pg_atoi(defGetString(optionDef), sizeof(int32), 0);
	}

	PG_RETURN_VOID();
}

/*
 * mongo_option_names_string
 *		Finds all options that are valid for the current context, and
 *		concatenates these option names in a comma separated string.
 */
StringInfo
mongo_option_names_string(Oid currentContextId)
{
	StringInfo	optionNamesString = makeStringInfo();
	bool		firstOptionPrinted = false;
	int32		optionIndex;

	for (optionIndex = 0; optionIndex < ValidOptionCount; optionIndex++)
	{
		const MongoValidOption *validOption;

		validOption = &(ValidOptionArray[optionIndex]);

		/* If option belongs to current context, append option name */
		if (currentContextId == validOption->optionContextId)
		{
			if (firstOptionPrinted)
				appendStringInfoString(optionNamesString, ", ");

			appendStringInfoString(optionNamesString, validOption->optionName);
			firstOptionPrinted = true;
		}
	}

	return optionNamesString;
}

/*
 * mongo_get_options
 *		Returns the option values to be used when connecting to and querying
 *		MongoDB.
 *
 * To resolve these values, the function checks the foreign table's options,
 * and if not present, falls back to default values.
 */
MongoFdwOptions *
mongo_get_options(Oid foreignTableId)
{
	ForeignTable *foreignTable;
	ForeignServer *foreignServer;
	UserMapping *mapping;
	char       *portName;
	List	   *optionList = NIL;
	MongoFdwOptions *options;

	foreignTable = GetForeignTable(foreignTableId);
	foreignServer = GetForeignServer(foreignTable->serverid);
	mapping = GetUserMapping(GetUserId(), foreignTable->serverid);

	optionList = list_concat(optionList, foreignTable->options);
	optionList = list_concat(optionList, foreignServer->options);
	optionList = list_concat(optionList, mapping->options);

	options = (MongoFdwOptions *) palloc0(sizeof(MongoFdwOptions));

#ifdef META_DRIVER
	options->readPreference = mongo_get_option_value(optionList,
													 OPTION_NAME_READ_PREFERENCE);
	options->authenticationDatabase = mongo_get_option_value(optionList,
															 OPTION_NAME_AUTHENTICATION_DATABASE);
	options->replicaSet = mongo_get_option_value(optionList,
												 OPTION_NAME_REPLICA_SET);
	options->ssl = mongo_get_option_value(optionList, OPTION_NAME_SSL);
	options->pem_file = mongo_get_option_value(optionList,
											   OPTION_NAME_PEM_FILE);
	options->pem_pwd = mongo_get_option_value(optionList, OPTION_NAME_PEM_PWD);
	options->ca_file = mongo_get_option_value(optionList, OPTION_NAME_CA_FILE);
	options->ca_dir = mongo_get_option_value(optionList,
											 OPTION_NAME_CA_DIR);
	options->crl_file = mongo_get_option_value(optionList,
											   OPTION_NAME_CRL_FILE);
	options->weak_cert_validation = mongo_get_option_value(optionList,
														   OPTION_NAME_WEAK_CERT);
#endif
	options->svr_address = mongo_get_option_value(optionList,
												  OPTION_NAME_ADDRESS);
	if (options->svr_address == NULL)
		options->svr_address = pstrdup(DEFAULT_IP_ADDRESS);

	portName = mongo_get_option_value(optionList, OPTION_NAME_PORT);
	if (portName == NULL)
		options->svr_port = DEFAULT_PORT_NUMBER;
	else
		options->svr_port = pg_atoi(portName, sizeof(int32), 0);

	options->svr_database = mongo_get_option_value(optionList,
												   OPTION_NAME_DATABASE);
	if (options->svr_database == NULL)
		options->svr_database = pstrdup(DEFAULT_DATABASE_NAME);

	options->collectionName = mongo_get_option_value(optionList,
													 OPTION_NAME_COLLECTION);
	if (options->collectionName == NULL)
		options->collectionName = get_rel_name(foreignTableId);

	options->svr_username = mongo_get_option_value(optionList,
												   OPTION_NAME_USERNAME);
	options->svr_password = mongo_get_option_value(optionList,
												   OPTION_NAME_PASSWORD);

	return options;
}

void
mongo_free_options(MongoFdwOptions *options)
{
	if (options)
	{
		pfree(options->svr_address);
		pfree(options->svr_database);
		pfree(options);
	}
}

/*
 * mongo_get_option_value
 *		Walks over foreign table and foreign server options, and looks for the
 *		option with the given name.  If found, the function returns the
 *		option's value.
 */
static char *
mongo_get_option_value(List *optionList, const char *optionName)
{
	ListCell   *optionCell;
	char	   *optionValue = NULL;

	foreach(optionCell, optionList)
	{
		DefElem    *optionDef = (DefElem *) lfirst(optionCell);
		char	   *optionDefName = optionDef->defname;

		if (strncmp(optionDefName, optionName, NAMEDATALEN) == 0)
		{
			optionValue = defGetString(optionDef);
			break;
		}
	}

	return optionValue;
}
