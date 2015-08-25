#! /bin/bash

#-------------------------------------------------------------------------
#
# autogen.sh
#             Foreign-data wrapper for remote MongoDB servers
#
# Portions Copyright (c) 2012-2014, PostgreSQL Global Development Group
#
# Portions Copyright (c) 2004-2014, EnterpriseDB Corporation.
#
# IDENTIFICATION
#             autogen.sh
#
#-------------------------------------------------------------------------

if [ "$#" -ne 1 ]; then
    echo "Usage: autogen.sh --[with-legacy | with-master]"
    exit
fi

###
# Pull the latest version of Monggo C Driver's master branch
#
function checkout_mongo_driver
{
	rm -rf mongo-c-driver
	wget https://github.com/mongodb/mongo-c-driver/releases/download/1.1.0/mongo-c-driver-1.1.0.tar.gz
	tar -zxvf mongo-c-driver-1.1.0.tar.gz
	mv mongo-c-driver-1.1.0 mongo-c-driver
	rm -rf mongo-c-driver-1.1.0.tar.gz
}

###
# Pull the legacy branch from the Mongo C Driver's
#
function checkout_legacy_branch
{
	rm -rf mongo-c-driver
	wget https://github.com/mongodb/mongo-c-driver/archive/v0.8.tar.gz
	tar -zxvf v0.8.tar.gz
	mv  mongo-c-driver-0.8 mongo-c-driver
	rm -rf v0.8.tar.gz
}
##
# Pull the json-c library
#
function checkout_json_lib
{
	echo $PWD
	rm -rf json-c
	wget https://github.com/json-c/json-c/archive/json-c-0.12-20140410.tar.gz
	tar -zxvf json-c-0.12-20140410.tar.gz
	mv json-c-json-c-0.12-20140410 json-c
	cd json-c
	patch -p1 < ../json_compilation_error.patch
	cd ..
	rm -rf json-c-0.12-20140410.tar.gz
	echo $PWD
}


##
# Compile and instal json-c library
#
function install_json_lib
{
	cd json-c
	sh ./autogen.sh
	./configure
	make install
	cd ..
}

###
# Configure and install the Mongo C Driver and libbson
#
function install_mongoc_driver
{
	cd mongo-c-driver
	./autogen.sh 
	./configure --with-libbson=system
	make install
	cd ..
}

###
# Cleanup the system
#
function cleanup
{
	rm config.h
	touch config.h
}

###
# Create a config file and append #define META_DRIVER which will be
# used in case of Meta Driver (master branch) option.
#
function create_config
{
	echo "#ifdef __CONFIG__" >> config.h
	echo "#define META_DRIVER" >> config.h
	echo "#endif" >> config.h
}

cleanup

if [ "--with-legacy" = $1 ]; then
	checkout_mongo_driver
	checkout_json_lib
	checkout_legacy_branch
	install_json_lib
	cp Makefile.legacy Makefile
	echo "Done"
elif [ "--with-master" == $1 ]; then
	checkout_mongo_driver
	checkout_json_lib
	install_mongoc_driver
	install_json_lib
	create_config
	export PKG_CONFIG_PATH=mongo-c-driver/src/:mongo-c-driver/src/libbson/src
	cp Makefile.meta Makefile
	echo "Done"
else
	echo "Usage: autogen.sh --[with-legacy | with-master]"
fi

