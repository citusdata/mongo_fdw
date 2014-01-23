# mongo_fdw/Makefile

MODULE_big = mongo_fdw

#
# We assume we are running on a POSIX compliant system (Linux, OSX). If you are
# on another platform, change env_posix.os in MONGO_OBJS with the appropriate
# environment object file.
#

MONGO_DRIVER = mongo-c-driver
MONGO_PATH = $(MONGO_DRIVER)/src
MONGO_OBJS = $(MONGO_PATH)/bson.os $(MONGO_PATH)/encoding.os $(MONGO_PATH)/md5.os \
             $(MONGO_PATH)/mongo.os $(MONGO_PATH)/numbers.os $(MONGO_PATH)/env_posix.os

PG_CPPFLAGS = --std=c99 -I$(MONGO_PATH)
OBJS = mongo_fdw.o mongo_query.o $(MONGO_OBJS)

EXTENSION = mongo_fdw
DATA = mongo_fdw--1.0.sql

#
# We first build our dependency, the Mongo C driver library here. For this, we
# explicitly invoke the subdirectory's make system.
#

all: all-mongo-driver
all-mongo-driver:
	$(MAKE) -C $(MONGO_DRIVER) all

clean: clean-mongo-driver
clean-mongo-driver:
	$(MAKE) -C $(MONGO_DRIVER) clean

#
# Users need to specify their Postgres installation path through pg_config. For
# example: /usr/local/pgsql/bin/pg_config or /usr/lib/postgresql/9.1/bin/pg_config
#

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
