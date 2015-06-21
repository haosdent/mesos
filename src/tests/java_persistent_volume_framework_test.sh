#!/usr/bin/env bash

# Expecting MESOS_SOURCE_DIR and MESOS_BUILD_DIR to be in environment.

env | grep MESOS_SOURCE_DIR >/dev/null

test $? != 0 && \
  echo "Failed to find MESOS_SOURCE_DIR in environment" && \
  exit 1

env | grep MESOS_BUILD_DIR >/dev/null

test $? != 0 && \
  echo "Failed to find MESOS_BUILD_DIR in environment" && \
  exit 1

source ${MESOS_SOURCE_DIR}/support/atexit.sh

MESOS_WORK_DIR=`mktemp -d -t mesos-XXXXXX`

atexit "rm -rf ${MESOS_WORK_DIR}"
export MESOS_WORK_DIR=${MESOS_WORK_DIR}

# Set local Mesos runner to use 3 slaves.
export MESOS_NUM_SLAVES=3

# Set acl envs
export MESOS_ACLS='{"run_tasks":[{"principals":{"type": "ANY"},"users":{"values":["test"]}}]}'
export MESOS_DEFAULT_ROLE=test
export MESOS_ROLES=test
export MESOS_AUTHENTICATE=false

# Check that the Java test framework executes without crashing (returns 0).
exec ${MESOS_BUILD_DIR}/src/examples/java/test-persistent-volume-framework --master=local