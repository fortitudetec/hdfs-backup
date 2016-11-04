#!/bin/bash

HDFS_BACKUP_CORE=$PARCELS_ROOT/$PARCEL_DIRNAME

if [ -n "${HADOOP_CLASSPATH}" ]; then
  export HADOOP_CLASSPATH="${HADOOP_CLASSPATH}:${HDFS_BACKUP_CORE}/lib/*"
else
  export HADOOP_CLASSPATH="${HDFS_BACKUP_CORE}/lib/*"
fi