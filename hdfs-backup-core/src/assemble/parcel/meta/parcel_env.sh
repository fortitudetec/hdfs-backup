#!/bin/bash

HDFS_BACKUP_CORE=$PARCELS_ROOT/$PARCEL_DIRNAME
for f in ${HDFS_BACKUP_CORE}/lib/*
do
  if [ -n "${HADOOP_CLASSPATH}" ]; then
    export HADOOP_CLASSPATH="${HADOOP_CLASSPATH}:${f}"
  else
    export HADOOP_CLASSPATH="${f}"
  fi
done
