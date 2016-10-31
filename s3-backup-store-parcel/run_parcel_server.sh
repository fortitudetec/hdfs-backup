#!/bin/bash

PROJECT_DIR=`dirname "$0"`
PROJECT_DIR=`cd "$PROJECT_DIR"; pwd`

PARCEL_VERSION=`mvn help:evaluate -Dexpression=project.version | grep -Ev '(^\[|Download\w+:)'`
echo "PARCEL_VERSION=${PARCEL_VERSION}"
TARGET="${PROJECT_DIR}/target"

PARCEL_NAME=`mvn help:evaluate -Dexpression=parcel.name | grep -Ev '(^\[|Download\w+:)'`

LAST_UPDATED_SEC=`date +%s`
LAST_UPDATED="${LAST_UPDATED_SEC}0000"

PARCEL="${TARGET}/${PARCEL_NAME}-${PARCEL_VERSION}.tar.gz"
PARCEL_SHA="${PARCEL}.sha"

HTTP_DIR="${TARGET}/http"
MANIFEST="${HTTP_DIR}/manifest.json"

rm -r $HTTP_DIR
mkdir $HTTP_DIR

if hash sha1sum 2>/dev/null; then
  sha1sum $PARCEL | awk '{print $1}' > $PARCEL_SHA
else
  shasum $PARCEL | awk '{print $1}' > $PARCEL_SHA
fi

HASH=`cat $PARCEL_SHA`
echo "{\"lastUpdated\":${LAST_UPDATED},\"parcels\": [" > $MANIFEST
for DISTRO in el5 el6 sles11 lucid precise trusty squeeze wheezy
do
	if [ $DISTRO != "el5" ] ; then
		echo "," >> $MANIFEST
	fi
	DISTRO_PARCEL="${PARCEL_NAME}-${PARCEL_VERSION}-${DISTRO}.parcel"
	DISTRO_PARCEL_SHA="${PARCEL_NAME}-${PARCEL_VERSION}-${DISTRO}.parcel.sha"
	ln $PARCEL "${HTTP_DIR}/${DISTRO_PARCEL}"
	ln $PARCEL_SHA "${HTTP_DIR}/${DISTRO_PARCEL_SHA}"
	echo "{\"parcelName\":\"${DISTRO_PARCEL}\",\"components\": [{\"name\" : \"${PARCEL_NAME}\",\"version\" : \"${PARCEL_VERSION}\",\"pkg_version\": \"${PARCEL_VERSION}\"}],\"hash\":\"${HASH}\"}" >> $MANIFEST
done
echo "]}" >> $MANIFEST
cd ${HTTP_DIR}
python -m SimpleHTTPServer
