#!/bin/bash
SCHEMADIR=../../gaiadb-schemas/obj_defs/

find "${SCHEMADIR}" -name "*.json" | while read -r file
do
	cp "${file}" .
	echo "cp ${file} ."
done
