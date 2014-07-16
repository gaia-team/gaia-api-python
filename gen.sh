#!/bin/bash

# Get the dir of this script so we can be called from anywhere.
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

SCHEMA_DIR="${SCRIPT_DIR}/../gpudb-schemas/obj_defs/"
OUTPUT_DIR="${SCRIPT_DIR}/gpudb/obj_defs/"

# Git doesn't allow empty dirs, create it on demand.
if [ ! -d "$OUTPUT_DIR" ]; then
    mkdir $OUTPUT_DIR
fi

find "${SCHEMA_DIR}" -name "*.json" | while read -r file
do
    cp "${file}" ${OUTPUT_DIR}/.
    echo "cp ${file}"
done
