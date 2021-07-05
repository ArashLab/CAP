#!/bin/bash

# Cap Vep Script (CVS)

VCF=$1
JSON=$2
TBL=$3
DONE=$4
SC=$5 # Script Path

rm -rf "${DONE}"

bash "${SC}/vep.sh" "${VCF}" "${JSON}"

python -m cap.vepparser --json "${JSON}" --parquet "${TBL}"

touch "${DONE}"
