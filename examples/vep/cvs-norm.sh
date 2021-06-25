#!/bin/bash

# Cap Vep Script (CVS) BackGround job (BG) wrapper

VCF=$1
JSON=$2
TSV=$3
JNAME=$4 # Job Name
JPATH=$5 # Job files location
SC=$6 # Script Path

STDOUT="${JPATH}.output.txt"
STDERR="${JPATH}.error.txt"

bash "${SC}/cvs.sh" "${VCF}" "${JSON}" "${TSV}" "${JPATH}.done" "${SC}" > "${STDOUT}" 2> "${STDERR}"