#!/bin/bash

VCF=$1
JSON=$2

VCF_DIR=`dirname "${VCF}"`
VCF_BASE=`basename "${VCF}"`

JSON_DIR=`dirname "${JSON}"`
JSON_BASE=`basename "${JSON}"`

docker run -d --name "${VCF_BASE}" -v "$HOME/vep_data":/opt/vep/.vep -v "${VCF_DIR}":/opt/vep/vcf -v "${JSON_DIR}":/opt/vep/json ensemblorg/ensembl-vep vep --format vcf --json --compress_output bgzip --everything --assembly GRCh37 --cache --offline --merged --no_stats -i "/opt/vep/vcf/${VCF_BASE}" -o "/opt/vep/json/${JSON_BASE}"
docker wait "${VCF_BASE}"
docker rm "${VCF_BASE}"