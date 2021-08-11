import argparse
import gzip
import json
import pandas as pd
import yaml

from .logutil import *
from .common import *
from .helper import *
from .decorators import *


@D_General
def VepParser(args):
    """Convert VEP json output to a set of tsv files for parsing with Hail

    Note:
        - This function generates 4 tsv files:
            - var.tsv: Variants
            - clvar.tsv: Colocated variants
            - freq.tsv: Frequencies of the colocated variants
            - conseq.tsv: Consequences of variants
        - tsv are compressed with blocked-gzip ('.tsv.bgz')

    Args:
        args (string): argparser object
    """
    with gzip.open(args.json, 'r') as vepJsonFile:

        clVarCnt = 0  # colocated-variant variant counter

        variants = list()
        clVariants = list()  # colocated-variant
        frequencies = list()
        consequences = list()

        # Each line is the VEP annotation for a variant in JSON format
        for line in vepJsonFile:
            try:
                variant = json.loads(line)
            except:
                LogException(f'Cannot load json form {line}')

            if 'id' not in variant:
                LogException(f'There is no id in the VEP json output')

            variant['varId'] = variant['id']
            del variant['id']
            if 'input' in variant:
                del variant['input']

            # Get a list of variant's features with the consequences
            features = [feature for feature in variant if feature.endswith('_consequences')]

            # For each feature
            for feature in features:
                featureName = feature[:-13]  # remove '_consequences' from the feature name
                # Add consequences of that feature to list of consequences
                for consequence in variant[feature]:
                    consequence['feature'] = featureName
                    consequence['varId'] = variant['varId']
                    consequences.append(consequence)
                del variant[feature]

            # Process colocated variants and their frequencies
            if 'colocated_variants' in variant:
                # for clVariant in variant['colocated_variants']:
                #     clVariant['varId'] = variant['varId']
                #     clVariant['clVarId'] = clVarCnt
                #     clVarCnt += 1

                #     if 'frequencies' in clVariant:
                #         for allele in clVariant['frequencies']:
                #             freq = clVariant['frequencies'][allele]  # get the dict of frequencys per allele
                #             #TBF the variant id is no longer chr:pos:ref:alt but it is an integer.
                #             # if allele == variant['varId'].split(':')[3]:
                #             #     variant.update(freq)
                #             #     freq['mainVariant'] = True
                #             # else:
                #             #     freq['mainVariant'] = False
                #             freq['varId'] = variant['varId']
                #             freq['clvarId'] = clVariant['clVarId']
                #             freq['allele'] = allele
                #             frequencies.append(freq)
                #         del clVariant['frequencies']
                #     clVariants.append(clVariant)
                del variant['colocated_variants']

            # add remaining field
            variants.append(variant)

        parquetPath = args.parquet
        # write the variant to the file
        if variants:
            cdf = pd.DataFrame(variants)
            cdf = InferColumnTypes(cdf)
            # cdf.to_parquet(f'{parquetPath}.var.parquet', index=False)
            cdf.to_csv(f'{parquetPath}.var.tsv', sep='\t', index=False)
            with open(f'{parquetPath}.var.types.yaml', 'w') as file:
                yaml.dump({k:str(v) for k,v in cdf.dtypes.items()}, file)
            

        # # write colocated variant to the file
        # if clVariants:
        #     cdf = pd.DataFrame(clVariants)
        #     cdf = InferColumnTypes(cdf)
        #     cdf.to_parquet(f'{parquetPath}.clvar.parquet', index=False)

        # # write colocated variant frequencies to the file
        # if frequencies:
        #     cdf = pd.DataFrame(frequencies)
        #     cdf = InferColumnTypes(cdf)
        #     cdf.to_parquet(f'{parquetPath}.freq.parquet', index=False)

        # write consequences to the file
        if consequences:
            cdf = pd.DataFrame(consequences)
            cdf = InferColumnTypes(cdf)
            # cdf.to_parquet(f'{parquetPath}.conseq.parquet', index=False)
            cdf.to_csv(f'{parquetPath}.conseq.tsv', sep='\t', index=False)
            with open(f'{parquetPath}.conseq.types.yaml', 'w') as file:
                yaml.dump({k:str(v) for k,v in cdf.dtypes.items()}, file)

@D_General
def FindSuperSetOfColumns(args):
    fileList = [f'{fileName}' for fileName in glob.glob(args.inYaml)]
    listOfDtypes = list()
    for fileName in fileList:
        with open(fileName, 'r') as file:
            dtypes = yaml.safe_load(file)
            listOfDtypes.append(dtypes)
    dfDtypes = pd.DataFrame(listOfDtypes)
    #===
    dtypes = dict()
    for column in dfDtypes:
        for typeName in ['object', 'float64', 'int64', 'bool']:
            if typeName in list(dfDtypes[column]):
                dtypes[column] = typeName
                break
    #===
    for k in dtypes:
        if dtypes[k] == 'object':
            dtypes[k] = 'str'
        elif dtypes[k] == 'float64':
            dtypes[k] = 'float'
        elif dtypes[k] == 'int64':
            dtypes[k] = 'int'
    
    with open(args.outYaml, 'w') as file:
        yaml.dump(dtypes, file)

@D_General
def Stag3(args):
    
    for k in ['var', 'conseq']:
        pdf = pd.read_csv(f'{args.parquet}.{k}.tsv', sep='\t')
        # print(pdf)
        with open(f'{args.inYaml}/all.table.{k}.types.yaml', 'r') as file:
            dtypes = yaml.safe_load(file)
        # print(dtypes)
        for col in dtypes:
            if col not in pdf:
                pdf[col] = ''
        pdf = pdf.reindex(sorted(pdf.columns), axis=1)
        pdf.to_csv(f'{args.parquet}.{k}.tsv', sep='\t', index=False)

@D_General
def Main():
    parser = argparse.ArgumentParser(
        description='Parse VEP json output and produce a set of TSV files'
    )
    parser.add_argument('-s', '--stage', required=True, type=int, help='Stage.')
    parser.add_argument('-j', '--json', required=False, type=str, help='Input JSON file contains VEP annotations one per line.')
    parser.add_argument('-p', '--parquet', required=False, type=str, help='Output parquet file prefix (do not include ".parquet" at the end).')
    parser.add_argument('-i', '--inYaml', required=False, type=str, help='asdf.')
    parser.add_argument('-o', '--outYaml', required=False, type=str, help='asdf.')
    args = parser.parse_args()
    #print(args)

    # InitLogger(capLog=args.capLog)
    # Log(f'Runtime Information: {Shared.runtime}')

    if args.stage == 1:
        VepParser(args)
    elif args.stage == 2:
        FindSuperSetOfColumns(args)
    elif args.stage == 3:
        Stag3(args)
    else:
        pass

if __name__ == '__main__':
    Log('Collect logs for vepparser module.')
    Main()
