import glob
from hail.expr.functions import log
from pyspark.sql import SQLContext
from functools import reduce
from pyspark.sql.functions import lit, overlay
from hail import Table

import subprocess
from .decorators import *
from .common import *
from .logutil import *
from .shared import Shared

import hail as hl
import random
import string
from munch import Munch, munchify
import io
import pandas as pd

if __name__ == '__main__':
    print('This module is not executable. Import this module in your program.')
    exit(0)

# TBF: Live JSON Input/Output are not supported yet. (Like PCA Eigen)

@D_General
def Bash(command, isPath):

    if len(command) != len(isPath):
        Log(command)
        Log(isPath)
        LogException(f'Command array is of lenght {len(command)} but isPath array is of lenght {len(isPath)}')

    for i in range(len(isPath)):
        if isPath[i]:
            command[i] =  GetLocalPath(command[i])

    LogPrint(f'Executing bash command {command}')
    process = subprocess.run(command)
    if process.returncode:
        Log(f'Failed to execute command {command}', level='ERROR')
        LogException(f'Return code is {process.returncode}')

@D_General
def HailPath(path):
    """Form a hail expression from a list (allow Programmatic access to fields)

    Args:
        path (list): A list where the first element is Hail Table or MatrixTable and the rest define the field path in the Table or MatrixTable.

    Returns:
        HailExpression: The Hail expression to point to a filed and to be passed to hail function
    """
    if isinstance(path, list):
        if len(path) > 0:
            if type(path[0]) not in [hl.Table, hl.MatrixTable]:
                LogException(f'The first element of path argument (list) must be of type Table or MatrixTable but it is of type {type(path[0])} and the value is {path[0]}.')
            target = path[0]
            for i in range(1, len(path)):
                target = target[path[i]]
            return target
        else:
            LogException('Path is empty.')
    else:
        LogException(f'Path must be a list but it is of type {type(path)} and the value is {path}.')

@D_General
def Count(table):
    """Count a Hail Table or MatrixTable.

    Args:
        table (hail.Table, hail.MatrixTable): To be counted.

    Returns:
        dict: Count of rows and columns.
    """

    Log(f'Counting {type(table)}...')
    if isinstance(table, hl.Table):
        ht = table
        cnt = munchify({'rows': ht.count()})
    elif isinstance(table, hl.MatrixTable):
        mt = table
        cnt = munchify(dict(zip(['variants', 'samples'], mt.count())))
    else:
        LogException(f'Counting is not implemented for type {type(table)}.')

    Log(f'Counts: {JsonDumps(cnt)}.')

    return cnt

@D_General
def SampleRows(mt, subSample):
    """Subsample rows of MatrixTable by a fraction or number of rows.

    Note:
        - If subsample is
            - between 0 and 1 it is considered as fraction of rows.
            - greater than 1 it is considered as number of rows.
                - The number of rows in output may be slightly different to the number of requested rows.
            - is 1 then the input MatrixTable is retunred with no changes.
            - otherwise Exception is raised

    Args:
        mt (MatrixTable): To be sampled.
        subSample (int, float): fraction or number of rows to be sampled.

    Returns:
        MatrixTable: Sampled data.
    """

    if type(subSample) not in [float, int]:
        LogException('subsample should be number.')
    if subSample == 1:
        pass
    elif 0 < subSample < 1:
        mt = mt.sample_rows(p=subSample)
    elif subSample > 1:
        cnt = Count(mt)
        if cnt.variants <= subSample:
            LogPrint(f'Number of variants {cnt.variants} is less than what is requsted {subSample}.', level='WARNING')
        else:
            ratio = subSample / cnt.variants
            mt = mt.sample_rows(p=ratio)
    else:
        LogException(f'invalid subsample {subSample} value.')

    Count(mt)
    return mt

@D_General
def SplitMulti(mt, params):
    if 'withHTS' in params:
        withHTS = params.withHTS
        del params.withHTS
    else:
        withHTS = False

    if withHTS:
        mt = hl.split_multi_hts(mt, **params)
    else:
        mt = hl.split_multi(mt, **params)
    
    return mt

@D_General
def AddId(mt, params):

    if 'sampleId' in params:
        mt = mt.annotate_cols(sampleId=mt[params.sampleId])
        mt = mt.key_cols_by('sampleId')
    
    if 'variantId' in params:
        if params.variantId== 'CHR:POS:ALLELES':
            mt = mt.annotate_rows(variantId=hl.str(':').join(hl.array([mt.locus.contig, hl.str(mt.locus.position)]).extend(mt.alleles)))
        else:
            LogException('VariantId is not Supported')
    
    return mt

@D_General
def FlattenTable(ht):
    """Recursively flatten table fields including arrays.

    Note:
        - This function ignores flattening variable size array.

    Args:
        ht (Table): To be flattend.

    Returns:
        Table: Flattened table.
    """

    doneFlag = False
    while not doneFlag:
        ht = ht.flatten().expand_types().flatten()
        doneFlag = True
        for k, t in ht.row.items():
            if str(t.dtype).startswith('array'):
                Log(f'Flattening {k} array.')
                try:
                    maxLen = ht.aggregate(hl.agg.max(hl.len(ht[k])))
                    minLen = ht.aggregate(hl.agg.min(hl.len(ht[k])))
                except:
                    LogException(f'Cannot aggregate min or max lenght of the the array {k}.')

                if minLen == maxLen:
                    Log(f'{maxLen} new column to be created out of {k} array.')
                    expr = dict()
                    for i in range(1, maxLen+1):
                        expr[f'{k}.{i}'] = ht[k][i-1]
                    try:
                        ht = ht.annotate(**expr)
                    except:
                        LogException(f'Cannot perform annotation with expression {expr}.')
                    try:
                        ht = ht.drop(k)
                    except:
                        LogException(f'Cannot drop {k} from table.')
                    doneFlag = False
                else:
                    Log(f'{k} of type {t} can not be flattend beacuase its length is variable min:{minLen} max:{maxLen}.', level='WARNING')
                    Log(f'Variable length array {k} is converted to string with " ~#^#~ " as a seperator', level='WARNING')
                    expr = dict()
                    expr[k] = hl.str(' ~#^#~ ').join(ht[k])
                    try:
                        ht = ht.annotate(**expr)
                    except:
                        LogException(f'Cannot perform annotation with expression {expr}.')

    Count(ht)
    return ht


@D_General
def ImportMultipleTable(files, addFileNumber=False):
    """Load multiple tsv files and turn it into a Hail Table

    Notes:
        - There are difficulties merging tsv files comeing from VEP.
        - Each file miss some of the columns (we fix it by loading each file into a tsv and then union tsv with unify=True)
        - Each file is imputed differently (like chr could be int or str depending on if chr X and Y included)
        - The only solution to fix the impute issue is to load all without impute and ther write it in single table and then read it back with impute (That is why TMP directory is needed)
        - Cleaning TEMP file is difficult because of lazy load in spark. 
        - Without tsv.bgz file parallel read is impossible but the VepJsonToTsv canno yet create bgz files

    Args:
        files (list): list of tsv files.
        tempDir (string): Where to store temporary data.
        addFileNumber (bool, optional): Add file number to create uniqe ids. Defaults to False.

    Returns:
        Hail.Table: Merged Hail table. 
    """    
    if not files:
        LogException('No file to be loaded')


    sc = hl.spark_context()
    sqlc = SQLContext(sc)

    fileList = glob.glob(GetLocalPath(files))
    fileList = [f'file://{file}' for file in fileList]
    Log(f'Number of files linked to the input path {len(fileList)}')

    if addFileNumber:
        dfs = [sqlc.read.parquet(file).withColumn("fileNumber", lit(i)) for i, file in enumerate(fileList)]
    else:
        dfs = [sqlc.read.parquet(file) for file in fileList]

    def UnionByName(a, b):
        return a.unionByName(b, allowMissingColumns=True)

    df = reduce(UnionByName, dfs)
    Log(f'Count DataFrame: {df.count()}')

    ht = Table.from_spark(df)

    Count(ht)
    
    return ht


@D_General
def FlattenJson(iJson):  # TBF: not used currently . To be used in VepJsonToTsv 
    oJson = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '.')
        elif type(x) is list:
            for i, a in enumerate(x):
                flatten(a, name + str(i) + '.')
        else:
            oJson[name[:-1]] = x
    flatten(iJson)
    return oJson

#TBF: This function shoudl be more generalised
@D_General
def CheckRange(varName):
    if varName not in Shared.defaults:
        LogException(f'{varName} not in Shared.defaults')

    var = Shared.defaults[varName]

    if var.min > var.max:
        LogException(f'Shared.defaults.{varName}: minimum {var.min} is greater than maximum {var.max}')

    if not (var.min <= var.default <= var.max):
        LogException(f'Shared.defaults.{varName}: default value {var.default} must be in range [{var.min}, {var.max}]')

#TBF: This function is dedicated to Shared module and should not be here.
# However there is a loop dependency as it uses logutils and logutils uses Shared
@D_General
def CheckDefaults():
    CheckRange('numPartitions')
    CheckRange('numSgeJobs')

@D_General
def InferColumnTypes(df):
    Log(df.dtypes)
    memFile = io.StringIO()
    df.to_csv(memFile, index=False, sep='\t')
    memFile.seek(0)
    df = pd.read_csv(memFile, delimiter='\t')
    Log(df.dtypes)
    return df

@D_General
def GetFile(iof):    
    return Shared.dataFiles[iof.id]

@D_General
def UnpackStage(stage):
    arg = Munch()
    io = Munch()
    spec = Munch()

    if 'spec' in stage:
        spec = stage.spec

    if 'arg' in stage:
        arg = stage.arg
    
    if 'io' in stage:
        io = stage.io

    return spec, arg, io
