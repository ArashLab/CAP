import subprocess
from .decorators import *
from .common import *
from .logutil import *
from .shared import Shared

import hail as hl
import random
import string
from munch import munchify

if __name__ == '__main__':
    print('This module is not executable. Please import this module in your program.')
    exit(0)

# TBF: Live JSON Input/Output are not supported yet. (Like PCA Eigen)

@D_General
def AbsPath(path):
    abspath = os.path.abspath(path)
    Log(f'Absolute path of {path} is {abspath}')
    return abspath

@D_General
def Bash(command):
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
                    LogPrint(f'{k} of type {t} can not be flattend beacuase its length is variable min:{minLen} max:{maxLen}.', level='WARNING')

    Count(ht)
    return ht


@D_General
def ImportMultipleTsv(files, tempDir, addFileNumber=False):
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
    # TBF support for bgz and gz
    for i, file in enumerate(files):
        if i == 0:  # The first table
            ht = hl.import_table(file, impute=False, force=True)
            if addFileNumber:
                ht = ht.annotate(fileNumber=i)
        htx = hl.import_table(file, impute=False, force=True)
        if addFileNumber:
            htx = htx.annotate(fileNumber=i)
        ht = ht.union(htx, unify=True)

    # write table and read it back with impute=True
    rnd = ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))
    tempPath = os.path.join(tempDir, f'TEMP-{rnd}.tsv.bgz')
    if FileExist(tempPath):
        LogException(f'Temp file {tempPath} exist on the system')
    Log(f'Write to {tempPath}')
    ht.export(tempPath)
    ht = hl.import_table(tempPath, impute=True)
    # TBF: The above operation should happen in parallel
    # Hint1: parallel='header_per_shard'
    # Hint2: finder.glob(tempPath + '/part-*.bgz') "import glob as finder"
    # Hint3: types=ht.row.dtype
    # TBF: Temp files must be cleand up
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
def CheckRangeShared(varName):
    if varName not in Shared:
        LogException(f'{varName} not in shared')

    var = Shared[varName]

    if var.min > var.max:
        LogException(f'shared.{varName}: minimum {var.min} is greater than maximum {var.max}')

    if not (var.min <= var.default <= var.max):
        LogException(f'shared.{varName}: default value {var.default} must be in range [{var.min}, {var.max}]')

#TBF: This function is dedicated to Shared module and should not be here. However there is a loop dependency as it uses logutils and logutils uses Shared
@D_General
def CheckShared():
    CheckRangeShared('numPartitions')
    CheckRangeShared('numSgeJobs')
