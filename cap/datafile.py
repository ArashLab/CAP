from munch import Munch, munchify
import jsonschema
import os

import hail as hl
from pandas.core.indexing import is_label_like
from .logutil import *
from .common import *
from .helper import *
from .decorators import *
from .pyobj import PyObj
from .shared import Shared

if __name__ == '__main__':
    print('This module is not executable.')
    exit(0)

class DataFile(Munch):

    @D_General
    def __init__(self, file):
        if isinstance(file, Munch):
            self.update(file)
        else:
            LogException(f'`file` must be of type Munch but the type is `{type(file)}`.')

        self.Infer()
        self.InferPath()
        self.isInferred = True
        self.ProcessPath()

        self.InferMemory()

    @D_General
    def Infer(self):
        file = self

        if 'storageType' not in file:
            if 'disk' in file:
                file.storageType = 'disk'
            else:
                file.storageType = 'memory'

        if file.storageType == 'memory' and 'disk' in file:
            LogException('`disk` should not present when storage type is `memory`')

        if file.storageType == 'disk':
            if 'disk' not in file:
                LogException('`disk` should present when storage type is `disk`')
            if 'isProduced' not in file.disk:
                file.disk.isProduced = False

        if 'disk' in file and 'path' not in file.disk:
            LogException('`path` must present in `disk`')


    suffixMapper = {
        '.mt': ('mt', 'None'),
        '.ht': ('ht', 'None'),
        '.vcf': ('vcf', 'None'),
        '.vcf.gz': ('vcf', 'gz'),
        '.vcf.bgz': ('vcf', 'bgz'),
        '.tsv': ('tsv', 'None'),
        '.tsv.gz': ('tsv', 'gz'),
        '.tsv.bgz': ('tsv', 'bgz'),
        '.csv': ('csv', 'None'),
        '.csv.gz': ('csv', 'gz'),
        '.csv.bgz': ('csv', 'bgz'),
        '.json': ('json', 'None'),
        '.json.gz': ('json', 'gz'),
        '.json.bgz': ('json', 'bgz'),
        '.bed': ('bed', 'None'),
        '.bim': ('bim', 'None'),
        '.fam': ('fam', 'None'),
        '.parquet': ('parquet', 'None')
    }

    @D_General
    def InferFormat(self, path):
        for suffix in self.suffixMapper:
            if path.endswith(suffix):
                return self.suffixMapper[suffix]
        return None, None

    prefixMapper = {
        'hdfs://': 'hadoop',
        'file://': 'local',
        's3://': 'aws',
        'gs://': 'google'
    }

    @D_General
    def InferFileSystem(self, path):
        for prefix in self.prefixMapper:
            if path.startswith(prefix):
                return self.prefixMapper[prefix]
        return Shared.defaults.fileSystem

    @D_General
    def InferPath(self):
        inferred = Munch()
        inferred.format = None
        inferred.compression = None
        inferred.fileSystem = None
        inferred.numPath = None
        inferred.isParallel = False
        inferred.isWildcard = False

        self.disk.inferred = inferred

        disk = self.disk
        path = disk.path

        # Check if path is a list
        if isinstance(disk.path, str):
            disk.path = [disk.path]

        if isinstance(disk.path, list):
            inferred.numPath = len(disk.path)
            if not inferred.numPath:
                LogException('path array should not be empty')
            for p in disk.path:
                if not isinstance(p, str):
                    LogException(f'Each path must be of type string but found {p} of type {type(p)}.')
        else:
            LogException(f'path should be of type list but not {type(path)}')

        path = disk.path

        # If path list, infer the first path in the list
        inferred.format, inferred.compression = self.InferFormat(path[0])
        inferred.fileSystem = self.InferFileSystem(path[0])

        # If path list, make sure the all paths in the list infered the same
        if inferred.numPath > 1:
            for i, p in enumerate(path):
                # Inffer the i^th path
                format, compression = self.InferFormat(p)
                fileSystem = self.InferFileSystem(p)

                # Check consistancy of the infered data
                if format != inferred.format:
                    LogException(f'{i}th path format mismatch: {format} is not {inferred.format}')
                if compression != inferred.compression:
                    LogException(f'{i}th path compression mismatch: {compression} is not {inferred.compression}')
                if fileSystem != inferred.fileSystem:
                    LogException(f'{i}th path fileSystem mismatch: {fileSystem} is not {inferred.fileSystem}')

        if 'isWildcard' not in disk:
            for p in disk.path:
                if any([c in p for c in ['*', '?', '[', ']']]):
                    inferred.isWildcard = True

        if 'exportParam' in disk and 'parallel' in disk.exportParam and disk.exportParam.parallel:
            inferred.isParallel = True

        # Check consistensy of the inferred and given information
        for k in disk.inferred:
            if k in disk:
                if disk.inferred[k] and disk[k] != disk.inferred[k]:
                    LogException(f'Infered `{k}` ({disk.inferred[k]}) is different from the given information ({disk[k]})')
            else:
                disk[k] = disk.inferred[k]
                Log(f'Infered `{k}` ({disk.inferred[k]}) is used.')

    @D_General
    def ProcessPath(self):
        disk = self.disk

        newPath = list()

        for rawPath in disk.path:
            # replace ~ and ${VAR} with actual values
            path = os.path.expandvars(rawPath)

            # Check if the path has the prefix (i.e. hdfs://)
            isPrefixed = False
            for prefix in self.prefixMapper:
                if path.lower().startswith(prefix):
                    isPrefixed = True
                    break

            # Get the absolute path (for local storage only and if 'file://' prefix is not attached)
            # Add the prefix based on the fileSystem (if path does not have prefix)
            if not isPrefixed:
                fs = self.disk.fileSystem
                fsFound = False
                for prefix, storage in self.prefixMapper.items():
                    if fs == storage:
                        if storage == 'local':
                            path = os.path.abspath(path)
                        path = f'{prefix}{path}'
                        fsFound = True
                if not fsFound:
                    LogException(f'fileSystem `{fs}` is not supported yet.')

            Log(f'Absolute path of {rawPath} is {path}')

            newPath.append(path)

        disk.path = list(set(newPath))  # remove duplicated path
        disk.numPath = len(newPath)

        disk.fsPrefix = True

        if 'localMode' in Shared.defaults and Shared.defaults.localMode:
            disk.path = [self.GetLocalPath(p) for p in disk.path]
            disk.fsPrefix = False

    @D_General
    def ExpandWildcardPath(self):

        disk = self.disk
        if disk.isWildcard:
            if self.disk.fileSystem != 'local':
                LogException('Wildcard only supported for local fileSystem')

            # keep a copy of original wildcard path
            disk.wildcard = Munch()
            disk.wildcard.path = disk.path
            disk.wildcard.numPath = disk.numPath

            newPath = list()
            for path in disk.path:
                paths = self.ExpandWildcardPathInternal(path)
                newPath.extend(paths)

            disk.path = list(set(newPath))
            disk.numPath = len(newPath)

        if 'localMode' not in Shared.defaults or not Shared.defaults.localMode:
            disk.path = [f'file://{p}' for p in disk.path]

    @D_General
    def GetLocalPath(self, path):
        if self.disk.fileSystem != 'local':
            LogException('Cannot produce local path for non-local storage type')

        return path[7:] if path.lower().startswith('file://') else path

    formatMapper = {
        'mt': ['mt'],
        'ht': ['ht', 'pandas'],
        'csv': ['ht', 'pandas'],
        'tsv': ['ht', 'pandas'],
        'parquet': ['ht', 'pandas'],
        'json': ['obj', 'pandas', 'ht'],
        'yaml': ['obj', 'pandas', 'ht'],
        'vcf': ['mt', 'ht'],
        'plink-bfile': ['mt', 'ht'],
        'bim': ['ht', 'pandas'],
        'fam': ['ht', 'pandas']
    }

    @D_General
    def InferMemory(self):
        file = self
        
        if not file.get('memory'):  # memory could exist but an empty field
            file.memory = Munch()
        memory = file.memory

        memory.inferred = Munch()
        inferred = memory.inferred

        if file.storageType == 'disk':
            inferred.formats = self.formatMapper.get(file.disk.format, [None])
            if not memory.get('format'):
                memory.format = inferred.formats[0]
            else:
                if memory.format not in inferred.formats:
                    LogException(f'Memory format `{memory.format}` is not supported for disk format `{file.disk.format}`. Acceptable memory formats are `{inferred.formats}`')
            memory.isProduced = False #always false at the begining


    @D_General
    def ExistInternal(self, path):
        fs = self.disk.fileSystem
        if fs in ['aws', 'google']:
            LogException(f'`{fs}` not supported')
        elif fs == 'hadoop':
            return not subprocess.run(['hdfs', 'dfs', '-test', '-e', path]).returncode
        elif fs == 'local':
            path = self.GetLocalPath(path)
            return os.path.exists(path)

    @D_General
    def ExistAll(self):
        disk = self.disk
        return all([self.ExistInternal(p) for p in disk.path])

    @D_General
    def ExistAny(self):
        disk = self.disk
        return any([self.ExistInternal(p) for p in disk.path])
        
    @D_General
    def ExpandWildcardPathInternal(self, path):
        path = self.GetLocalPath(path)
        fileList = glob.glob(path)
        Log(f'{len(fileList)} files are found in {path}')
        return fileList

    @D_General
    def Load(self):
        file = self
        disk = file.disk
        memory = file.memory

        if not disk.isProduced:
            LogException('file is not produced yet')

        importParam = disk.get('importParam', Munch())

        data = None
        if memory.format == 'mt':

            if disk.format == 'mt':
                if len(disk.path) > 1:
                    LogException('Not supported')
                mt = hl.read_matrix_table(disk.path[0])    
                data = mt

            elif disk.format == 'vcf':
                mt = hl.import_vcf(disk.path, **importParam)
                data = mt

            elif disk.format == 'plink-bfile':
                if len(disk.path) > 1:
                    LogException('Not supported')
                mt = hl.import_plink(bed=f'{disk.path[0]}.bed', bim=f'{disk.path[0]}.bim', fam=f'{disk.path[0]}.fam', **importParam)
                data = mt

        elif memory.format == 'ht':

            if disk.format == 'ht':
                if len(disk.path) > 1:
                    LogException('Not supported')
                ht = hl.read_table(disk.path[0])    
                data = ht

            elif disk.format == 'tsv':
                importParam['delimiter'] = '\t'
                ht = hl.import_table(disk.path, **importParam)
                data = ht

            elif disk.format == 'csv':
                importParam.delimiter = ','
                ht = hl.import_table(disk.path, **importParam)
                data = ht

        self.SetData(data)

    @D_General
    def Dump(self):
        file = self
        disk = file.disk
        memory = file.memory

        exportParam = disk.get('exportParam', Munch())

        if len(disk.path) > 1:
            LogException('Not supported')

        if memory.format == 'mt':

            if disk.format == 'mt':
                mt = self.data
                mt.write(disk.path[0])   

            elif disk.format == 'vcf':
                mt = self.data
                hl.export_vcf(mt, disk.path[0], **exportParam)

            elif disk.format == 'plink-bfile':
                mt = self.data
                for k in ['call', 'fam_id', 'ind_id', 'pat_id', 'mat_id', 'is_female', 'pheno', 'varid', 'cm_position']:
                    if k in exportParam:
                        exportParam[k] = HailPath([mt]+exportParam[k])
                hl.export_plink(mt, disk.path[0], **exportParam)

        elif memory.format == 'ht':

            if disk.format == 'ht':
                ht = self.data
                ht.write(disk.path[0])    

            elif disk.format == 'tsv':
                exportParam.delimiter = '\t'
                ht = self.data
                ht.export(disk.path[0], **exportParam)

            elif disk.format == 'csv':
                exportParam.delimiter = ','
                ht = self.data
                ht.export(disk.path[0], **exportParam)

        disk.isProduced = True

    @D_General
    def Partitioning(self):
        if not self.isLoaded:
            LogException('Data is not ready to perform common operation')

        memory = self.memory
        if memory.format in ['mt', 'ht']:
            mht = self.data
            mht = mht.repartition(memory.numPartitions)
            if memory.persistence:
                if memory.persistence not in ['DISK_ONLY', 'DISK_ONLY_2', 'MEMORY_ONLY', 'MEMORY_ONLY_2', 'MEMORY_ONLY_SER', 'MEMORY_ONLY_SER_2', 'MEMORY_AND_DISK', 'MEMORY_AND_DISK_2', 'MEMORY_AND_DISK_SER', 'MEMORY_AND_DISK_SER_2', 'OFF_HEAP']:
                    LogException(f'Persistance {memory.persistence} level not supported')
                mht = mht.persist(memory.persistence)
            self.data = mht
    
    @D_General
    def SetData(self, data):
        self.data = data 
        self.memory.isProduced = True

    @D_General
    def CommonOperations(self, operations):
        file = self
        
        mt = self.data

        supportedOperations = [
            'addIndex', # rc
            'aggregate', # rce
            'annotate', # rcge
            'antiJoin', # rc Function
            'semiJoin', # rc Function
            'union', # rc Function
            'collect', # - ??
            'collectBykey', # c ??
            'count', # rcx (x:rc together)
            'distinct', # rc
            'drop',
            'explode', # rc ??
            'filter', # rce
            'groupBy', #rc
            'index', # rcge
            'keyBy', # rc
            'rename',
            'sample', #rc
            'select', # rcge

            'keyBy', # rc

            'repartition',
            'persist',
            'unpersist',

            'addId', # rc same as nnotate col and row this is a alisa
            # Genomic ones
            'maf',
            'ldPrune',
            'splitMulti',
            'forVep'
        ]

        for op in operations:
            if op not in supportedOperations:
                LogException(f'Operation {op} is not supported')

        for op in operations:
            params = operations[op]
            try:
                if op=='rename':
                    mt = mt.rename(params)
                elif op=='drop':
                    mt = mt.drop(*params)
                elif op=='gtOnly' and params==True:
                    mt = mt.select_entries('GT')
                elif op=='annotateRows': ### TBF so that the type is mentiond and enough data to form expression
                    for k in params:
                        if isinstance(params[k], dict):
                            params[k] = hl.struct(**params[k])
                        elif isinstance(params[k], list):
                            if len(params[k]) == len(set(params[k])):
                                params[k] = hl.set(params[k])
                            else:
                                params[k] = hl.array(params[k])
                    mt = mt.annotate_rows(**params)
                elif op=='annotateCols':
                    mt = mt.annotate_cols(**params)
                elif op=='annotateGlobals':
                    mt = mt.annotate_globals(**params)
                elif op=='annotateEntries':
                    mt = mt.annotate_entries(**params)
                elif op=='maf':
                    # Calculate MAF in a coloum (avoid writing on existing cols by using a random col name)
                    mafColName = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(8))
                    mafExpr = {mafColName : hl.min(hl.agg.call_stats(mt.GT, mt.alleles).AF)}
                    mt = mt.annotate_rows(**mafExpr)
                    # Apply filter
                    mt = mt.filter_rows((mt[mafColName] >= params.min) & (mt[mafColName] <= params.max), keep=True)
                elif op=='ldPrune':
                    prunList = hl.ld_prune(mt.GT, **params)
                    mt = mt.filter_rows(hl.is_defined(prunList[mt.row_key]))
                elif op=='subSample':
                    mt = SampleRows(mt, params)
                elif op=='splitMulti':
                    mt = SplitMulti(mt, params)
                elif op=='addId':
                    mt = AddId(mt, params)
                elif op=='forVep' and params==True:
                    mt = ForVep(mt)
                else:
                    LogException(f'Something Wrong in the code')
            except:
                LogException(f'Hail cannot perfom {op} with args: {params}.')
            Log(f'{op} done with agrs: {params}.')

        self.data = mt

    @D_General
    def ExecuteAsInput(self):
        file = self

        if file.get('externalUse'):
            Log('This file is for external use and not loaded')
            return
        
        if file.storageType == 'memory':
            if not file.memory.isProduced:
                LogException('Data has not produced yet')
            else:
                return

        if file.storageType == 'disk':
            self.ExpandWildcardPath()
            if not self.ExistAll():
                LogException('Input file does not exist')
            self.Load()

    @D_General
    def ExecuteAsOutput(self):
        file = self

        if file.get('externalUse'):
            Log('This file is for external use and not loaded')
            return

        if file.storageType == 'memory':
            if not file.memory.isProduced:
                LogException('Data has not produced yet')
            else:
                return

        if file.storageType == 'disk':
            if file.disk.isWildcard:
                LogException('Output file cannot be wildcard')
            if self.ExistAny():
                LogException(f'Output path already exist')
            self.Dump()
