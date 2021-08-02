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

class DataFile:

    @D_General
    def __init__(self, file):
        if isinstance(file, Munch):
            self.file = file
        else:
            LogException(f'`file` must be of type Munch but the type is `{type(file)}`.')

        self.Infer()
        self.InferPath()
        self.isInferred = True
        self.ProcessPath()

        self.InferMemory()

    @D_General
    def Infer(self):
        file = self.file

        if 'isInferred' in file and file.isInferred:
            Log('File has been already infered.')
            return

        if 'storageType' not in file:
            if 'disk' in file:
                file.storageType = 'disk'
            elif 'memory' in file:
                file.storageType = 'memory'
            else:
                LogException('`disk` or `memory` must exist in a `file`')

        if file.storageType == 'memory' and 'disk' in file:
            LogException('`disk` should not present when storage type is `memory`')

        if file.storageType == 'disk' and 'disk' not in file:
            LogException('`disk` should present when storage type is `disk`')

        if 'disk' in file and 'path' not in file.disk:
            LogException('`path` must present in `disk`')

        if 'isLoaded' not in file:
            file.isLoaded = False

        if 'isDumped' not in file:
            file.isDumped = False

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

        self.file.disk.inferred = inferred

        disk = self.file.disk
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
        disk = self.file.disk

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
                fs = self.file.disk.fileSystem
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

        disk = self.file.disk
        if disk.isWildcard:
            if self.file.disk.fileSystem != 'local':
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
        if self.file.disk.fileSystem != 'local':
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
        file = self.file

        if file.storageType == 'disk':
            if not 'memory' in file or not file.memory:  # memory could exist but an empty field
                file.memory = Munch()
            memory = file.memory

            if not 'inferred' in memory:
                memory.inferred = Munch()
            inferred = memory.inferred

            if file.disk.format in self.formatMapper:
                inferred.formats = self.formatMapper[file.disk.format]
            else:
                inferred.formats = None

            if 'format' not in memory:
                if inferred.formats:
                    memory.format = inferred.formats[0]
                else:
                    Log('Could not identify memory format')
            else:
                if memory.format not in inferred.formats:
                    LogException(f'Memory format `{memory.format}` is not supported for disk format `{file.disk.format}`. Acceptable memory formats are `{inferred.formats}`')
        else:
            if 'format' not in file.memory:
                LogException('memory format must present')

        memory = file.memory

        if 'persistence' not in memory:
            memory.persistence = Shared.defaults.persistence

        if 'numPartitions' not in memory:
            memory.numPartitions = Shared.defaults.numPartitions.default

    @D_General
    def ExistInternal(self, path):
        fs = self.file.disk.fileSystem
        if fs in ['aws', 'google']:
            LogException(f'`{fs}` not supported')
        elif fs == 'hadoop':
            return not subprocess.run(['hdfs', 'dfs', '-test', '-e', path]).returncode
        elif fs == 'local':
            path = self.GetLocalPath(path)
            return os.path.exists(path)

    @D_General
    def ExistAll(self):
        disk = self.file.disk
        return all([self.ExistInternal(p) for p in disk.path])

    @D_General
    def ExistAny(self):
        disk = self.file.disk
        return any([self.ExistInternal(p) for p in disk.path])
        
    @D_General
    def ExpandWildcardPathInternal(self, path):
        path = self.GetLocalPath(path)
        fileList = glob.glob(path)
        Log(f'{len(fileList)} files are found in {path}')
        return fileList

    @D_General
    def Load(self):
        file = self.file
        if 'isLoaded' in file and file.isLoaded:
            Log('Data file is already loaded.')
            return

        disk = file.disk
        memory = file.memory

        importParam = disk.importParam if 'importParam' in disk else {}

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
                importParam['delimiter'] = ','
                ht = hl.import_table(disk.path, **importParam)
                data = ht

        self.SetData(data)

    @D_General
    def Dump(self):
        file = self.file
        if 'isDumped' in file and file.isDumped:
            LogException('Data file is already Dumped.')

        disk = file.disk
        memory = file.memory

        exportParam = disk.exportParam if 'exportParam' in disk else {}

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
                exportParam['delimiter'] = '\t'
                ht = self.data
                ht.export(disk.path[0], **exportParam)

            elif disk.format == 'csv':
                exportParam['delimiter'] = ','
                ht = self.data
                ht.export(disk.path[0], **exportParam)

        file.isDumped = True

    @D_General
    def Partitioning(self):
        if not self.file.isLoaded:
            LogException('Data is not ready to perform common operation')

        memory = self.file.memory
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
        if self.file.isLoaded:
            LogException('This file is alredy loaded')
        
        self.data = data 
        self.file.isLoaded = True

        self.Partitioning()
        self.CommonOperations()
        self.Partitioning()  ## TBF

    @D_General
    def CommonOperations(self):
        file = self.file

        if 'commonOperations' in self.file:
            if not file.isLoaded:
                LogException('Data is not ready to perform common operation')
            if file.isDumped:
                LogException('Data has been already dumped. Applying common operation will take no effect.')


            if file.memory.format == 'mt':
                self.CommonOperationsMatrixTable()
            elif file.memory.format == 'ht':
                self.CommonOperationsTable()

    @D_General
    def CommonOperationsTable(self):
        file = self.file
        if file.memory.format != 'mt':
            LogException('This function only act on matrix tables')
        
        mt = self.data
        operations = self.file.commonOperations

        print(operations)  # To be implemented

        self.data = mt

    @D_General
    def CommonOperationsMatrixTable(self):

        file = self.file
        if file.memory.format != 'mt':
            LogException('This function only act on matrix tables')
        
        mt = self.data
        operations = self.file.commonOperations

        supportedOperations = [
            'gtOnly',
            'drop',
            'rename',
            'annotateRows',
            'annotateCols',
            'annotateGlobals',
            'annotateEntries',
            'maf',
            'ldPrune',
            'subSample',
            'splitMulti',
            'addId',
            'forVep'
        ]
        # KeyBy KeyBY row and col

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

