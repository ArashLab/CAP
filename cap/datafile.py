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
        if self.storageType == 'disk':
            self.InferPath()
            self.isInferred = True
            self.ProcessPath()

        self.InferMemory()

    @D_General
    def SetData(self, data):
        self.__data = data
        format = dataTypeMapper.get(type(data))
        #TBF check for null format
        if format in self.memory:
            if self.memory.format != format:
                LogException('XXX')
        else:
            self.memory.format = format
        self.memory.isProduced = True
        if self.storageType == 'disk':
            self.Dump()
            self.disk.isProduced = True

    @D_General
    def GetData(self):
        if not self.memory.get('isProduced'):
            if self.storageType == 'disk':
                if self.disk.get('isProduced'):
                    self.Load()
                    self.memory.isProduced = True
        return self.__data

    @D_General
    def Infer(self):

        if 'storageType' not in self:
            if 'disk' in self:
                self.storageType = 'disk'
            else:
                self.storageType = 'memory'

        if self.storageType == 'memory' and 'disk' in self:
            LogException('`disk` should not present when storage type is `memory`')

        if self.storageType == 'disk':
            if 'disk' not in self:
                LogException('`disk` should present when storage type is `disk`')
            if 'isProduced' not in self.disk:
                self.disk.isProduced = False

        if 'disk' in self and 'path' not in self.disk:
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
    def Load(self):
        disk = self.disk
        memory = self.memory

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
        self.__data = data

    @D_General
    def Dump(self):
        disk = self.disk
        memory = self.memory
        data = self.__data
        exportParam = disk.get('exportParam', Munch())

        if len(disk.path) > 1:
            LogException('Not supported')

        if memory.format == 'mt':
            mt = data

            if disk.format == 'mt':
                mt.write(disk.path[0])   

            elif disk.format == 'vcf':
                hl.export_vcf(mt, disk.path[0], **exportParam)

            elif disk.format == 'plink-bfile':
                for k in ['call', 'fam_id', 'ind_id', 'pat_id', 'mat_id', 'is_female', 'pheno', 'varid', 'cm_position']:
                    if k in exportParam:
                        exportParam[k] = HailPath([mt]+exportParam[k])
                hl.export_plink(mt, disk.path[0], **exportParam)

        elif memory.format == 'ht':
            ht = data

            if disk.format == 'ht':
                ht.write(disk.path[0])    

            elif disk.format == 'tsv':
                exportParam.delimiter = '\t'
                ht.export(disk.path[0], **exportParam)

            elif disk.format == 'csv':
                exportParam.delimiter = ','
                ht.export(disk.path[0], **exportParam)

            elif disk.format == 'sql':
                ht = FlattenTable(ht)
                try:
                    # TBF overwrite? really?
                    spdf = ht.to_spark()
                    spdf = spdf.fillna(0)
                    spdf.write.format('jdbc').options(**exportParam).mode('overwrite').save()
                except:
                    LogException('Hail cannot write data into MySQL database')
                Log(f'Data is exported to MySQL')