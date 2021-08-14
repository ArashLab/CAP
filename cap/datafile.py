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
from .pathlist import PathList

if __name__ == '__main__':
    print('This module is not executable.')
    exit(0)

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

class DataFile(Munch):

    def __init__(self, dataFile):
        self.update(dataFile)
        self.InitialCheck()
        if self.storageType == 'disk':
            self.disk.paths = PathList(self.disk.path)

        # TBF
        self.disk.format = self.disk.paths.paths[0].format
        self.disk.compression = self.disk.paths.paths[0].compression

        self.InferMemory()

    def InitialCheck(self):
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
            elif 'isProduced' not in self.disk:
                self.disk.isProduced = False

        if 'disk' in self and 'path' not in self.disk:
            LogException('`path` must present in `disk`')

    def InferMemory(self):
        if not self.get('memory'):  # Memory could exist but an empty field
            self.memory = Munch()
        memory = self.memory

        memory.inferred = Munch()
        inferred = memory.inferred

        if self.storageType == 'disk':
            inferred.formats = formatMapper.get(self.disk.format, [None])
            if not memory.get('format'):
                memory.format = inferred.formats[0]
            else:
                if memory.format not in inferred.formats:
                    LogException(f'Memory format `{memory.format}` is not supported for disk format `{self.disk.format}`. Acceptable memory formats are `{inferred.formats}`')
            memory.isProduced = False  # is always false at the begining

    def SetData(self, data):
        self.__data = data
        format = dataTypeMapper.get(type(data))
        # TBF check for null format
        if format in self.memory:
            if self.memory.format != format:
                LogException(f'data type `{format}` is not as expected `{self.memory.format}`')
        else:
            self.memory.format = format
        self.memory.isProduced = True
        if self.storageType == 'disk':
            self.Dump()
            self.disk.paths.ExpandWildCard()
            if not self.disk.paths.ExistAll():
                LogException('File are not dumpd')
            self.disk.isProduced = True

    def GetData(self):
        if not self.memory.get('isProduced'):
            if self.storageType == 'disk':
                if self.disk.get('isProduced'):
                    self.disk.paths.ExpandWildCard()
                    if self.disk.paths.ExistAll():
                        self.Load()
                    else:
                        LogException('Data File does not exist')
                    self.memory.isProduced = True
                else:
                    LogException('Can not load data file from disk beacuse it is not produced.')
            else:
                LogException('Data file is not produced and not connected to disk')
        return self.__data

    def Load(self):
        disk = self.disk
        memory = self.memory

        importParam = disk.get('importParam', Munch())

        data = None
        if memory.format == 'mt':

            if disk.format == 'mt':
                if len(disk.path) > 1:
                    LogException('Not supported')
                mt = hl.read_matrix_table(disk.paths.path.path)
                data = mt

            elif disk.format == 'vcf':
                mt = hl.import_vcf(disk.paths.solos, **importParam)
                data = mt

            # elif disk.format == 'plink-bfile':
            #     if len(disk.path) > 1:
            #         LogException('Not supported')
            #     mt = hl.import_plink(bed=f'{disk.path[0]}.bed', bim=f'{disk.path[0]}.bim', fam=f'{disk.path[0]}.fam', **importParam)
            #     data = mt

        elif memory.format == 'ht':

            if disk.format == 'ht':
                if len(disk.path) > 1:
                    LogException('Not supported')
                ht = hl.read_table(disk.paths.path.path)
                data = ht

            elif disk.format == 'tsv':
                importParam['delimiter'] = '\t'
                ht = hl.import_table(disk.paths.solos, **importParam)
                data = ht

            elif disk.format == 'csv':
                importParam.delimiter = ','
                ht = hl.import_table(disk.paths.solos, **importParam)
                data = ht
        self.__data = data

    def Dump(self):
        disk = self.disk
        memory = self.memory
        data = self.__data
        exportParam = disk.get('exportParam', Munch())

        if memory.format == 'mt':
            mt = data

            if disk.format == 'mt':
                mt.write(disk.paths.path.path)

            elif disk.format == 'vcf':
                hl.export_vcf(mt, disk.paths.path.path, **exportParam)

            # elif disk.format == 'plink-bfile':
            #     for k in ['call', 'fam_id', 'ind_id', 'pat_id', 'mat_id', 'is_female', 'pheno', 'varid', 'cm_position']:
            #         if k in exportParam:
            #             exportParam[k] = HailPath([mt]+exportParam[k])
            #     hl.export_plink(mt, disk.path[0], **exportParam)

        elif memory.format == 'ht':
            ht = data

            if disk.format == 'ht':
                ht.write(disk.paths.path.path)

            elif disk.format == 'tsv':
                exportParam.delimiter = '\t'
                ht.export(disk.paths.path.path, **exportParam)

            elif disk.format == 'csv':
                exportParam.delimiter = ','
                ht.export(disk.paths.path.path, **exportParam)

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
