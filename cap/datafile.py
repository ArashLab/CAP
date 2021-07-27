from munch import Munch, munchify
import jsonschema
import os

import hail as hl
from .logutil import *
from .common import *
from .helper import *
from .decorators import *
from .pyobj import PyObj
from .shared import Shared

if __name__ == '__main__':
    print('This module is not executable. Import this module in your program.')
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

        if file.storageType == 'disk' and ('disk' not in file or 'memory' not in file):
            LogException('`disk` and `memory` should present when storage type is `disk`')

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
        '.fam': ('fam', 'None')
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
            if path.startsswith(prefix):
                return self.prefixMapper[prefix]
        return Shared.defaults.fileSystem

    @D_General
    def InferPath(self):
        Inferred = Munch()
        Inferred.format = None
        Inferred.compression = None
        Inferred.fileSystem = None
        Inferred.isList = None
        Inferred.listLen = None
        Inferred.isParallel = None

        self.file.disk.inferred = Inferred

        path = self.file.disk.path

        # Check if path is a list
        if isinstance(path, string):
            Inferred.isList = False
        elif isinstance(path, list):
            if not len(path):
                LogException('path array should not be empty')
            for p in path:
                if not isinstance(p, string):
                    LogException(f'Each path must be of type string but found {p} of type {type(p)}.')
            Inferred.isList = True
            Inferred.listLen = len(path)
        else:
            LogException(f'Path of type {type(path)} is not supported')

        # If path list, infer the first path in the list
        if Inferred.isList:
            Inferred.format, Inferred.compression = self.InferFormat(path[0])
            Inferred.fileSystem = self.InferFileSystem(path[0])
        else:
            Inferred.format, Inferred.compression = self.InferFormat(path)
            Inferred.fileSystem = self.InferFileSystem(path)

        # If path list, make sure the all paths in the list infered the same
        if Inferred.isList:
            for i, p in enumerate(path):
                # Inffer the i^th path
                format, compression = self.InferFormat(path)
                fileSystem = self.InferFileSystem(path)

                # Check consistancy of the infered data
                if format != Inferred.format:
                    LogException(f'{i}th path format mismatch: {format} is not {Inferred.format}')
                if compression != Inferred.compression:
                    LogException(f'{i}th path compression mismatch: {compression} is not {Inferred.compression}')
                if fileSystem != Inferred.fileSystem:
                    LogException(f'{i}th path fileSystem mismatch: {fileSystem} is not {Inferred.fileSystem}')

        # Check consistensy of the inferred and given information
        disk = self.file.disk
        for k in disk.inferred:
            if k in disk:
                if disk.inferred[k] and disk[k] != disk.inferred[k]:
                    LogException(f'Infered `{k}` ({disk.inferred[k]}) is different from the given information ({disk[k]})')
            else:
                disk[k] = disk.inferred[k]
                Log(f'Infered `{k}` ({disk.inferred[k]}) is used.')

    @D_General
    def ProcessPathInternal(self, inPath):

        # replace ~ and ${VAR} with actual values
        path = os.path.expandvars(inPath)

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

        Log(f'Absolute path of {inPath} is {path}')
        return path

    @D_General
    def ProcessPath(self):
        disk = self.file.disk
        if disk.isList:
            disk.path = [self.ProcessPathInternal(p) for p in disk.path]
        else:
            disk.path = self.ProcessPathInternal(disk.path)
        disk.isPrefix = True

        if 'localMode' in Shared.defaults and Shared.defaults.localMode:
            disk.path = self.GetLocalPath()
            disk.isPrefix = False

    @D_General
    def GetLocalPath(self):
        disk = self.file.disk
        if disk.fileSyste != 'local':
            LogException('Cannot produce local path for non-local storage type')

        if disk.isList:
            path = [p[7:] if p.lower().startswith('file://') else p for p in disk.path]
        else:
            path = disk.path[7:] if disk.path.lower().startswith('file://') else disk.path

        return path

    # @D_General
    # def FileExist(path, silent=False):
    #     if path.lower().startswith('hdfs://'):
    #         return not subprocess.run(['hdfs', 'dfs', '-test', '-e', path]).returncode
    #     else:
    #         path = GetLocalPath(path, silent)
    #         return os.path.exists(path)

    # @D_General
    # def WildCardPath(path):
    #     path = GetLocalPath(path)
    #     fileList = glob.glob(path)
    #     Log(f'{len(fileList)} files are found in {path}')
    #     return fileList

# @D_General
# def Check(self):

#     Log(f'There are {len(stage.io)} io/s to be checked.')

#     for name, io in stage.io.items():
#         Log(f'<< io: {name} >> Checking...')

#         if 'pathType' not in io:
#             io.pathType = 'file'

#         if io.pathType=='file':
#             io.path = AbsPath(io.path)
#         elif io.pathType=='fileList':
#             io.path = [AbsPath(f) for f in io.path]

#         self.InferFileFormat(io, name)

#         if io.format not in ['mt', 'ht']:
#             if 'isAlive' in io:
#                 LogException(f'<< io: {name} >> isAlive should not be presented when input format is {io.format}')
#         else:
#             if 'isAlive' not in io:
#                 io.isAlive = True
#             if io.isAlive:
#                 ### TBF what if the user dont want to repartition at all
#                 if 'numPartitions' not in io:
#                     io.numPartitions = Shared.numPartitions.default

#                 if not (Shared.numPartitions.min <= io.numPartitions <= Shared.numPartitions.max):
#                     LogException(f'<< io: {name} >> numPartitions {io.numPartitions} must be in range [{Shared.numPartitions.min}, {Shared.numPartitions.max}]')

#                 for key in ['toBeCached', 'toBeCounted']:
#                     if key not in io:
#                         io[key] = True

#         if 'isAlive' in io and not io.isAlive:
#             for key in ['numPartitions', 'toBeCached', 'toBeCounted']:
#                 if key in io:
#                     LogException(f'<< io: {name} >> When isAlive is explicitly set to false, "{key}" should not be presented at io.')

#     # TBF this file existance check needs to be reviewd
#     if stage.spec.status != 'Completed':
#         for name, io in stage.io.items():
#             if io.direction == 'output':
#                 if io.format == 'bfile':
#                     cond = any([FileExist(f'{io.path}{suffix}') for suffix in ['bed', 'bim', 'fam']])
#                 else:
#                     cond = FileExist(io.path)

#                 if cond:
#                     LogException(f'<< io: {name} >> Output path (or plink bfile prefix) {io.path} already exist in the file system')


# @D_General
# def ProcessAsInput(self, input):
#     Log(f'<< io: {input.name} >> is {JsonDumps(input)}.')
#     if 'isAlive' in input and input.isAlive:
#         if input.pathType!='fileList':
#             paths = [input.path]
#         else:
#             paths = input.path
#         for path in paths:
#             if path not in Shared.data:
#                 Log(f'<< io: {input.name} >> Loading.')
#                 try:
#                     if input.format == 'ht':
#                         mht = hl.read_table(path)
#                     elif input.format == 'mt':
#                         mht = hl.read_matrix_table(path)
#                     else:
#                         pass  # Already handled in Checkio
#                 except:
#                     LogException(f'<< io: {input.name} >> Cannot read input form {path}.')
#                 else:
#                     Shared.data[path] = mht
#                     Log('<< io: {name} >> Loaded.')
#             else:
#                 mht = Shared.data[path]
#                 Log(f'<< io: {input.name} >> Preloaded.')

#             if input.numPartitions and mht.n_partitions() != input.numPartitions:
#                 np = mht.n_partitions()
#                 mht = mht.repartition(input.numPartitions)
#                 Log(f'<< io: {input.name} >> Repartitioned from {np} to {input.numPartitions}.')
#             if input.toBeCached:
#                 mht = mht.cache()
#                 Log(f'<< io: {input.name} >> Cached.')
#             if input.toBeCounted:
#                 input.count = Count(mht)
#                 Log(f'<< io: {input.name} >> Counted.')
#                 self.Update()


# @D_General
# def ProcessAsOutput(self, output):

#     Log(f'<< io: {output.name} >> is {JsonDumps(output)}')

#     if 'isAlive' in output and output.isAlive:
#         if 'data' in output:
#             mht = output.data
#         else:
#             LogException(f'<< io: {output.name} >> No "data" field is provided.')

#         if output.path in Shared.data:
#             LogException(f'<< io: {output.name} >> Output path {output.path} alredy exist in the shared.')

#         if output.numPartitions and mht.n_partitions() != output.numPartitions:
#             np = mht.n_partitions()
#             mht = mht.repartition(output.numPartitions)
#             Log(f'<< io: {output.name} >> Repartitioned from {np} to {output.numPartitions}.')

#         if output.toBeCached:
#             mht = mht.cache()
#             Log(f'<< io: {output.name} >> Cached.')

#         if output.toBeCounted:
#             output.count = Count(mht)
#             Log(f'<< io: {output.name} >> Counted.')
#             self.Update()

#         Shared.data[output.path] = mht
#         Log(f'<< io: {output.name} >> Added to shared.')

#         if output.format in ['ht', 'mt']:
#             mht.write(output.path, overwrite=False)
#             Log(f'<< io: {output.name} >> Dumped.')
#         else:
#             pass  # Already handled in Checkio
