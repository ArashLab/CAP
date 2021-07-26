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

#TBF add to be deleted flag to io and delete files after sucessfull compeletion of the workload
class IO(PyObj):
    
    @D_General
    def __init__(self, io):
        if isinstance(io, Munch):
            self.io = io
        elif isinstance(io, dict):
            self.io = munchify(dict(io))

    @D_General
    def TestFormat(io, name, suffix, format, compression):
        if io.path.endswith(suffix):
            io.format = format
            io.compression = compression
            Log(f'<< io: {name} >> Inferred Format:Compression is {format}:{compression}.')

    @D_General
    def Infer(self):

        if io.pathType == 'file' and 'format' not in io:

            if 'compression' in io:
                LogException(f'<< io: {name} >> When format is not provided (infer format) compression should not be provided.')

            self.TestFormat(io, name, '.mt', 'mt', 'None')
            self.TestFormat(io, name, '.ht', 'ht', 'None')
            self.TestFormat(io, name, '.vcf', 'vcf', 'None')
            self.TestFormat(io, name, '.vcf.gz', 'vcf', 'gz')
            self.TestFormat(io, name, '.vcf.bgz', 'vcf', 'bgz')
            self.TestFormat(io, name, '.tsv', 'tsv', 'None')
            self.TestFormat(io, name, '.tsv.gz', 'tsv', 'gz')
            self.TestFormat(io, name, '.tsv.bgz', 'tsv', 'bgz')
            self.TestFormat(io, name, '.csv', 'csv', 'None')
            self.TestFormat(io, name, '.csv.gz', 'csv', 'gz')
            self.TestFormat(io, name, '.csv.bgz', 'csv', 'bgz')
            self.TestFormat(io, name, '.json', 'json', 'None')
            self.TestFormat(io, name, '.json.gz', 'json', 'gz')
            self.TestFormat(io, name, '.json.bgz', 'json', 'bgz')
            self.TestFormat(io, name, '.bed', 'bed', 'None')
            self.TestFormat(io, name, '.bim', 'bim', 'None')
            self.TestFormat(io, name, '.fam', 'fam', 'None')

        if 'format' not in io:
            LogException(f'<< io: {name} >> Format is not provided and cannot be inferred.')

        if 'compression' not in io:
            io.compression = 'None'

    @D_General
    def Check(self):

        Log(f'There are {len(stage.io)} io/s to be checked.')

        for name, io in stage.io.items():
            Log(f'<< io: {name} >> Checking...')
            
            if 'pathType' not in io:
                io.pathType = 'file'
            
            if io.pathType=='file':
                io.path = AbsPath(io.path)
            elif io.pathType=='fileList':
                io.path = [AbsPath(f) for f in io.path]

            self.InferFileFormat(io, name)

            if io.format not in ['mt', 'ht']:
                if 'isAlive' in io:
                    LogException(f'<< io: {name} >> isAlive should not be presented when input format is {io.format}')
            else:
                if 'isAlive' not in io:
                    io.isAlive = True
                if io.isAlive:
                    ### TBF what if the user dont want to repartition at all
                    if 'numPartitions' not in io:
                        io.numPartitions = Shared.numPartitions.default

                    if not (Shared.numPartitions.min <= io.numPartitions <= Shared.numPartitions.max):
                        LogException(f'<< io: {name} >> numPartitions {io.numPartitions} must be in range [{Shared.numPartitions.min}, {Shared.numPartitions.max}]')

                    for key in ['toBeCached', 'toBeCounted']:
                        if key not in io:
                            io[key] = True

            if 'isAlive' in io and not io.isAlive:
                for key in ['numPartitions', 'toBeCached', 'toBeCounted']:
                    if key in io:
                        LogException(f'<< io: {name} >> When isAlive is explicitly set to false, "{key}" should not be presented at io.')

        # TBF this file existance check needs to be reviewd
        if stage.spec.status != 'Completed':
            for name, io in stage.io.items():
                if io.direction == 'output':
                    if io.format == 'bfile':
                        cond = any([FileExist(f'{io.path}{suffix}') for suffix in ['bed', 'bim', 'fam']])
                    else:
                        cond = FileExist(io.path)

                    if cond:
                        LogException(f'<< io: {name} >> Output path (or plink bfile prefix) {io.path} already exist in the file system')


    @D_General
    def ProcessAsInput(self, input):
        Log(f'<< io: {input.name} >> is {JsonDumps(input)}.')
        if 'isAlive' in input and input.isAlive:
            if input.pathType!='fileList':
                paths = [input.path]
            else:
                paths = input.path
            for path in paths:    
                if path not in Shared.data:
                    Log(f'<< io: {input.name} >> Loading.')
                    try:
                        if input.format == 'ht':
                            mht = hl.read_table(path)
                        elif input.format == 'mt':
                            mht = hl.read_matrix_table(path)
                        else:
                            pass  # Already handled in Checkio
                    except:
                        LogException(f'<< io: {input.name} >> Cannot read input form {path}.')
                    else:
                        Shared.data[path] = mht
                        Log('<< io: {name} >> Loaded.')
                else:
                    mht = Shared.data[path]
                    Log(f'<< io: {input.name} >> Preloaded.')

                if input.numPartitions and mht.n_partitions() != input.numPartitions:
                    np = mht.n_partitions()
                    mht = mht.repartition(input.numPartitions)
                    Log(f'<< io: {input.name} >> Repartitioned from {np} to {input.numPartitions}.')
                if input.toBeCached:
                    mht = mht.cache()
                    Log(f'<< io: {input.name} >> Cached.')
                if input.toBeCounted:
                    input.count = Count(mht)
                    Log(f'<< io: {input.name} >> Counted.')
                    self.Update()


    @D_General
    def ProcessAsOutput(self, output):
        
        Log(f'<< io: {output.name} >> is {JsonDumps(output)}')

        if 'isAlive' in output and output.isAlive:
            if 'data' in output:
                mht = output.data
            else:
                LogException(f'<< io: {output.name} >> No "data" field is provided.')

            if output.path in Shared.data:
                LogException(f'<< io: {output.name} >> Output path {output.path} alredy exist in the shared.')

            if output.numPartitions and mht.n_partitions() != output.numPartitions:
                np = mht.n_partitions()
                mht = mht.repartition(output.numPartitions)
                Log(f'<< io: {output.name} >> Repartitioned from {np} to {output.numPartitions}.')

            if output.toBeCached:
                mht = mht.cache()
                Log(f'<< io: {output.name} >> Cached.')

            if output.toBeCounted:
                output.count = Count(mht)
                Log(f'<< io: {output.name} >> Counted.')
                self.Update()

            Shared.data[output.path] = mht
            Log(f'<< io: {output.name} >> Added to shared.')

            if output.format in ['ht', 'mt']:
                mht.write(output.path, overwrite=False)
                Log(f'<< io: {output.name} >> Dumped.')
            else:
                pass  # Already handled in Checkio

    