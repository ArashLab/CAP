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
class Workload(PyObj):

    @D_General
    def __init__(self, path, format=None, reset=False):
        super().__init__(path, format, reset)

        if reset:
            self.Clear()

        if 'globConfig' not in self.obj:
            self.obj.globConfig = Munch()
        if 'config' not in self.obj:
            self.obj.config = Munch()

        self.globConfig = self.obj.globConfig
        self.config = self.obj.config
        self.order = self.obj.order
        self.stages = self.obj.stages

        if 'ENVIRONMENT_VARIABLES' in self.config:
            envVars = self.config.ENVIRONMENT_VARIABLES
            for envVar in envVars:
                os.environ[envVar] = str(envVars[envVar])

        if 'runtimes' not in self.globConfig:
            self.globConfig.runtimes = list()
        self.globConfig.runtimes.append(Shared.runtime)
        self.Update()

        CheckShared()
        Shared.update(self.globConfig)
        CheckShared()
        
        if self.order:
            for stageId in self.order:
                if stageId not in self.stages:
                    LogException(f'{stageId} is listed in the "order" but not defined in "stages".')

        for stageId, stage in self.stages.items():
            if 'spec' not in stage:
                print(stage)
                LogException(f'stage {stageId} does not have the "spec"')
            
            # Push stage id and io name into the structure
            stage.spec.id = stageId
            if 'io' in stage:
                for name, io in stage.io.items():
                    io.name = name

            if 'status' not in stage.spec:
                stage.spec.status = 'Initiated'

        self.stageSchema = PyObj(path='StageSchema.json', isInternal=True, isSchema=True)
        self.functionsSchema = PyObj(path='FunctionsSchema.json', isInternal=True)
        
        self.workloadSchema = PyObj(path='WorkloadSchema.json', isInternal=True, isSchema=True)
        self.schema = self.workloadSchema.obj
        self.CheckObject()

        self.Update()

        self.CheckStages()

        Log('Initialised')

    @D_General
    def Clear(self):
        obj = {
            'globConfig': dict(),
            'config': dict(),
            'order': list(),
            'stages': dict()
        }
        self.obj = munchify(obj)
        self.Update()
        Log('Cleared')

    @D_General
    def InferFileFormat(self, io, name):

        def TestFormat(io, name, suffix, format, compression):
            if io.path.endswith(suffix):
                io.format = format
                io.compression = compression
                Log(f'<< io: {name} >> Inferred Format:Compression is {format}:{compression}.')

        if io.pathType == 'file' and 'format' not in io:

            if 'compression' in io:
                LogException(f'<< io: {name} >> When format is not provided (infer format) compression should not be provided.')

            TestFormat(io, name, '.mt', 'mt', 'None')
            TestFormat(io, name, '.ht', 'ht', 'None')
            TestFormat(io, name, '.vcf', 'vcf', 'None')
            TestFormat(io, name, '.vcf.gz', 'vcf', 'gz')
            TestFormat(io, name, '.vcf.bgz', 'vcf', 'bgz')
            TestFormat(io, name, '.tsv', 'tsv', 'None')
            TestFormat(io, name, '.tsv.gz', 'tsv', 'gz')
            TestFormat(io, name, '.tsv.bgz', 'tsv', 'bgz')
            TestFormat(io, name, '.csv', 'csv', 'None')
            TestFormat(io, name, '.csv.gz', 'csv', 'gz')
            TestFormat(io, name, '.csv.bgz', 'csv', 'bgz')
            TestFormat(io, name, '.json', 'json', 'None')
            TestFormat(io, name, '.json.gz', 'json', 'gz')
            TestFormat(io, name, '.json.bgz', 'json', 'bgz')
            TestFormat(io, name, '.bed', 'bed', 'None')
            TestFormat(io, name, '.bim', 'bim', 'None')
            TestFormat(io, name, '.fam', 'fam', 'None')

        if 'format' not in io:
            LogException(f'<< io: {name} >> Format is not provided and cannot be inferred.')

        if 'compression' not in io:
            io.compression = 'None'

    @D_General
    def Checkio(self, stage):

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
                        io.numPartitions = Shared.defaults.numPartitions.default

                    if not (Shared.defaults.numPartitions.min <= io.numPartitions <= Shared.defaults.numPartitions.max):
                        LogException(f'<< io: {name} >> numPartitions {io.numPartitions} must be in range [{Shared.defaults.numPartitions.min}, {Shared.defaults.numPartitions.max}]')

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
    def CheckStage(self, stage):
        """Check if stage is ok to be executed.

        Note:
            - This function should be called just before executing the stage beacuase it checks if the output file exist and prevent overwriting before executing stage.

        Args:
            stage (Stage): the stage to be processed.
        """

        ### Compelete stage schema template by adding arg and io field for the specific function.
        if False:
            try:
                functionsSchema = self.functionsSchema.obj
                stageSchema = self.stageSchema.obj

                for key in ['arg', 'io']:
                    if key not in functionsSchema[stage.spec.function]:
                        LogException(f'The FunctionSchema does not include {key} for {stage.spec.function}')
                    stageSchema['properties'][key] = functionsSchema[stage.spec.function][key]

                jsonschema.Draft7Validator.check_schema(stageSchema)
                jsonschema.validate(instance=stage, schema=stageSchema)
            except jsonschema.exceptions.SchemaError:
                LogException(f'StageSchema is invalid')
            except jsonschema.exceptions.ValidationError:
                LogException(f'Stage is not validated by the schema')

        ##### Check each Input/Output (io).
        self.Checkio(stage)

        LogPrint(f'Stage is Checked')

    @D_General
    def CheckStages(self):
        if self.order:
            for stageId in self.order:
                stage = self.stages[stageId]
                Shared.CurrentStageForLogging = stage
                self.CheckStage(stage)
                Shared.CurrentStageForLogging = None

    @D_General
    def ProcessLiveInput(self, input):
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
    def ProcessLiveInputs(self, stage):
        """Make sure that live input files are loaded in the shared object.

        Args:
            stage (Stage): To be Processed.
        """

        numInput = len([1 for io in stage.io.values() if io.direction == 'input'])
        Log(f'Out of {len(stage.io)} ios {numInput} are inputs.')

        for input in stage.io.values():
            if input.direction == 'input':
                self.ProcessLiveInput(input)

    @D_General
    def ProcessLiveOutput(self, output):
        
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

    @D_General
    def ProcessLiveOutputs(self, stage):
        """Write live output to disk.

        Args:
            stage (Stage): To be processed
        """

        numOutput = len([1 for io in stage.io.values() if io.direction == 'output'])
        logger.info(f'Out of {len(stage.io)} ios {numOutput} are outputs')

        for output in stage.io.values():
            if output.direction == 'output':
                self.ProcessLiveOutput(output)
