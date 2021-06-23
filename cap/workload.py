from munch import Munch, munchify
import jsonschema

import hail as hl
from .logutil import *
from .common import *
from .helper import *
from .decorators import *
from .pyobj import PyObj
from .shared import Shared

if __name__ == '__main__':
    print('This module is not executable. Please import this module in your program.')
    exit(0)

#TBF add to be deleted flag to inout and delete files after sucessfull compeletion of the workload
class Workload(PyObj):

    @D_General
    def __init__(self, path, format=None, reset=False):
        super().__init__(path, format, reset)

        if reset:
            self.Clear()

        if 'globConfig' in self.obj:
            self.globConfig = self.obj.globConfig
        else:
            self.globConfig = Munch()
        if 'config' in self.obj:
            self.config = self.obj.config
        else:
            self.config = Munch()
        self.order = self.obj.order
        self.stages = self.obj.stages

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
            stage.spec.id = stageId
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
    def InferFileFormat(self, inout, name):

        def TestFormat(inout, name, suffix, format, compression):
            if inout.path.endswith(suffix):
                inout.format = format
                inout.compression = compression
                Log(f'<< inout: {name} >> Inferred Format:Compression is {format}:{compression}.')

        if inout.pathType == 'file' and 'format' not in inout:

            if 'compression' in inout:
                LogException(f'<< inout: {name} >> When format is not provided (infer format) compression should not be provided.')

            TestFormat(inout, name, '.mt', 'mt', 'None')
            TestFormat(inout, name, '.ht', 'ht', 'None')
            TestFormat(inout, name, '.vcf', 'vcf', 'None')
            TestFormat(inout, name, '.vcf.gz', 'vcf', 'gz')
            TestFormat(inout, name, '.vcf.bgz', 'vcf', 'bgz')
            TestFormat(inout, name, '.tsv', 'tsv', 'None')
            TestFormat(inout, name, '.tsv.gz', 'tsv', 'gz')
            TestFormat(inout, name, '.tsv.bgz', 'tsv', 'bgz')
            TestFormat(inout, name, '.csv', 'csv', 'None')
            TestFormat(inout, name, '.csv.gz', 'csv', 'gz')
            TestFormat(inout, name, '.csv.bgz', 'csv', 'bgz')
            TestFormat(inout, name, '.json', 'json', 'None')
            TestFormat(inout, name, '.json.gz', 'json', 'gz')
            TestFormat(inout, name, '.json.bgz', 'json', 'bgz')
            TestFormat(inout, name, '.bed', 'bed', 'None')
            TestFormat(inout, name, '.bim', 'bim', 'None')
            TestFormat(inout, name, '.fam', 'fam', 'None')

        if 'format' not in inout:
            LogException(f'<< inout: {name} >> Format is not provided and cannot be inferred.')

        if 'compression' not in inout:
            inout.compression = 'None'

    @D_General
    def CheckInout(self, stage):
        """Check all inout of the `stage`.

        Args:
            stage (Stage): the stage to be processed.
        """

        Log(f'There are {len(stage.inout)} inout/s to be checked.')

        for name, inout in stage.inout.items():
            Log(f'<< inout: {name} >> Checking...')
            if not (inout.path.startswith('hdfs:///') or inout.path.startswith('file:///')):
                inout.path = AbsPath(inout.path)
            
            if 'pathType' not in inout:
                inout.pathType = 'file'

            self.InferFileFormat(inout, name)

            if inout.format not in ['mt', 'ht']:
                if 'isAlive' in inout:
                    LogException(f'<< inout: {name} >> isAlive should not be presented when input format is {inout.format}')
            else:
                if 'isAlive' not in inout:
                    inout.isAlive = True
                if inout.isAlive:
                    ### TBF what if the user dont want to repartition at all
                    if 'numPartitions' not in inout:
                        inout.numPartitions = Shared.numPartitions.default

                    if not (Shared.numPartitions.min <= inout.numPartitions <= Shared.numPartitions.max):
                        LogException(f'<< inout: {name} >> numPartitions {inout.numPartitions} must be in range [{Shared.numPartitions.min}, {Shared.numPartitions.max}]')

                    for key in ['toBeCached', 'toBeCounted']:
                        if key not in inout:
                            inout[key] = True

            if 'isAlive' in inout and not inout.isAlive:
                for key in ['numPartitions', 'toBeCached', 'toBeCounted']:
                    if key in inout:
                        LogException(f'<< inout: {name} >> When isAlive is explicitly set to false, "{key}" should not be presented at inout.')

        # TBF this file existance check needs to be reviewd
        if stage.spec.status != 'Completed':
            for name, inout in stage.inout.items():
                if inout.direction == 'output':
                    if inout.format == 'bfile':
                        cond = any([FileExist(f'{inout.path}{suffix}') for suffix in ['bed', 'bim', 'fam']])
                    else:
                        cond = FileExist(inout.path)

                    if cond:
                        LogException(f'<< inout: {name} >> Output path (or plink bfile prefix) {inout.path} already exist in the file system')

    @D_General
    def CheckStage(self, stage):
        """Check if stage is ok to be executed.

        Note:
            - This function should be called just before executing the stage beacuase it checks if the output file exist and prevent overwriting before executing stage.

        Args:
            stage (Stage): the stage to be processed.
        """

        # Compelete stage schema template by adding arg and inout field for the specific function.
        try:
            functionsSchema = self.functionsSchema.obj
            stageSchema = self.stageSchema.obj

            for key in ['arg', 'inout']:
                if key not in functionsSchema[stage.spec.function]:
                    LogException(f'The FunctionSchema does not include {key} for {stage.spec.function}')
                stageSchema['properties'][key] = functionsSchema[stage.spec.function][key]

            jsonschema.Draft7Validator.check_schema(stageSchema)
            jsonschema.validate(instance=stage, schema=stageSchema)
        except jsonschema.exceptions.SchemaError:
            LogException(f'StageSchema is invalid')
        except jsonschema.exceptions.ValidationError:
            LogException(f'Stage is not validated by the schema')

        # Step 3: Check each Input/Output (inout).
        self.CheckInout(stage)

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
    def ProcessLiveInputs(self, stage):
        """Make sure that live input files are loaded in the shared object.

        Args:
            stage (Stage): To be Processed.
        """

        numInput = len([1 for inout in stage.inout.values() if inout.direction == 'input'])
        Log(f'Out of {len(stage.inout)} inouts {numInput} are inputs.')

        for name, input in stage.inout.items():
            if input.direction == 'input':
                Log(f'<< inout: {name} >> is {JsonDumps(input)}.')
                if 'isAlive' in input and input.isAlive:
                    if input.path not in Shared:
                        Log(f'<< inout: {name} >> Loading.')
                        try:
                            if input.format == 'ht':
                                mht = hl.read_table(input.path)
                            elif input.format == 'mt':
                                mht = hl.read_matrix_table(input.path)
                            else:
                                pass  # Already handled in CheckInout
                        except:
                            LogException(f'<< inout: {name} >> Cannot read input form {input.path}.')
                        else:
                            Shared[input.path] = mht
                            Log('<< inout: {name} >> Loaded.')
                    else:
                        mht = Shared[input.path]
                        Log(f'<< inout: {name} >> Preloaded.')

                    if input.numPartitions and mht.n_partitions() != input.numPartitions:
                        np = mht.n_partitions()
                        mht = mht.repartition(input.numPartitions)
                        Log(f'<< inout: {name} >> Repartitioned from {np} to {input.numPartitions}.')
                    if input.toBeCached:
                        mht = mht.cache()
                        Log(f'<< inout: {name} >> Cached.')
                    if input.toBeCounted:
                        input.count = Count(mht)
                        Log(f'<< inout: {name} >> Counted.')

    @D_General
    def ProcessLiveOutputs(self, stage):
        """Write live output to disk.

        Args:
            stage (Stage): To be processed
        """

        numOutput = len([1 for inout in stage.inout.values() if inout.direction == 'output'])
        logger.info(f'Out of {len(stage.inout)} inouts {numOutput} are outputs')

        for name, output in stage.inout.items():
            if output.direction == 'output':
                Log(f'<< inout: {name} >> is {JsonDumps(output)}')

                if 'isAlive' in output and output.isAlive:
                    if 'data' in output:
                        mht = output.data
                    else:
                        LogException(f'<< inout: {name} >> No "data" field is provided.')

                    if output.path in Shared:
                        LogException(f'<< inout: {name} >> Output path {output.path} alredy exist in the shared.')

                    if output.numPartitions and mht.n_partitions() != output.numPartitions:
                        np = mht.n_partitions()
                        mht = mht.repartition(output.numPartitions)
                        Log(f'<< inout: {name} >> Repartitioned from {np} to {output.numPartitions}.')

                    if output.toBeCached:
                        mht = mht.cache()
                        Log(f'<< inout: {name} >> Cached.')

                    if output.toBeCounted:
                        output.count = Count(mht)
                        Log(f'<< inout: {name} >> Counted.')

                    Shared[output.path] = mht
                    Log(f'<< inout: {name} >> Added to shared.')

                    if output.format in ['ht', 'mt']:
                        mht.write(output.path, overwrite=False)
                        Log(f'<< inout: {name} >> Dumped.')
                    else:
                        pass  # Already handled in CheckInout
