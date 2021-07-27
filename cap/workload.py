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
from .datafile import DataFile

if __name__ == '__main__':
    print('This module is not executable. Import this module in your program.')
    exit(0)

# TBF add to be deleted flag to io and delete files after sucessfull compeletion of the workload


class Workload(PyObj):

    @D_General
    def __init__(self, path, format=None):

        # Load the workload into the self.obj
        super().__init__(path, format)

        # Check against workload schema
        self.workloadSchema = PyObj(path='WorkloadSchema.json', isInternal=True, isSchema=True)
        self.schema = self.workloadSchema.obj
        self.CheckObject()
        Log('Workload is checked against schema.')

        # Loading stage schemas
        self.stageSchema = PyObj(path='StageSchema.json', isInternal=True, isSchema=True)
        self.functionsSchema = PyObj(path='FunctionsSchema.json', isInternal=True)
        Log('Stage schemas are loaded.')

        # Handeling default values
        # Defaults are read only values and to be accessed through Shared
        if 'defaults' in self.obj:
            # If defaults exist in both workload and shared
            # workload is on proyority
            Shared.defaults.update(self.obj.defaults)
        else:
            self.obj.defaults = Munch()
        self.obj.defaults.update(Shared.defaults)
        CheckDefaults()
        Log('Default values are updated and checked as below.')
        Log(Shared.defaults)

        # Shortcut to stages, files, and executionPlan
        self.stages = self.obj.stages
        self.files = self.obj.files
        if self.obj.executionPlan:
            self.executionPlan = self.obj.executionPlan
        else:
            self.executionPlan = []

        # Environmental variables to be set
        if 'env' in self.obj:
            envVars = self.obj.env
            for envVar in envVars:
                os.environ[envVar] = str(envVars[envVar])
        Log('Environmental Variables are set.')

        # Append runtime information to the runtimes
        # Eachtime Shared.runtime is changed the workload must be updated to save the change in the workload file
        if 'runtimes' not in self.obj:
            self.obj.runtimes = list()
        self.obj.runtimes.append(Shared.runtime)
        self.Update()
        Log('Runtime information is updated in the workload.')

        # Check stages exist and set stage.spec.id
        for stageId in self.executionPlan:
            if stageId not in self.stages:
                LogException(f'{stageId} is listed in the `executionPlan` but not defined in the `stages`.')
            else:
                stage = self.stages[stageId]

                self.InferStage(stage, stageId)

                Shared.CurrentStageForLogging = stage
                self.CheckStage(stage)
                Shared.CurrentStageForLogging = None
        self.Update()
        Log('All stages are checked.')

        # For all dataFiles which are used in one of the stages of the executionPlan, create the DataFile object and put it in Shared.dataFiles
        # The dataFile spec in shared.dataFile point to the workload data (self.obj.dataFiles)
        for stageId in self.executionPlan:
            stage = self.stages[stageId]
            if 'io' in stage:
                for fileId in stage.io:
                    if fileId not in self.obj.dataFiles:
                        LogException(f'`fileId` (`{fileId}`) is used in the `stage` (`{stageId}`) IO but does not exist in `dataFiles`')
                    dataFileSpec = self.obj.dataFiles[fileId]
                    if fileId not in Shared.dataFiles:
                        dataFile = DataFile(dataFileSpec)
                        Shared.dataFiles[fileId] = dataFile
        self.Update()
        Log('All dataFiles are processed.')

        Log('Workload has been initialised.')

    @D_General
    def InferStage(self, stage, stageId):
        if 'spec' not in stage:  # stage has not been checked yet
            LogException(f'stage `{stageId}` does not have `spec` key.')
        else:
            stage.spec.id = stageId
            if 'status' not in stage.spec:
                stage.spec.status = 'Initiated'
        self.Update()

    @D_General
    def CheckStage(self, stage):

        # Compelete stage schema template by adding arg and io field for the specific function.
        # try:
        #     functionsSchema = self.functionsSchema.obj
        #     stageSchema = self.stageSchema.obj

        #     for key in ['arg', 'io']:
        #         if key not in functionsSchema[stage.spec.function]:
        #             LogException(f'The FunctionSchema does not include {key} for {stage.spec.function}')
        #         stageSchema['properties'][key] = functionsSchema[stage.spec.function][key]

        #     jsonschema.Draft7Validator.check_schema(stageSchema)
        #     jsonschema.validate(instance=stage, schema=stageSchema)
        # except jsonschema.exceptions.SchemaError:
        #     LogException(f'StageSchema is invalid')
        # except jsonschema.exceptions.ValidationError:
        #     LogException(f'Stage is not validated by the schema')

        # Check each Input/Output (io).

        LogPrint(f'Stage `{stage.spec.id}` is Checked')

    @D_General
    def CheckIO(self, stage):
        if 'io' in stage:
            io = stage.io
            if 'input' in io:
                input = io.input

    @D_General
    def ProcessLiveInput(self, input):
        Log(f'<< io: {input.name} >> is {JsonDumps(input)}.')
        if 'isAlive' in input and input.isAlive:
            if input.pathType != 'fileList':
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
