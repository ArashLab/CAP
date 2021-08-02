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
    print('This module is not executable.')
    exit(0)

# TBF add to be deleted flag to io and delete files after sucessfull compeletion of the workload


class Stage:

    @D_General
    def __init__(self, stage):
        if isinstance(stage, Munch):
            self.stageInfo = stage
        else:
            LogException(f'`stage` must be of type Munch but the type is `{type(stage)}`.')

        ##### Loading stage schemas
        # self.stageSchema = PyObj(path='StageSchema.json', isInternal=True, isSchema=True)
        # self.functionsSchema = PyObj(path='FunctionsSchema.json', isInternal=True)
        # Log('Stage schemas are loaded.')

        # self.Infer()
        # self.InferPath()
        # self.isInferred = True
        # self.ProcessPath()

        # self.InferMemory()

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
    def ExecuteInput(self, inFile):
        dataFile = Shared.dataFiles[inFile.id]

        if 'externalUse' in inFile and inFile.externalUse:
            Log('This file is for external use and not loaded')
            return

        if dataFile.file.storageType == 'memory':
            if not dataFile.file.isReady:
                LogException('Input data is not ready yet')
        elif dataFile.file.storageType == 'disk':
            if not dataFile.file.isLoaded:
                dataFile.ExpandWildcardPath()
                if not dataFile.ExistAll():
                    LogException('Input file does not exist')
                dataFile.Load()

    @D_General
    def ExecuteOutput(self, outFile):
        dataFile = Shared.dataFiles[outFile.id]

        if 'externalUse' in outFile and outFile.externalUse:
            Log('This file is for external use and not dumped')
            return

        if not dataFile.file.isLoaded:
            LogException('Output data is not ready yet')

        if dataFile.file.storageType == 'memory':
            Log('No need to write on disk')
        elif dataFile.file.storageType == 'disk':
            if dataFile.file.disk.isWildcard:
                LogException('Output file cannot be wildcard')
            if dataFile.ExistAny():
                LogException(f'Output path already exist {outFile.id}')
            
            dataFile.Dump()

    @D_General
    def ExecuteInputs(self, stage):
        if 'io' in stage:
            for io in stage.io.values():
                if io.direction == 'input':
                    self.ExecuteInput(io)

    @D_General
    def ExecuteOutputs(self, stage):
        if 'io' in stage:
            for io in stage.io.values():
                if io.direction == 'output':
                    self.ExecuteOutput(io)
