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
from . import operation as Operation

if __name__ == '__main__':
    print('This module is not executable.')
    exit(0)

# TBF add to be deleted flag to io and delete files after sucessfull compeletion of the workload


class Stage(Munch):

    @D_General
    def __init__(self, stage, stageId):
        stage.specifications.id = stageId
        stage.specifications.status = 'Initiated'
        stage.inouts = stage.get('inouts', Munch())
        self.update(stage)


    @D_General
    def Execute(self, dataFiles):
        workload = Shared.workload
        
        Shared.CurrentStageForLogging = self
        
        #self.CheckStage(stage)  # Check the stage right before execution to make sure no dynamic error occurs
        func = getattr(Operation, self.specifications.function)
        
        LogPrint(f'Started')
        
        self.specifications.status = 'Running'
        runtime = Munch()
        runtime.base = Shared.runtime.base
        self.specifications.runtimes = self.specifications.get('runtimes', list())
        self.specifications.runtimes.append(runtime)
        runtime.startTime = datetime.now()
        workload.Update()

        self.ExecuteInputs()
        workload.Update()

        func(self, dataFiles)
        workload.Update()

        self.ExecuteOutputs()
        workload.Update()

        runtime.endTime = datetime.now()
        runtime.execTime = str(runtime.endTime - runtime.startTime)
        runtime.status = 'Completed'
        self.specifications.status = 'Completed'
        workload.Update()

        LogPrint(f'Completed in {runtime.execTime}')
        Shared.CurrentStageForLogging = None


    @D_General
    def ExecuteInputs(self):
        for inout in self.inouts.values():
            if inout.direction == 'input':
                for items in inout.dataFiles:
                    pass

    @D_General
    def ExecuteOutputs(self):
        for inout in self.inouts.values():
            if inout.direction == 'output':
                for dataFileId in inout.id:
                    Shared.workload.dataFiles[dataFileId].ExecuteAsOutput()
