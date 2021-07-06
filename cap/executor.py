from .workload import Workload
from . import operation as Operation
from .helper import *
from .common import *
from .logutil import *
from .shared import Shared


import hail as hl
from munch import Munch
from datetime import datetime

if __name__ == '__main__':
    print('This module is not executable. Please import this module in your program.')
    exit(0)

class Executor:

    @D_General
    def __init__(self, workload, hailLog=None):

        if not isinstance(workload, Workload):
            LogException('workload must be of type Workload')
        self.workload = workload

        try:
            if hailLog:
                hl.init(log=hailLog)
                Shared.hailLog = hailLog
                Log(f'Hail Log is written to {Shared.hailLog}')
            else:
                hl.init()
                Shared.hailLog = "NotAvailable (To Be Fixed)" #TBF

        

            workload.globConfig.hailLog = Shared.hailLog
            workload.globConfig.capLog = Shared.capLog
            workload.Update()

            LogPrint("+++++++++++++++++++++++++++++++")
            LogPrint("+++++++++++++++++++++++++++++++")
            LogPrint("+++++++++++++++++++++++++++++++")
            LogPrint(hl.spark_context().getConf().getAll())
            LogPrint("+++++++++++++++++++++++++++++++")
            LogPrint("+++++++++++++++++++++++++++++++")
            LogPrint("+++++++++++++++++++++++++++++++")
        except:
            pass  # hail throw exception when you call init() more than once but it is ok
            # TBF: what about other exceptions

        self.initialised = True

    @D_General
    def Execute(self, reset=False):
        workload = self.workload
        if workload.order:
            for stageId in workload.order:
                stage = workload.stages[stageId]
                if stage.spec.status != 'Completed' or reset:
                    self.ExecuteStage(stage)
                    workload.Update()

    @D_General
    def ExecuteStage(self, stage):
        workload = self.workload
        Shared.CurrentStageForLogging = stage
        workload.CheckStage(stage)  # Check the stage right before execution to make sure no dynamic error occurs
        LogPrint(f'Started')
        func = getattr(Operation, stage.spec.function)
        stage.spec.logFile = Shared.logFile
        stage.spec.startTime = datetime.now()
        workload.ProcessLiveInputs(stage)
        func(stage)
        workload.ProcessLiveOutputs(stage)
        stage.spec.endTime = datetime.now()
        stage.spec.execTime = str(stage.spec.endTime - stage.spec.startTime)
        stage.spec.status = 'Completed'
        LogPrint(f'Completed in {stage.spec.execTime}')
        Shared.CurrentStageForLogging = None
