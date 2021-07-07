from .workload import Workload
from . import operation as Operation
from .helper import *
from .common import *
from .logutil import *
from .shared import Shared

import importlib.resources
from pathlib import Path


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
                Shared.hailLog = hailLog
            else:
                randomStr = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(8))
                now = str(datetime.now().strftime("%Y%m%d-%H%M%S"))
                Shared.hailLog = f'hail.{now}.{randomStr}.log'

            hl.init(log=Shared.hailLog)

            with importlib.resources.path('cap', 'VERSION') as path:
                Shared.capVersion = Path(path).read_text()
            Shared.hailVersion = hl.version()
            sc = hl.spark_context()
            Shared.sparkVersion = sc.version

            Log(f'CAP Version: {Shared.capVersion}')
            Log(f'Hail Version: {Shared.hailVersion}')
            Log(f'Spark Version {Shared.sparkVersion}')
            Log(f'CAP Log {Shared.capLog}')
            Log(f'Hail Log {Shared.hailLog}')


            workload.globConfig.capVersion = Shared.capVersion
            workload.globConfig.hailVersion = Shared.hailVersion
            workload.globConfig.sparkVersion = Shared.sparkVersion
            workload.globConfig.capLog = Shared.capLog
            workload.globConfig.hailLog = Shared.hailLog
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
        stage.spec.capLog = Shared.capLog
        stage.spec.hailLog = Shared.hailLog
        stage.spec.startTime = datetime.now()
        workload.ProcessLiveInputs(stage)
        func(stage)
        workload.ProcessLiveOutputs(stage)
        stage.spec.endTime = datetime.now()
        stage.spec.execTime = str(stage.spec.endTime - stage.spec.startTime)
        stage.spec.status = 'Completed'
        LogPrint(f'Completed in {stage.spec.execTime}')
        Shared.CurrentStageForLogging = None
