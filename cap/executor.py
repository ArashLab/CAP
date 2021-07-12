from .workload import Workload
from . import operation as Operation
from .helper import *
from .common import *
from .logutil import *
from .shared import Shared


import hail as hl
from munch import Munch
from datetime import datetime

from copy import deepcopy

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
                Shared.runtime.hailLog = hailLog
            else:
                randomStr = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(8))
                now = str(datetime.now().strftime("%Y%m%d-%H%M%S"))
                Shared.runtime.hailLog = f'hail.{now}.{randomStr}.log'

            appName = f'CAP - {Shared.runtime.hailLog}'
     
            hl.init(log=Shared.runtime.hailLog, app_name=appName)

            Shared.runtime.hailVersion = hl.version()
            sc = hl.spark_context()
            Shared.runtime.sparkVersion = sc.version
            workload.Update()
     
            Log(f'CAP Version: {Shared.runtime.capVersion}')
            Log(f'Hail Version: {Shared.runtime.hailVersion}')
            Log(f'Spark Version {Shared.runtime.sparkVersion}')
            Log(f'CAP Log {Shared.runtime.capLog}')
            Log(f'Hail Log {Shared.runtime.hailLog}')

            

            LogPrint("+++++++++++++++++++++++++++++++")
            LogPrint("+++++++++++++++++++++++++++++++")
            LogPrint("+++++++++++++++++++++++++++++++")
            LogPrint(hl.spark_context().getConf().getAll())
            LogPrint("+++++++++++++++++++++++++++++++")
            LogPrint("+++++++++++++++++++++++++++++++")
            LogPrint("+++++++++++++++++++++++++++++++")
        except:
            LogException('Something wrong')

        self.initialised = True

    @D_General
    def Execute(self, reset=False):
        workload = self.workload
        if workload.order:
            for stageId in workload.order:
                stage = workload.stages[stageId]
                if stage.spec.status != 'Completed' or reset:
                    self.ExecuteStage(stage)

    @D_General
    def ExecuteStage(self, stage):
        workload = self.workload
        Shared.CurrentStageForLogging = stage
        workload.CheckStage(stage)  # Check the stage right before execution to make sure no dynamic error occurs
        LogPrint(f'Started')
        func = getattr(Operation, stage.spec.function)
        runtime = Munch()
        runtime.base = Shared.runtime.base
        if 'runtimes' not in stage.spec:
            stage.spec.runtimes = list()
        stage.spec.runtimes.append(runtime)
        runtime.startTime = datetime.now()
        workload.Update()
        workload.ProcessLiveInputs(stage)
        workload.Update()
        func(stage)
        workload.Update()
        workload.ProcessLiveOutputs(stage)
        workload.Update()
        runtime.endTime = datetime.now()
        runtime.execTime = str(runtime.endTime - runtime.startTime)
        runtime.status = 'Completed'
        stage.spec.status = 'Completed'
        workload.Update()
        LogPrint(f'Completed in {runtime.execTime}')
        Shared.CurrentStageForLogging = None
