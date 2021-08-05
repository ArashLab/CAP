from .workload import Workload

from .helper import *
from .common import *
from .logutil import *
from .shared import Shared


import hail as hl
from munch import Munch
from datetime import datetime

from copy import deepcopy

if __name__ == '__main__':
    print('This module is not executable.')
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
            Shared.runtime.sparkConfig = dict(sc.getConf().getAll())
            workload.Update()
     

            LogPrint("+++++++++++++++++++++++++++++++")
            LogPrint("+++++++++++++++++++++++++++++++")
            LogPrint("+++++++++++++++++++++++++++++++")
            LogPrint(f'Runtime Information: {Shared.runtime}')
            LogPrint("+++++++++++++++++++++++++++++++")
            LogPrint("+++++++++++++++++++++++++++++++")
            LogPrint("+++++++++++++++++++++++++++++++")
        except:
            LogException('Something wrong')

        self.initialised = True

    @D_General
    def Execute(self):
        workload = self.workload
        for stageId in workload.executionPlan:
            stage = workload.stages[stageId]
            stage.Execute(self.workload)
    
