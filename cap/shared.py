from munch import Munch
from datetime import datetime
import importlib.resources
from pathlib import Path
import random
import string
import os
import platform

Shared = Munch()

Shared.data = Munch()

Shared.runtime = Munch()
Shared.runtime.base = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(8))
with importlib.resources.path('cap', 'VERSION') as path:
    Shared.runtime.capVersion = Path(path).read_text()
Shared.runtime.capLog = 'ToBeSet'
Shared.runtime.hailLog = 'ToBeSet'
Shared.runtime.hailVersion = 'ToBeSet'
Shared.runtime.sparkVersion = 'ToBeSet'
Shared.runtime.sparkConfig = 'ToBeSet'
Shared.runtime.dateTime = str(datetime.now().strftime("%Y/%m/%d-%H:%M:%S"))
Shared.runtime.host = platform.node()
Shared.runtime.environment = Munch()

for envVar in ['CAP_DIR', 'PYSPARK_PYTHON', 'PYSPARK_PYTHON', 'HAIL_HOME', 'SPARK_HOME', 'HDFS_HOME']:
    if envVar in os.environ:
        Shared.runtime.environment[envVar] = os.environ[envVar]
    else:
        Shared.runtime.environment[envVar] = '__NOT_SET__'

Shared.fileSystem = None

Shared.vepCheckWaitTime = 5

Shared.CurrentStageForLogging = None
Shared.CurrentFunctionForLogging = list()

Shared.numPartitions = Munch()
Shared.numPartitions.default = 4
Shared.numPartitions.min = 1
Shared.numPartitions.max = 64  

Shared.numSgeJobs = Munch()
Shared.numSgeJobs.default = 4
Shared.numSgeJobs.min = 1
Shared.numSgeJobs.max = 32