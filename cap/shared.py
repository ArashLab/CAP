from munch import Munch
from datetime import datetime
import importlib.resources
from pathlib import Path
import random
import string
import os
import platform

Shared = Munch()

#################################################
##### Runtime information
#################################################

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

for envVar in ['CAP_HOME', 'PYSPARK_PYTHON', 'HAIL_HOME', 'SPARK_HOME', 'HDFS_HOME']:
    if envVar in os.environ:
        Shared.runtime.environment[envVar] = os.environ[envVar]
    else:
        Shared.runtime.environment[envVar] = '__NOT_SET__'

#################################################
##### Current executong stage
#################################################

Shared.CurrentStageForLogging = None

#################################################
##### input & output (io) of the stages
#################################################

Shared.dataFiles = Munch()

#################################################
##### Default Values
#################################################

Shared.defaults = Munch()

Shared.defaults.fileSystem = 'local'
Shared.defaults.vepCheckWaitTime = 5
Shared.defaults.localMode = True
Shared.defaults.persistence = None

Shared.defaults.numPartitions = Munch()
Shared.defaults.numPartitions.default = 4
Shared.defaults.numPartitions.min = 1
Shared.defaults.numPartitions.max = 64 
 
Shared.defaults.numSgeJobs = Munch()
Shared.defaults.numSgeJobs.default = 4
Shared.defaults.numSgeJobs.min = 1
Shared.defaults.numSgeJobs.max = 32