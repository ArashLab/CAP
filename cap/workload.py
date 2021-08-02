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
from .stage import Stage

if __name__ == '__main__':
    print('This module is not executable.')
    exit(0)

class Workload(PyObj):

    @D_General
    def __init__(self, path, format=None):

        ##### Load the workload into the wl
        super().__init__(path, format)
        wl = self.obj

        ##### Add all necessary Keys if not exist
        wl.runtimes = wl.get('runtimes', list())
        wl.defaults = wl.get('defaults', Munch())
        wl.environmentVariables = wl.get('environmentVariables', Munch())
        wl.dataFiles = wl.get('dataFiles', Munch())
        wl.stages = wl.get('stages', Munch())
        wl.executionPlan = wl.get('executionPlan', list())

        ##### Check against workload schema
        # self.workloadSchema = PyObj(path='WorkloadSchema.json', isInternal=True, isSchema=True)
        # self.schema = self.workloadSchema.obj
        # self.CheckObject()
        Log('Workload is checked against schema.')

        ##### Append runtime information to the runtimes
        ##### Changes to Shared.runtime does not automatically update the workload.
        wl.runtimes.append(Shared.runtime)
        Log('Runtime information is updated in the workload.')
        self.Update()

        ##### Handeling default values
        ##### Default values are read only and are accessed through Shared module
        ##### If defaults exist in both workload and shared, default values in the workload file must be used.
        Shared.defaults.update(wl.defaults)
        wl.defaults.update(Shared.defaults)
        CheckDefaults()
        Log(f'Default values are as follow `{Shared.defaults}`')
        self.Update()
        
        ##### Set the environment variables
        for variable, value in wl.environmentVariables.items():
            os.environ[variable] = str(value)
            Shared.runtime.environment[variable] = os.environ[variable]
        Log('Environmental Variables are set.')
        self.Update()

        ##### Create DataFile objects
        self.dataFiles = Munch()
        for ID, dataFile in wl.dataFiles.items():
            self.dataFiles[ID] = DataFile(dataFile)

        ##### Create Stage objects
        self.stages = Munch()
        for ID, stage in wl.stages.items():
            self.stages[ID] = Stage(stage)

        ##### Add execution plan to the runtime
        Shared.runtime.executionPlan = wl.executionPlan
        self.Update()

        Log('Workload has been initialised.')