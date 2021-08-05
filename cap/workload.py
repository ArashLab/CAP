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
from .inout import InOut


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

        self.dataFiles = wl.dataFiles
        self.stages = wl.stages
        self.executionPlan = wl.executionPlan

        ##### Append runtime information to the runtimes
        ##### Changes to Shared.runtime does not automatically update the workload.
        wl.runtimes.append(Shared.runtime)
        Log('Runtime information is updated in the workload.')

        ##### Handeling default values
        ##### Default values are read only and are accessed through Shared module
        ##### If defaults exist in both workload and shared, default values in the workload file must be used.
        Shared.defaults.update(wl.defaults)
        wl.defaults.update(Shared.defaults)
        CheckDefaults()
        Log(f'Default values are as follow `{Shared.defaults}`')
        
        ##### Set the environment variables
        for variable, value in wl.environmentVariables.items():
            os.environ[variable] = str(value)
            Shared.runtime.environment[variable] = os.environ[variable]
        Log('Environmental Variables are set.')

        ##### Create DataFile objects
        self.dataFiles = Munch()
        for ID, dataFile in wl.dataFiles.items():
            dataFile.id = ID
            self.dataFiles[ID] = DataFile(dataFile)
            dataFile = self.dataFiles[ID]

        ##### Create Stage objects
        self.stages = Munch()
        for ID, stage in wl.stages.items():
            stage.id = ID
            self.stages[ID] = Stage(stage)
            stage = self.stages[ID]

        for stage in self.stages.values():
            for ID, inout in stage.inouts.items():
                inout.id = ID
                stage.inouts[ID] = InOut(inout)

        ##### Add execution plan to the runtime
        Shared.runtime.executionPlan = wl.executionPlan

        self.Update()
        Log('Workload has been initialised.')


    @D_General
    def GetDataFileById(self, ID):
        wl = self.obj

        if ID not in self.dataFiles: # create a memory datafile on the fly
            dataFile = Munch()
            dataFile.id = ID
            self.dataFiles[ID] = DataFile(dataFile)
            wl.dataFiles[ID] = self.dataFiles[ID]
            self.Update()

        return self.dataFiles[ID]
