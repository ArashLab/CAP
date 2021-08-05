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

class InOut(Munch):

    @D_General
    def __init__(self, inout):
        self.update(inout)

    @D_General
    def GetData(self):
        data = self.dataFile.GetData()
        # operations = self.get('commonOperations')
        # if operations:
        #     data = CommonOperations(data, operations)
        return data

    @D_General
    def SetData(self, data):
        # operations = self.get('commonOperations')
        # if operations:
        #     data = CommonOperations(data, operations)
        self.dataFile.SetData(data)

    data = property(GetData, SetData)

   