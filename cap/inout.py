from munch import Munch
from .logutil import *
from .common import *
from .helper import *
from .decorators import *
from .microfunc import *

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
        microFunctions = self.get('microFunctions')
        if microFunctions:
            data = MicroFunction(data, microFunctions)
        return data

    @D_General
    def SetData(self, data):
        microFunctions = self.get('microFunctions')
        if microFunctions:
            data = MicroFunction(data, microFunctions)
        self.dataFile.SetData(data)

    data = property(GetData, SetData)

   