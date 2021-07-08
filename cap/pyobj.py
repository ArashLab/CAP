import json
import yaml
from munch import munchify
import jsonschema
import importlib.resources

from .logutil import *
from .common import *
from .helper import *
from .decorators import *

if __name__ == '__main__':
    print('This module is not executable. Please import this module in your program.')
    exit(0)


class PyObj:

    @D_General
    def __init__(self, path, format=None, reset=False, isInternal=False, isSchema=False, schema=None):

        self.path = path
        self.format = format
        self.obj = None
        self.isInternal = isInternal
        self.isSchema = isSchema
        self.schema = schema

        if isSchema and schema:
            Log(f'Schema object cannot have a schema')
        if (isInternal or isSchema):
            self.readOnly = True
            if reset:
                Log(f'Schema and Internal file are not supposed to be reset')
        else:
            self.readOnly = False

        if path.endswith('.json'):
            detectedFormat = 'json'
        elif path.endswith('.yaml'):
            detectedFormat = 'yaml'
        else:
            detectedFormat = None

        if not format:
            if detectedFormat:
                self.format = detectedFormat
                Log(f'Format is set to {detectedFormat}')
            else:
                LogException(f'Could not infer the file format.')
        else:
            if detectedFormat != format:
                LogException('Given format is different from file extension.')

        if not self.readOnly and (reset or not FileExist(self.path)):
            self.Clear()
        else:
            self.Load()

        if isSchema:
            self.CheckSchema()

        if schema:
            self.CheckObject()

        Log('Initialised.')

    @D_General
    def CheckSchema(self):
        try:
            jsonschema.Draft7Validator.check_schema(self.obj)
        except:
            LogException('Invalid schema')
        Log('Schema Validated.')

    @D_General
    def CheckObject(self):
        try:
            jsonschema.validate(instance=self.obj, schema=self.schema)
        except:
            LogException('Invalid Object based on schema')
        Log('Object Validated.')

    @D_General
    def LoadExtenal(self, path):
        if not FileExist(path):
            LogException(f'File does not exist: {path}')

        with open(path) as inFile:
            if self.format == 'json':
                obj = json.load(inFile)
            elif self.format == 'yaml':
                obj = yaml.load(inFile, Loader=yaml.FullLoader)
            self.obj = munchify(obj)

    @D_General
    def LoadInternal(self, name):
        with importlib.resources.path('cap', name) as path:
            self.LoadExtenal(str(path))

    @D_General
    def Load(self):
        if self.isInternal:
            self.LoadInternal(self.path)
        else:
            self.LoadExtenal(self.path)

        if self.schema:
            self.CheckObject()

        Log('Loaded')

    @D_General
    def UpdateExternal(self, path):
        obj = ToDict(self.obj)
        with open(path, 'w') as outfile:
            if self.format == 'json':
                json.dump(obj, outfile, indent=4, sort_keys=True, default=str)
            elif self.format == 'yaml':
                yaml.dump(obj, outfile, indent=4, sort_keys=True)

    @D_General
    def UpdateInternal(self, name):
        with importlib.resources.path('cap', name) as path:
            self.UpdateExternal(path)

    @D_General
    def Update(self):
        if self.readOnly:
            LogException('Is read-only')

        if self.schema:
            self.CheckObject()

        if self.isInternal:
            self.UpdateInternal(self.path)
        else:
            self.UpdateExternal(self.path)
        Log('Updated')

    @D_General
    def Clear(self):
        self.obj = dict()
        self.Update()
        Log('Cleared')
