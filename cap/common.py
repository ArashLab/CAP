import os
import json

# IMPORTANT You may not include other cap modules in this module

if __name__ == '__main__':
    print('This module is not executable.')
    exit(0)

def JsonDumps(obj):
    return json.dumps(obj, default=str)


def ToDict(obj):
    return json.loads(JsonDumps(obj))

def IsListOfStr(data):
    return isinstance(data, list) and all([isinstance(value, str) for value in list(data)])

def ExpandVars(path):
    return os.path.expandvars(path)