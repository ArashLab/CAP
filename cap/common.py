import os
import json

# IMPORTANT You may not include other cap modules in this module

if __name__ == '__main__':
    print('This module is not executable. Please import this module in your program.')
    exit(0)

def JsonDumps(obj):
    return json.dumps(obj, default=str)


def ToDict(obj):
    return json.loads(JsonDumps(obj))

