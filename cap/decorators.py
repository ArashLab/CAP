from .common import *
from .logutil import *
from .shared import Shared

import time

if __name__ == '__main__':
    print('This module is not executable. Please import this module in your program.')
    exit(0)


def D_General(func):
    def W_General(*args, **kwargs):
        Shared.CurrentFunctionForLogging.append(func)
        Log(f'Starts with args {JsonDumps(locals())}.', level='DEBUG')
        start = time.time()
        try:
            ret = func(*args, **kwargs)
        except:
            LogException('Unknown exception occurs.')
        end = time.time()
        Log(f'Ends in {end - start} seconds.', level='DEBUG')
        Shared.CurrentFunctionForLogging.pop()
        return ret
    return W_General
