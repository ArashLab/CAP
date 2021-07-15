from .common import *
from .logutil import *
from .shared import Shared

from datetime import datetime

if __name__ == '__main__':
    print('This module is not executable. Please import this module in your program.')
    exit(0)


def D_General(func):
    def W_General(*args, **kwargs):
        Log(f'Starts with args {JsonDumps(locals())}.', level='DEBUG')
        start = datetime.now()
        try:
            ret = func(*args, **kwargs)
        except:
            LogException('Unknown exception occurs.')
        end = datetime.now()
        Log(f'Ends in {end - start}', level='DEBUG')
        return ret
    return W_General
