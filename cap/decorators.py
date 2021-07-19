from .common import *
from .logutil import *
from .shared import Shared

from datetime import datetime
from inspect import getframeinfo, stack

if __name__ == '__main__':
    print('This module is not executable. Please import this module in your program.')
    exit(0)


def D_General(func):
    def W_General(*args, **kwargs):
        caller = getframeinfo(stack()[1][0])
        Log(f'*D* Starts with args {JsonDumps(locals())}.', level='DEBUG', caller=caller)
        start = datetime.now()
        try:
            ret = func(*args, **kwargs)
        except:
            LogException('*D* Unknown exception occurs.', caller=caller)
        end = datetime.now()
        Log(f'*D* Ends in {end - start}', level='DEBUG', caller=caller)
        return ret
    return W_General
