from .common import *
from .shared import Shared

import sys
import logging
from datetime import datetime
import random
import string
from inspect import getframeinfo, stack

logger = logging.getLogger(__name__)

def InitLogger(capLog=None):

    global logger
    if capLog:
        fileName = capLog
    else:
        randomStr = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(8))
        now = str(datetime.now().strftime("%Y%m%d-%H%M%S"))
        fileName = f'cap.{now}.{randomStr}.log.tsv'
    
    Shared.runtime.capLog = fileName

    # Gets or creates a logger
    # logger = logging.getLogger(__name__)
    # set log level
    logger.setLevel(logging.DEBUG)
    # define file handler and set formatter
    file_handler = logging.FileHandler(filename=Shared.runtime.capLog,  mode='w')
    formatter = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
    file_handler.setFormatter(formatter)
    # add file handler to logger
    logger.addHandler(file_handler)
    print(f'*** logger is initialised to write to {Shared.runtime.capLog}')

# IMPORTANT You may not include other cap modules in this module

if __name__ == '__main__':
    print('This module is not executable. Please import this module in your program.')
    exit(0)


def FixMsg(msg, caller=None):


    stage = None

    if 'CurrentStageForLogging' in Shared:
        stage = Shared.CurrentStageForLogging

    if stage:
        sid = stage.spec.id
    else:
        sid = 'None'

    if caller:
        funcName = caller.function
        fileName = caller.filename
        lineNum = caller.lineno
    else:
        funcName = 'None'
        fileName = 'None'
        lineNum = 'None'

    msg = f'{sid}\t{funcName}\t{msg}\t{fileName}\t{lineNum}'
    return msg


def LogException(msg='An error occurred.', caller=None):
    """Log and raise the exception.

    Note:
        - If CurrentStageForLogging is set then stage.spec.id is added to message and log the entire stage data.

    Args:
        msg (str, optional): Error message. Defaults to 'An error occurred.'.
    """

    if not caller:
        caller = getframeinfo(stack()[1][0])

    stage = Shared.CurrentStageForLogging
    if stage:
        stageStr = JsonDumps(stage)
        logger.error(f'{stageStr}')
        msg = FixMsg(msg=f'{msg}', caller=caller)

    logger.exception(msg)
    raise Exception(msg)


def LogOptionalPrint(msg, level='INFO', file=None, doPrint=True, caller=None):
    """Log a message at desired level and print it if needed.

    Note:
        - If CurrentStageForLogging is set then stage.spec.id is added to message.
        - If file is not identified, it is chosen between based on the level.
        - If level is CRITICAL or ERROR the stderr is chosen otherwise the message is printed on stdout.

    Args:
        msg (str): To be loged and printed.
        level (str, optional): Log level for the message. Defaults to 'INFO'.
        file (file, optional): Where the message is printed. Defaults to None.
        doPrint (bool, optional): If the message should be printed too. Defaults to True.
    """

    if not caller:
        caller = getframeinfo(stack()[1][0])

    if file and not doPrint:
        LogException('Set doPrint to True when you pass the file argument.')

    if level not in ['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG', 'NOTSET']:
        LogException(f'{level} is not a valid loging level.')

    if not file and doPrint:
        if level in ['CRITICAL', 'ERROR']:
            file = sys.stderr
        else:
            file = sys.stdout

    msg = FixMsg(msg=f'{msg}', caller=caller)

    if doPrint:
        print(msg, file=file)

    logger.log(getattr(logging, level),  msg)


def Log(msg, level='INFO', caller=None):
    if not caller:
        caller = getframeinfo(stack()[1][0])
    LogOptionalPrint(msg, level, file=None, doPrint=False, caller=caller)


def LogPrint(msg, level='INFO', file=None, caller=None):
    if not caller:
        caller = getframeinfo(stack()[1][0])
    LogOptionalPrint(msg, level, file, doPrint=True, caller=caller)
