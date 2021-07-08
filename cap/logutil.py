from .common import *
from .shared import Shared

import sys
import logging
from datetime import datetime
import random
import string

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


def FixMsg(msg):

    function = None
    stage = None

    if 'CurrentFunctionForLogging' in Shared:
        function = Shared.CurrentFunctionForLogging

    if 'CurrentStageForLogging' in Shared:
        stage = Shared.CurrentStageForLogging

    if len(function) > 0:
        fn = function[-1].__name__
    else:
        fn = 'None'

    if stage:
        sid = stage.spec.id
    else:
        sid = 'None'

    msg = f'{sid}\t{fn}\t{msg}'
    return msg


def LogException(msg='An error occurred.'):
    """Log and raise the exception.

    Note:
        - If CurrentStageForLogging is set then stage.spec.id is added to message and log the entire stage data.

    Args:
        msg (str, optional): Error message. Defaults to 'An error occurred.'.
    """

    stage = Shared.CurrentStageForLogging
    if stage:
        stageStr = JsonDumps(stage)
        logger.error(f'{stageStr}')
        msg = FixMsg(f'{msg}')

    logger.exception(msg)
    raise Exception(msg)


def LogOptionalPrint(msg, level='INFO', file=None, doPrint=True):
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

    if file and not doPrint:
        LogException('Set doPrint to True when you pass the file argument.')

    if level not in ['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG', 'NOTSET']:
        LogException(f'{level} is not a valid loging level.')

    if not file and doPrint:
        if level in ['CRITICAL', 'ERROR']:
            file = sys.stderr
        else:
            file = sys.stdout

    msg = FixMsg(msg)

    if doPrint:
        print(msg, file=file)

    logger.log(getattr(logging, level),  msg)


def Log(msg, level='INFO'):
    """Log a message at the desired level.

    Note:
        - If CurrentStageForLogging is set then stage.spec.id is added to message.

    Args:
        msg (str): To be loged and printed.
        level (str, optional): Log level for the message. Defaults to 'INFO'.
    """
    LogOptionalPrint(msg, level, file=None, doPrint=False)


def LogPrint(msg, level='INFO', file=None):
    """Log a message at desired level and print it.

    Note:
        - If CurrentStageForLogging is set then stage.spec.id is added to message
        - If file is not identified, it is chosen between based on the level.
        - If level is CRITICAL or ERROR the stderr is chosen otherwise the message is printed on stdout.

    Args:
        msg (str): To be loged and printed.
        level (str, optional): Log level for the message. Defaults to 'INFO'.
        file (file, optional): Where the message is printed. Defaults to None.
    """

    LogOptionalPrint(msg, level, file, doPrint=True)
