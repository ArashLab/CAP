import argparse
from os import path
from posixpath import abspath

from yaml import compose_all

from .logutil import *
from .common import *
from .decorators import *
from cap.workload import Workload
from cap.executor import Executor


@D_General
def CapMain(args):
    workload = Workload(path=args.workload)
    executor = Executor(workload, hailLog=args.hailLog)
    executor.Execute()


def Main():
    parser = argparse.ArgumentParser(
        description='Execute CAP workload'
    )
    parser.add_argument('-w', '--workload', required=True, type=str, help='The workload file (yaml or json).')
    parser.add_argument('-l', '--capLog', type=str, help='CAP log file')
    parser.add_argument('-hl', '--hailLog', type=str, help='Hail log file')
    args = parser.parse_args()
 
    if args.capLog:
        args.capLog = os.path.abspath(args.capLog)

    if args.hailLog:
        args.hailLog = os.path.abspath(args.hailLog)

    InitLogger(capLog=args.capLog)

    CapMain(args)


if __name__ == '__main__':
    Log('Collect logs for Main module.')
    Main()
