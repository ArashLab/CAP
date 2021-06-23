import argparse

from .logutil import *
from .common import *
from .decorators import *
from cap.workload import Workload
from cap.executor import Executor


@D_General
def CapMain(args):
    workload = Workload(path=args.workload)
    executor = Executor(workload)
    executor.Execute()


@D_General
def Main():
    parser = argparse.ArgumentParser(
        description='Execute CAP workload'
    )
    parser.add_argument('-w', '--workload', required=True, type=str, help='The workload file (yaml or json).')
    args = parser.parse_args()

    CapMain(args)


if __name__ == '__main__':
    Log('Collect logs for Main module.')
    Main()
