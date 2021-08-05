import argparse


from .logutil import *
from .common import *
from .decorators import *
from cap.workload import Workload
from cap.executor import Executor


@D_General
def CapMain(args):
    workload = Workload(path=args.workload)
    Shared.workload = workload
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
 
    ##### Note that logger has not been initialised yet.
    ##### Functions that uses the logger should NOT be used.
    if args.capLog:
        args.capLog = os.path.abspath(os.path.expandvars(args.capLog))
    if args.hailLog:
        args.hailLog = os.path.abspath(os.path.expandvars(args.hailLog))

    ##### Initialise Logger
    InitLogger(capLog=args.capLog)
    Log(f'Runtime Information: {Shared.runtime}')
    
    CapMain(args)


if __name__ == '__main__':

    print('============================================')
    print('============================================')
    print('============================================')
    print('CAP is executed as an standalon application.')
    print()

    Main()

    print()
    print('CAP execution is compeleted.')
    print('============================================')
    print('============================================')
    print('============================================')
