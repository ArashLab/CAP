import os
import subprocess

from .logutil import *
from .decorators import *


if __name__ == '__main__':
    print('This module is not executable.')
    exit(0)

FileSystems = [
    'file',
    'hdfs',
    's3',
    'gs',
    'mysql',
    'http',
    'https'
]

class Path:

    ##### If true, remove file system prefix (i.e. 'file://' or 'hdfs://') of the defaultFileSystem.
    ##### For example, if 'defaultFileSystem=local' it removes the 'file://' from the path 
    __defaultMode = True

    @classmethod
    def SetDefaultMode(cls, defaultMode):
        cls.__defaultMode = defaultMode

    @classmethod
    def GetDefaultMode(cls):
        return cls.__defaultMode

    ##### If the path does not have a file system prefix (i.e. 'file://' or 'hdfs://')  adds the prefix based on the default file system
    __defaultFileSystem = 'file'

    @classmethod
    def SetDefaultFileSystem(cls, defaultFileSystem):
        if defaultFileSystem in FileSystems:
            cls.__defaultFileSystem = defaultFileSystem
        else:
            LogException(f'File system `{defaultFileSystem}` is not supported')

    @classmethod
    def GetDefaultFileSystem(cls):
        return cls.__defaultFileSystem

    def __init__(self, path=None):
        self.__path = None
        self.__raw = None
        if path:
            self.path = path

    @property
    def path(self):
        if self.GetDefaultMode():
            if self.__fileSystem == self.GetDefaultFileSystem():
                return self.__path
        return '://'.join([self.__fileSystem, self.__path])

    @property
    def local(self):
        return self.__path

    @property
    def fileSystem(self):
        return self.__fileSystem

    @property
    def raw(self):
        return self.__raw

    @path.setter
    def path(self, path):
        if isinstance (path, str):
            self.__raw = str(path)
            self.Processes()
        else:
            LogExceptionType(path, expectedType='str')

    def Processes(self):
        ##### Identify the file system and extract it from the path
        rawPath = os.path.expandvars(self.__raw)
        if ':' in rawPath:
            parts = rawPath.split(':')
            if len(parts) > 2:
                LogException(f'Path `{rawPath}` has more than one `:`')
            elif not parts[0]:
                LogException(f'Path `{rawPath}` starts with `:`')
            elif parts[0] not in FileSystems:
                LogException(f'File system `{parts[0]}` in path `{rawPath}` not supported.')
            else:
                self.__fileSystem = parts[0]
                path = self.Trim(parts[1])
        else:
            self.__fileSystem = self.GetDefaultFileSystem()
            path = rawPath

        self.__path = self.Trim(path)
        self.Absolute()

    @classmethod
    def Trim(cls, path, char='/'):
        while True:
            if path.startswith(char*2):
                path = path[1:]
            else:
                break
        return path
    
    def Absolute(self):
        fs = self.fileSystem
        if fs not in ['file']:
            LogException(f'File system `{fs}` is not supported')
        elif fs == 'file':
            self.__path = os.path.abspath(self.__path)

    def Exist(self):
        fs = self.fileSystem
        if fs not in ['file', 'hdfs']:
            LogException(f'File system `{fs}` is not supported')
        elif fs == 'file':
            return os.path.exists(self.local)
        elif fs == 'local':
            return not subprocess.run(['hdfs', 'dfs', '-test', '-e', self.path]).returncode
