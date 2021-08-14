from glob import glob

from .logutil import *
from .decorators import *
from .path import Path


if __name__ == '__main__':
    print('This module is not executable.')
    exit(0)

class PathList:

    def __init__(self, paths=list()):
        self.__paths = list()
        self.__opaths = list()
        self.paths = paths

    def __repr__(self):
        maxShow = len(self.__paths) if len(self.__paths) < 10 else 10
        return str(self.__paths[:maxShow])
        
    @property
    def path(self):
        numPath = len(self.__paths) 
        if numPath == 1:
            return self.__paths[0]
        elif numPath > 1:
            LogException('More than one path (`{numPath}` paths) is in the list')
        else:
            LogException('No path exist')

    @property
    def solos(self): # string or list of string
        numPath = len(self.__paths) 
        if numPath == 1:
            return self.__paths[0].path
        elif numPath > 1:
            return [path.path for path in self.__paths]
        else:
            LogException('No path exist')

    @property
    def paths(self):
        return self.__paths

    @paths.setter
    def paths(self, data):
        if IsListOfStr(data):
            paths = [str(value) for value in list(data)]
        elif isinstance (data, str):
            paths = [str(data)]
        else:
            LogExceptionType(data, expectedType='str or list of str')

        self.__paths = list()
        for path in paths:
            self.__paths.append(Path(path))

    def Exist(self):
        return self.ExistAll()

    def ExistAll(self):
        return all([path.Exist() for path in self.__paths])

    def ExistAny(self):
        return any([path.Exist() for path in self.__paths])

    def ExistNone(self):
        return all([not path.Exist() for path in self.__paths])

    def IsWildcardPath(self, path):
        for ch in '*?[]':
            if ch in path:
                return True
        return False

    def ExpandWildCard(self):
        if self.__opaths:
            return # already expanded

        self.__opaths = self.__paths
        self.__paths = list()
        for path in self.__opaths:
            if self.IsWildcardPath(path.path):
                gpaths = glob(path.path)
                for gpath in gpaths:
                    self.__paths.append(Path(gpath))
            else:
                self.__paths.append(path)
