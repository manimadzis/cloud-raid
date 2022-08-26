class Error(Exception):
    def __init__(self, *args, **kwargs):
        pass


class NoDisks(Error):
    pass

class FileAlreadyExists(Error):
    pass