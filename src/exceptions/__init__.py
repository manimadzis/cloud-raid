class Error(Exception):
    def __init__(self, *args, **kwargs):
        pass


class NoStorage(Error):
    pass

class FileAlreadyExists(Error):
    pass

class CancelAction(Error):
    pass

class UnknownFIle(Error):
    pass