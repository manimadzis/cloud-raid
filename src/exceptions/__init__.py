class Error(Exception):
    def __init__(self, *args, **kwargs):
        pass


class NoStorage(Error):
    pass

class FileAlreadyExists(Error):
    pass

class CancelAction(Error):
    pass

class UnknownFile(Error):
    pass

class KeyAlreadyExists(Error):
    pass

class UnknownStorage(Error):
    pass

