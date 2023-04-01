class ErrorBase(Exception):
    pass


class DownloaderErrorBase(ErrorBase):
    pass


class ChecksumNoEqual(DownloaderError):
    pass


class BlockDownloadFailed(DownloaderError):
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

class NoCipher(Error):
    pass

class UploadFailed(Error):
    pass