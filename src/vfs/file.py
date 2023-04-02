from dataclasses import dataclass
from network import Uploader, Downloader

class File:
    def __init__(self, 
    uploader: Uploader,
        downloader: Downloader):
        self._uploader = uploader
        self._downloader = downloader



    def progress():