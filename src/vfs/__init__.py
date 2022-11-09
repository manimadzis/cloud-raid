import io
import os
import sys
import typing
from collections import OrderedDict
from os.path import join, abspath, normpath
from typing import List, Text, Optional, TextIO

from fs import ResourceType
from fs.base import FS
from fs.info import Info
from fs.memoryfs import MemoryFS, _DirEntry

from entities import File
from repository.block_repo import BlockRepo


class DirEntry(_DirEntry):
    def __init__(self, resource_type, name):
        super(DirEntry, self).__init__(resource_type, name)
        self._size = 0
        self._dir = OrderedDict()  # type: typing.MutableMapping[Text, DirEntry]

    @property
    def size(self):
        return self._size

    @size.setter
    def size(self, _size):
        self._size = _size


class VFS(MemoryFS):
    def __init__(self, block_repo: BlockRepo):
        super(VFS, self).__init__()
        self._block_repo = block_repo
        self._files: List[File] = None

    def _make_dir_entry(self, resource_type, name):  # type: (ResourceType, Text) -> _DirEntry
        return DirEntry(resource_type, name)

    async def load(self):
        self._files = await self._block_repo.get_files()
        for file in self._files:
            dirs, _ = os.path.split(file.filename)
            if dirs:
                self.makedirs(dirs)
            self.touch(file.filename)
            self.setsize(file.filename, file.size)

    def tree(self, with_color=False) -> str:
        stream = io.StringIO()
        render(self, file=stream, with_color=with_color)
        return ''.join(stream.getvalue())

    def setsize(self, path: str, size: int):
        _path = self.validatepath(path)
        with self._lock:
            dir_path, file_name = os.path.split(_path)
            parent_dir_entry = self._get_dir_entry(dir_path)

            resource_entry = typing.cast(
                    DirEntry, parent_dir_entry.get_entry(file_name)
            )
            resource_entry.size = size


def render(
        fs,  # type: FS
        path="/",  # type: Text
        file=None,  # type: Optional[TextIO]
        encoding=None,  # type: Optional[Text]
        max_levels=5,  # type: int
        with_color=None,  # type: Optional[bool]
        dirs_first=True,  # type: bool
):
    file = file or sys.stdout
    if encoding is None:
        encoding = getattr(file, "encoding", "utf-8") or "utf-8"
    is_tty = hasattr(file, "isatty") and file.isatty()

    if with_color is None:
        is_windows = sys.platform.startswith("win")
        with_color = False if is_windows else is_tty

    if encoding.lower() == "utf-8" and with_color:
        char_vertline = "│"
        char_newnode = "├"
        char_line = "──"
        char_corner = "└"
    else:
        char_vertline = "|"
        char_newnode = "|"
        char_line = "--"
        char_corner = "`"

    indent = " " * 4
    line_indent = char_vertline + " " * 3

    def write(line):
        print(line, file=file)

    def format_prefix(prefix):
        # type: (Text) -> Text
        """Format the prefix lines."""
        if not with_color:
            return prefix
        return "\x1b[32m%s\x1b[0m" % prefix

    def format_dirname(dirname):
        # type: (Text) -> Text
        """Format a directory name."""
        if not with_color:
            return dirname
        return "\x1b[1;34m%s\x1b[0m" % dirname

    def format_error(msg):
        # type: (Text) -> Text
        """Format an error."""
        if not with_color:
            return msg
        return "\x1b[31m%s\x1b[0m" % msg

    def format_filename(fname):
        # type: (Text) -> Text
        """Format a filename."""
        if not with_color:
            return fname
        if fname.startswith("."):
            fname = "\x1b[33m%s\x1b[0m" % fname
        return fname

    def sort_key_dirs_first(info):
        # type: (Info) -> Tuple[bool, Text]
        """Get the info sort function with directories first."""
        return (not info.is_dir, info.name.lower())

    def sort_key(info):
        # type: (Info) -> Text
        """Get the default info sort function using resource name."""
        return info.name.lower()

    counts = {"dirs": 0, "files": 0}

    def format_directory(path, levels):
        # type: (Text, List[bool]) -> None
        """Recursive directory function."""
        try:
            directory = sorted(
                    fs.scandir(path, namespaces=["base", "details"]),
                    key=sort_key_dirs_first if dirs_first else sort_key,  # type: ignore
            )
        except Exception as error:
            prefix = (
                    "".join(indent if last else line_indent for last in levels)
                    + char_corner
                    + char_line
            )
            write(
                    "{} {}".format(
                            format_prefix(prefix), format_error("error ({})".format(error))
                    )
            )
            return
        _last = len(directory) - 1
        for i, info in enumerate(directory):
            is_last_entry = i == _last
            counts["dirs" if info.is_dir else "files"] += 1
            prefix = "".join(indent if last else line_indent for last in levels)
            prefix += char_corner if is_last_entry else char_newnode
            if info.is_dir:
                write(
                        "{} {}".format(
                                format_prefix(prefix + char_line), format_dirname(info.name)
                        )
                )
                if max_levels is None or len(levels) < max_levels:
                    format_directory(join(path, info.name), levels + [is_last_entry])
            else:
                write(
                        "{} {} {}".format(
                                format_prefix(prefix + char_line), format_filename(info.name), size2human(info.size)
                        )
                )

    format_directory(abspath(normpath(path)), [])
    return counts["dirs"], counts["files"]

def size2human(size: int):
        if size < 1024:
            return "{:.1f}B".format(size)
        size /= 1024
        if size < 1024:
            return "{:.1f}KB".format(size)
        size /= 1024
        if size < 1024:
            return "{:.1f}MB".format(size)
        size /= 1024
        return "{:.1f}GB".format(size)
