import argparse
import os

import aiohttp
import aiosqlite
from loguru import logger

import entities
from cli.parser import Parser
from config import Config
from exceptions import *
from network.balancer import Balancer
from network.downloader import Downloader
from network.storage import StorageType
from network.storage_creator import StorageCreator
from network.uploader import Uploader
from repository.block_repo import BlockRepo
from vfs import VFS


class CLI:
    def __init__(self, config: Config, parser: Parser):
        self._balancer: Balancer = None
        self._block_repo: BlockRepo = None
        self._vfs: VFS = None
        self._parser = parser
        self.config = config
        self._init_parser()

    async def init(self):
        self._block_repo = await BlockRepo(self.config.db_path)
        disks = await self._block_repo.get_storages()
        self._balancer = Balancer(disks)

    @staticmethod
    def _replace_line(s: str):
        print("\r" + s, end='')

    def _init_parser(self):
        self._parser.set_upload_handler(self._upload_handler)
        self._parser.set_download_handler(self._download_handler)
        self._parser.set_addstorage_handler(self._addstorage_handler)
        self._parser.set_list_handler(self._list_handler)
        self._parser.set_liststorage_handler(self._liststorage_handler)

    async def _upload_handler(self, args: argparse.Action):
        src, dst = args.src, args.dst
        if not dst:
            _, dst = os.path.split(src)

        block_size = args.block_size
        file = entities.File(filename=dst, path=src, block_size=block_size)

        print(f"Upload file {repr(src)} like {repr(dst)}")

        try:
            await self._upload_file(file)
        except NoStorage as e:
            print("No disks. Add one by addstore command")
            logger.exception(e)
            return
        except FileAlreadyExists as e:
            print("File with this name already exists")
            logger.exception(e)
            return

        print("\nFile successfully uploaded")

    async def _download_handler(self, args: argparse.Action):
        src, dst = args.src, args.dst
        temp_dir = args.temp_dir
        if not dst:
            _, dst = os.path.split(src)

        if not os.path.isdir(temp_dir):
            os.makedirs(temp_dir)

        print(f"Start downloading file {repr(src)} to {repr(dst)}")
        await self._download_file(src, dst, temp_dir)
        print(f"\nFile {src} successfully downloaded to {dst}")

    async def _addstorage_handler(self, args: argparse.Action):
        token = args.token
        type_ = args.type

        storage_type = StorageType.from_str(type_)
        if not storage_type:
            print("Unknown storage type")
            return

        storage = StorageCreator.create(storage_type)
        storage.token = token

        try:
            await self._block_repo.add_storage(storage)
            print(f"Token {token} added")
        except aiosqlite.IntegrityError as e:
            print(f"Token {token} already exists")
            logger.exception(e)

        await self._block_repo.commit()

    async def _list_handler(self, args: argparse.Action):
        self._vfs = VFS(self._block_repo)
        await self._vfs.load()
        self._vfs.tree()

    async def _liststorage_handler(self, args: argparse.Action):
        async with aiohttp.ClientSession() as s:
            storages = await self._block_repo.get_storages()
            for storage in storages:
                storage.used_space, storage.total_space = await storage.size(s)

            used_space = self._size2human(storage.used_space)
            total_space = self._size2human(storage.total_space)

            print("{id} {type}, {used_space}/{total_space}".format(
                id=storage.id,
                type=storage.type,
                used_space=used_space,
                total_space=total_space,
            ))

    @staticmethod
    def _size2human(size: int):
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

    async def _download_file(self, src: str, dst: str, temp_dir: str) -> None:
        async with Downloader(self._block_repo) as downloader:
            file = await self._block_repo.get_file_by_filename(entities.File(filename=src))
            file.path = dst

            total_count = await downloader.count_blocks(file)

            async for done in downloader.download_file(file, temp_dir=temp_dir):
                self._replace_line(f"{done}/{total_count} blocks downloaded")

    async def _upload_file(self, file: entities.File) -> None:
        async with Uploader(self._balancer, self._block_repo) as u:
            total_blocks = u.count_blocks(file)
            file.size = os.path.getsize(file.path)
            self._replace_line(f"0/{total_blocks} blocks uploaded")
            try:
                async for done in u.upload_file(file):
                    self._replace_line(f"{done}/{total_blocks} blocks uploaded")
            except Exception as e:
                logger.exception(e)
                raise

    def _parse(self):
        return self._parser.parse_args()

    async def start(self):
        args = self._parse()
        await args.func(args)
        await self._block_repo.close()
