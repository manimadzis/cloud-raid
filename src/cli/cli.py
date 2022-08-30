import argparse
import asyncio
import os
from typing import List, Callable, Any

import aiohttp
import aiosqlite
from loguru import logger

import entities
import exceptions
from cli.parser import Parser
from config import Config
from crypto.aes import Aes
from exceptions import *
from network.balancer import Balancer
from network.downloader import Downloader
from network.storage_base import StorageType, DeleteStatus, DownloadStatus
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
        self._config = config
        self._init_parser()

    async def init(self):
        self._block_repo = await BlockRepo(self._config.db_path)

    @staticmethod
    def _replace_line(s: str):
        print("\r" + s, end='')

    def _init_parser(self):
        self._parser.set_upload_handler(self._upload_handler)
        self._parser.set_download_handler(self._download_handler)
        self._parser.set_list_handler(self._list_handler)
        self._parser.set_delete_handler(self._delete_handler)

        self._parser.set_storage_add_handler(self._storage_add_handler)
        self._parser.set_storage_list_handler(self._storage_list_handler)
        self._parser.set_storage_files_handler(self._storage_files_handler)
        self._parser.set_storage_delete_handler(self._storage_delete_handler)
        self._parser.set_storage_wipe_handler(self._storage_wipe_handler)

        self._parser.set_key_add_handler(self._key_add_handler)
        self._parser.set_key_list_handler(self._key_list_handler)

    # HANDLERS

    async def _upload_handler(self, args: argparse.Action):
        src, dst = args.src, args.dst
        block_size = args.block_size

        if not dst:
            _, dst = os.path.split(src)

        if args.cipher:
            keys = await self._block_repo.get_keys()
            if not keys:
                print("No keys. Add one by 'key add' or don't use '-c' parameter")
                return

            ciphers = [Aes(key) for key in keys]
        else:
            ciphers = []

        storages = await self._block_repo.get_storages()
        self._balancer = Balancer(storages, ciphers=ciphers, min_block_size=self._config.min_block_size,
                                  max_block_size=self._config.max_block_size, block_size=block_size)

        file = entities.File(filename=dst, path=src)

        print(f"Upload file {repr(src)} like {repr(dst)}")

        try:
            await self._upload_file(file)
        except NoStorage as e:
            print("No disks. Add one by 'store add' command")
            logger.exception(e)
            return
        except FileAlreadyExists as e:
            print("File with this name already exists")
            logger.exception(e)
            return
        except CancelAction as e:
            print()
            logger.exception(e)
            return

        print("File successfully uploaded")

    async def _download_handler(self, args: argparse.Action):
        src, dst = args.src, args.dst
        temp_dir = args.temp_dir
        if not dst:
            _, dst = os.path.split(src)

        if not os.path.isdir(temp_dir):
            os.makedirs(temp_dir)

        print(f"Start downloading file {repr(src)} to {repr(dst)}")
        try:
            await self._download_file(src, dst, temp_dir)
        except Exception as e:
            print(e)
            return
        print(f"\nFile {src} successfully downloaded to {dst}")

    async def _storage_add_handler(self, args: argparse.Action):
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
            print(f"Storage {type_} {token} added")
        except aiosqlite.IntegrityError as e:
            print(f"Storage {type_} {token} already exists")
            logger.exception(e)

        await self._block_repo.commit()

    async def _storage_files_handler(self, args: argparse.Action):
        storage_id = args.storage_id

        async with aiohttp.ClientSession() as session:
            storage = await self._block_repo.get_storage_by_id(storage_id)
            status, files = await storage.files(session)
        if status != DownloadStatus.OK:
            print("Cannot get files in storage. Something went wrong. Please check log file")
            return

        for file in files:
            print(file.filename, self._size2human(file.size))

    async def _storage_list_handler(self, args: argparse.Action):
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

    async def _storage_wipe_handler(self, args: argparse.Action):
        storage_id = args.storage_id
        worker_count = args.worker_count

        async def worker(queue: asyncio.Queue[entities.Block], session: aiohttp.ClientSession):
            while not queue.empty():
                block = await queue.get()
                await self._delete_block(block, session)
                queue.task_done()

        if self._yes_or_no("Are you sure you want to wipe ENTIRELY storage? [y/n]"):
            if not self._yes_or_no("Are you REALLY want to WIPE ENTIRELY storage? [y/n]"):
                return

        try:
            storage = await self._block_repo.get_storage_by_id(storage_id)
        except exceptions.UnknownStorage as e:
            logger.exception(e)
            print("Unknown storage id. Use 'storage list' to see all storages")
            return

        async with aiohttp.ClientSession() as session:
            status, files = await storage.files(session)
            if status != DownloadStatus.OK:
                print("Cannot get files in storage. Something went wrong. Please check log file")
                return

            queue = asyncio.Queue()
            for file in files:
                queue.put_nowait(entities.Block(name=file.filename, storage=storage))

            tasks = []
            for _ in range(worker_count):
                tasks.append(asyncio.create_task(worker(queue, session)))

            await queue.join()

    async def _storage_delete_handler(self, args: argparse.Action):
        storage_id = args.storage_id
        filenames = args.filenames

        storage = await self._block_repo.get_storage_by_id(storage_id)
        tasks = []
        async with aiohttp.ClientSession() as session:
            for filename in filenames:
                block = entities.Block(storage=storage, name=filename)
                tasks.append(asyncio.create_task(self._delete_block(block, session)))
            await asyncio.gather(*tasks)

    async def _list_handler(self, args: argparse.Action):
        self._vfs = VFS(self._block_repo)
        await self._vfs.load()
        print(self._vfs.tree())

    async def _delete_handler(self, args: argparse.Action):
        filenames = args.filenames

        files = []
        tasks = []
        for filename in filenames:
            question = f"File {filename} will be deleted. Are you sure you want to load it?[y/n]"
            if not self._yes_or_no(question):
                continue
            tasks.append(asyncio.create_task(self._block_repo.get_file_by_filename(filename)))
        await asyncio.gather(*tasks, return_exceptions=True)

        for task in tasks:
            try:
                files.append(task.result())
            except exceptions.UnknownFile as e:
                print("Unknown file")
                logger.exception(e)
                return

        for file in files:
            await self._delete_file(file)

    async def _key_add_handler(self, args: argparse.Action):
        key = args.key

        try:
            await self._block_repo.add_key(entities.Key(key=key))
            await self._block_repo.commit()
        except aiosqlite.IntegrityError:
            print("This key already exists")
            return
        print("Key successfully added")

    async def _key_list_handler(self, args: argparse.Action):
        keys = await self._block_repo.get_keys()
        for key in keys:
            print("{id} {key}".format(id=key.id, key=key.key))

    # OTHER

    async def _delete_block(self, block: entities.Block, session: aiohttp.ClientSession):
        status = await block.storage.delete(block.name, session)
        if status == DeleteStatus.OK:
            print(f"Block {block.name} on storage #{block.storage.id} deleted")
            await self._block_repo.del_block(entities.Block(name=block.name, storage=block.storage))
        else:
            print(f"Failed to delete block {block.name}")

    async def _delete_file(self, file: entities.File):
        blocks = await self._block_repo.get_blocks_by_file(file)

        async with aiohttp.ClientSession() as session:
            tasks = []
            for block in blocks:
                tasks.append(asyncio.create_task(self._delete_block(block, session)))
            await asyncio.gather(*tasks)
        await self._block_repo.del_file(file)
        await self._block_repo.commit()
        print(f"File {file.filename} deleted")

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

    @staticmethod
    def _yes_or_no(s: str) -> bool:
        print(s)
        return input().startswith("y")

    @staticmethod
    def _progress_bar(iteration, total, prefix='', suffix='', decimals=1, length=60, fill='█', end="\n"):
        """
        Call in a loop to create terminal progress bar
        @params:
            iteration   - Required  : current iteration (Int)
            total       - Required  : total iterations (Int)
            prefix      - Optional  : prefix string (Str)
            suffix      - Optional  : suffix string (Str)
            decimals    - Optional  : positive number of decimals in percent complete (Int)
            length      - Optional  : character length of bar (Int)
            fill        - Optional  : bar fill character (Str)
            printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
        """
        percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
        filledLength = int(length * iteration // total)
        bar = fill * filledLength + '-' * (length - filledLength)
        print(f'\r{prefix} |{bar}| {percent}% {suffix}', end=end)

    @staticmethod
    def _multi_progress_bar(progress: List[List[int]]):
        if not progress:
            return

        i = 0
        for _, (done, _) in enumerate(reversed(progress)):
            if done != 0:
                break
            i += 1

        last_started = len(progress) - i - 1
        if last_started == -1:
            return

        for i, (done, total) in enumerate(progress[:last_started], start=1):
            CLI._progress_bar(done, total=total, prefix=str(i))

        if last_started > 0:
            done, total = progress[last_started]
            CLI._progress_bar(done, total=total, prefix=str(last_started + 1), end=("\033[A" * last_started))

    async def _download_file(self, src: str, dst: str, temp_dir: str) -> None:
        async with Downloader(self._block_repo) as downloader:
            file = await self._block_repo.get_file_by_filename(src)
            file.path = dst

            try:
                download_task = asyncio.create_task(downloader.download_file(file, temp_dir=temp_dir))
                async for progress in self._poll_task(0.5, download_task, lambda: downloader.progress):
                    self._multi_progress_bar(progress)
            except Exception as e:
                logger.exception(e)
                print(e)

    @staticmethod
    async def _poll_task(period: float, task: asyncio.Task, func: Callable[[], Any]):
        while not task.done():
            yield func()
            await asyncio.sleep(period)

    async def _upload_file(self, file: entities.File) -> None:
        async with Uploader(self._balancer, self._block_repo) as uploader:
            file.block_size = self._balancer.block_size(file)
            file.block_count = uploader.count_blocks(file)
            file.size = os.path.getsize(file.path)

            question = f"File {file.filename} split into {file.block_count} {self._size2human(file.block_size)} blocks. Are you sure you want to load it?[y/n]"
            if not self._yes_or_no(question):
                raise exceptions.CancelAction()

            upload_task = asyncio.create_task(uploader.upload_file(file))
            async for progress in self._poll_task(0.5, upload_task, lambda: uploader.progress):
                self._multi_progress_bar(progress)

            print("\n" * len(uploader.progress))
            exc = upload_task.exception()
            if exc:
                raise exc

    def _parse(self):
        return self._parser.parse_args()

    async def start(self):
        args = self._parse()
        await args.func(args)
        await self._block_repo.close()
