import argparse
import asyncio
import os
import uuid
from typing import List, Callable, Any, Optional

import aiohttp
import aiosqlite
from loguru import logger
from tabulate import tabulate

import entity
import exceptions
from cli.parser import Parser
from crypto.aes import Aes
from exceptions import *
from network.balancer import Balancer
from network.block_progress import BlockProgress
from network.downloader import Downloader, ChecksumNoEqual
from network.storage_base import StorageType, DeleteStatus, DownloadStatus
from network.storage_creator import StorageCreator
from network.uploader import Uploader
from repository.block_repo import BlockRepo
from vfs import VFS


class CLI:
    def __init__(self, parser: Parser):
        self._balancer: Balancer = None
        self._block_repo: BlockRepo = None
        self._vfs: VFS = None
        self._parser = parser
        self._init_parser()

    async def init(self):
        self._block_repo = await BlockRepo(self._parser.parse_args().db_path)

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
        self._parser.set_key_generate_handler(self._key_generate_handler)
        self._parser.set_key_list_handler(self._key_list_handler)

        self._parser.set_empty_handler(self._empty_handler)

    # HANDLERS

    async def _upload_handler(self, args: argparse.Action):
        src, dst = args.src, args.dst
        block_size = args.block_size

        if not dst:
            _, dst = os.path.split(src)

        if not os.path.isfile(src):
            print(f"Cannot open file {src}")
            return

        if args.need_encrypt:
            keys = await self._block_repo.get_keys()
            if not keys:
                print("No keys. Add one by 'key add' or don't use '-e' parameter")
                return

            ciphers = [Aes(key) for key in keys]
        else:
            ciphers = None

        storages = await self._block_repo.get_storages()
        self._balancer = Balancer(storages,
                                  ciphers=ciphers,
                                  block_size=block_size)

        file = entity.File(filename=dst, path=src)

        print(f"Upload file {repr(src)} like {repr(dst)}\n")

        try:
            await self._upload_file(file)
        except NoStorage as e:
            print("No storage. Add one by 'storage add' command")
            logger.exception(e)
            return
        except FileAlreadyExists as e:
            print(f"File with name {file.filename} already exists")
            logger.exception(e)
            return
        except CancelAction as e:
            print("Canceled")
            logger.exception(e)
            return
        except Exception as e:
            logger.exception(e)
            print(e)
            print("Exit")
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
        except ChecksumNoEqual as e:
            logger.exception(e)
            print(f"Checksums not equal")
        except Exception as e:
            self._fatal_error()
            logger.exception(e)
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

            print(tabulate([(storage.id,
                             storage.type,
                             f"{self._size2human(storage.used_space)}/{self._size2human(storage.total_space)}")
                            for storage in storages], headers=["id", "type", "space"]))

    async def _storage_wipe_handler(self, args: argparse.Action):
        storage_id = args.storage_id
        worker_count = args.worker_count

        async def worker(queue: asyncio.Queue[entity.Block], session: aiohttp.ClientSession):
            while not queue.empty():
                block = await queue.get()
                await self._delete_block(block, session)
                queue.task_done()

        if self._yes_or_no("Are you sure you want to wipe ENTIRELY storage?"):
            if not self._yes_or_no("Are you REALLY want to WIPE ENTIRELY storage?"):
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
                queue.put_nowait(entity.Block(name=file.filename, storage=storage))

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
                block = entity.Block(storage=storage, name=filename)
                tasks.append(asyncio.create_task(self._delete_block(block, session)))
            await asyncio.gather(*tasks)

    async def _list_handler(self, args: argparse.Action):
        self._vfs = VFS(self._block_repo)
        await self._vfs.load()
        # await asyncio.sleep(2)
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
            await self._block_repo.add_key(entity.Key(key=key))
            await self._block_repo.commit()
        except aiosqlite.IntegrityError:
            print("This key already exists")
            return
        print("Key successfully added")

    async def _key_generate_handler(self, args: argparse.Action):
        key = uuid.uuid4().hex
        await self._block_repo.add_key(entity.Key(key=key))
        await self._block_repo.commit()
        print("Key successfully added")

    async def _key_list_handler(self, args: argparse.Action):
        keys = await self._block_repo.get_keys()
        table = []
        for key in keys:
            table.append((key.id, key.key))
        print(tabulate(table, headers=["id", "key"]))

    async def _empty_handler(self, args: argparse.Action):
        print("No params")

    # OTHER

    async def _delete_block(self, block: entity.Block, session: aiohttp.ClientSession):
        status = await block.storage.delete(block.name, session)
        if status == DeleteStatus.OK:
            print(f"Block {block.name} on storage #{block.storage.id} is deleted")
            await self._block_repo.del_block(entity.Block(name=block.name, storage=block.storage))
        else:
            print(f"Failed to delete block {block.name}")

    async def _delete_file(self, file: entity.File):
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
        print("{} [Y/N]".format(s))
        line = input()
        while not line:
            line = input()

        return line.lower().startswith("y")

    @staticmethod
    def _progress_bar(iteration: int,
                      total: int,
                      prefix: str = '',
                      suffix: str = '',
                      decimals: int = 1,
                      length: int = 60,
                      fill: str = '█',
                      end: str = "\n"):
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
            end    - Optional  : end character (e.g. "\r", "\r\n") (Str)
        """
        percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
        filledLength = int(length * iteration // total)
        bar = fill * filledLength + '-' * (length - filledLength)
        print(f'\r{prefix} |{bar}| {percent}% {suffix}', end=end)

    @staticmethod
    def _multi_progress_bar(progress: List[BlockProgress]) -> int:
        if not progress:
            return 0

        indexed_progress = [block_progress
                            for block_progress in progress
                            if block_progress.done != 0]

        logger.info([f"{p.done}/{p.total}" for p in indexed_progress])

        for block_progress in indexed_progress:
            CLI._progress_bar(block_progress.done, block_progress.total,
                              prefix=f"b {block_progress.block_number} d {block_progress.duplicate_number}:")

        CLI._progress_bar(len([0 for block_progress in progress
                               if block_progress.done == block_progress.total]), len(progress), prefix="Total:")

        return len(indexed_progress) + 1

    @staticmethod
    def _go_up(count: int = 1):
        print(end=("\033[A" * count))

    @staticmethod
    def _go_down(count: int = 1):
        print(end=("\033[B" * count))

    async def _download_file(self, src: str, dst: str, temp_dir: str) -> None:
        """
        Download file by name in system

        :param src: Source file path
        :param dst: Destination file path
        :param temp_dir: Path to directory for blocks
        :return:
        """
        async with Downloader(self._block_repo) as downloader:
            file = await self._block_repo.get_file_by_filename(src)
            file.path = dst

            block_size = self._size2human(file.size // file.total_blocks)
            print(f"File {file.filename} consist of {file.total_blocks} {block_size} blocks")

            download_task = asyncio.create_task(downloader.download_file(file, temp_dir=temp_dir))
            logger.info("Create download task")
            bar_size = 0
            async for progress in self._poll_task(0.5, download_task, lambda: downloader.progress):
                bar_size = self._multi_progress_bar(progress)
                self._go_up(bar_size)
            self._go_down(bar_size + 1)

            download_task.result()

    @staticmethod
    async def _poll_task(period: float, task: asyncio.Task, func: Callable[[], Any]) -> Optional[Any]:
        """
        Execute some function while given task not done every period seconds

        Yield func result
        """

        while not task.done():
            yield func()
            await asyncio.sleep(period)

    async def _upload_file(self, file: entity.File) -> None:
        """
        Upload file

        If user don't confirm operation it will raise CancelAction
        """
        async with Uploader(self._balancer, self._block_repo) as uploader:
            self._balancer.fill_file(file)

            print(f"File {file.filename} split into {file.total_blocks} {self._size2human(file.block_size)} blocks.")
            if not self._yes_or_no(f"Are you sure you want to load it?"):
                raise exceptions.CancelAction()

            partially_loaded = True
            try:
                db_file = await self._block_repo.get_file_by_filename(file.filename)
            except UnknownFile as e:
                partially_loaded = False

            if partially_loaded and db_file.total_blocks != db_file.uploaded_blocks:
                print(f"File {file.filename} partially loaded ({db_file.uploaded_blocks}/{db_file.total_blocks}).")
                if not self._yes_or_no(f"Do you want to continue load?"):
                    raise exceptions.CancelAction()

            upload_task = asyncio.create_task(uploader.upload_file(file))
            bar_size = 0
            async for progress in self._poll_task(0.5, upload_task, lambda: uploader.progress):
                bar_size = self._multi_progress_bar(progress)
                self._go_up(bar_size)
            self._go_down(bar_size + 1)

            unloaded_blocks = upload_task.result()
            if unloaded_blocks:
                print("Failed to load following blocks:")
                for status, block in unloaded_blocks:
                    print(f"block_number={block.number} status={status}")
                raise exceptions.UploadFailed()

    def _parse(self):
        return self._parser.parse_args()

    @staticmethod
    def _fatal_error():
        print("Fatal error occurred. Check log file")

    async def start(self):
        args = self._parse()
        await args.func(args)

    async def close(self):
        await self._block_repo.close()

    def interrupt(self):
        print("\nInterrupted")
