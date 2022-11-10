import asyncio
import dataclasses
import math
from typing import Iterator, Tuple, List, Sequence

import aiohttp
from loguru import logger
from tqdm.asyncio import tqdm

import entity
import exceptions
import repository
from .balancer import Balancer
from .storage_base import UploadStatus


@dataclasses.dataclass(kw_only=True)
class BlockProgress:
    done: int
    total: int
    block_number: int
    duplicate_number: int


class Uploader:
    def __init__(self,
                 balancer: Balancer,
                 blocks_repo: repository.BlockRepo,
                 chunk_size: int = 64 * 2 ** 10,
                 repeat_count: int = 3,
                 parallel_num: int = 5):
        self._balancer = balancer
        self._blocks_repo = blocks_repo
        self._session = None
        self._progress: List[BlockProgress] = []
        self._chunk_size = chunk_size
        self._repeat_count = repeat_count  # number of upload attempts
        self._parallel_num = parallel_num  # number of simultaneous uploads

    async def _upload_block(self, block: entity.Block) -> Tuple[UploadStatus, entity.Block]:
        if block.cipher:
            block.data = block.cipher.encrypt(block.data)

        status = await block.storage.upload(block.name, block.data, self._session)

        if status == UploadStatus.OK:
            logger.info(f"Upload block: {block}")
        else:
            logger.warning(f"Failed to upload block: {block}: {status}")

        return status, block

    def _block_by_chunk(self, block: entity.Block) -> Iterator[bytes]:
        offset = 0
        while offset < len(block.data) - self._chunk_size:
            yield block.data[offset:offset + self._chunk_size]
            self._progress[block.number * block.file.duplicate_count + block.duplicate_number].done += 1
            offset += self._chunk_size

    async def _upload_block_by_chunks(self, block: entity.Block, ) -> Tuple[UploadStatus, entity.Block]:
        if block.cipher:
            block.data = block.cipher.encrypt(block.data)

        data = tqdm(self._block_by_chunk(block), disable=True)

        for _ in range(self._repeat_count):
            status = await block.storage.upload_by_chunks(block.name, data, self._session)

            if status == UploadStatus.OK:
                logger.info(f"Upload block: {block}")
                break
            else:
                logger.warning(f"Failed to upload block: {block}: {status}")

        return status, block

    def _block_generator(self, file: entity.File) -> Iterator[entity.Block]:
        """
        Iterate over file by blocks without duplicates
        """

        with open(file.path, "rb") as f:
            data = f.read(file.block_size)
            number = 0
            while data:
                blocks = [entity.Block(file=file, number=number, data=data, size=len(data))
                          for _ in range(file.duplicate_count)]
                self._balancer.fill_blocks(blocks)
                yield from blocks
                data = f.read(file.block_size)
                number += 1

    def _block_generator_and_filter(self,
                                    file: entity.File,
                                    uploaded_blocks: Sequence[entity.Block]) -> Iterator[entity.Block]:
        """
        Iterate over file by blocks with duplicates and filter already uploaded
        """
        block_generator = Uploader._block_generator(self, file)
        for block in block_generator:
            if block.number not in [block_.number for block_ in uploaded_blocks]:
                yield block

    async def _upload_blocks(self, blocks: Iterator[entity.Block]) -> List[Tuple[UploadStatus, entity.Block]]:
        """
        Upload parallel_num blocks simultaneously

        Return list of blocks not uploaded to storage with its upload status
        """

        upload_tasks: List[asyncio.Task] = []
        db_tasks: List[asyncio.Task] = []
        failed = []
        first = True
        totally_failed = []

        while first or upload_tasks or db_tasks:
            first = False
            try:
                for _ in range(self._parallel_num - len(upload_tasks)):
                    block = next(blocks)
                    upload_tasks.append(asyncio.create_task(self._upload_block_by_chunks(block)))
            except StopIteration:
                pass

            logger.trace(f"Upload tasks: {upload_tasks}")
            logger.trace(f"DB tasks: {db_tasks}")

            if upload_tasks:
                done, pending = await asyncio.wait(upload_tasks, return_when=asyncio.FIRST_COMPLETED)

                upload_tasks = list(pending)
                done_tasks: List[asyncio.Task] = list(done)

                for task in done_tasks:
                    status, block = task.result()

                    if status == UploadStatus.OK:
                        db_tasks.append(asyncio.create_task(self._blocks_repo.add_block(block)))
                    else:
                        failed.append(self._upload_block_by_chunks(block))
                        logger.error(f"Cannot load block {block}")
                db_tasks.append(asyncio.create_task(self._blocks_repo.commit()))

            if db_tasks:
                await asyncio.wait(db_tasks, return_when=asyncio.ALL_COMPLETED)
                db_tasks = []

        if len(failed) != 0:
            failed = [asyncio.create_task(coro) for coro in failed]
            done, _ = await asyncio.wait(failed, return_when=asyncio.ALL_COMPLETED)
            done: List[asyncio.Task] = list(done)

            for task in done:
                status, block = task.result()
                if status == UploadStatus.OK:
                    db_tasks.append(asyncio.create_task(self._blocks_repo.add_block(block)))
                else:
                    totally_failed.append((status, block))
        if db_tasks:
            await asyncio.gather(*db_tasks)
        await self._blocks_repo.commit()
        return totally_failed

    def _init_progress(self, file: entity.File):
        """
        Reset progress
        """

        block_count = math.ceil(file.size / file.block_size) * file.duplicate_count
        total_chuck = math.ceil(file.block_size / self._chunk_size)
        self._progress = [BlockProgress(done=0,
                                        total=total_chuck,
                                        block_number=i // file.duplicate_count,
                                        duplicate_number=i % file.duplicate_count)
                          for i in range(block_count)]

        for i in range(file.duplicate_count):
            self._progress[-i - 1].total = math.ceil((file.size % file.block_size) / self._chunk_size)

    async def upload_file(self, file: entity.File) -> List[Tuple[UploadStatus, entity.Block]]:
        """
        Return list of files not uploaded to storage (if empty then everything is ok)

        Raise FileAlreadyExists if file.filename already exists in repository
        """

        uploaded_blocks = []
        try:
            db_file = await self._blocks_repo.get_file_by_filename(file.filename)
        except exceptions.UnknownFile as e:
            await self._blocks_repo.add_file(file)
            await self._blocks_repo.commit()
        else:
            file.id = db_file.id
            if file.total_blocks == db_file.uploaded_blocks:
                raise exceptions.FileAlreadyExists()
            uploaded_blocks = await self._blocks_repo.get_blocks_by_file(file)

        if uploaded_blocks:
            blocks = self._block_generator_and_filter(file, uploaded_blocks)
        else:
            blocks = self._block_generator(file)

        self._init_progress(file)

        unloaded_blocks = await self._upload_blocks(blocks)
        if unloaded_blocks:
            logger.error(f"Failed to upload file {file}: Can't upload blocks: {uploaded_blocks}")

        logger.info(f"Upload file: {file}")

        return unloaded_blocks

    @property
    def progress(self) -> List[BlockProgress]:
        return self._progress

    async def __aenter__(self) -> "Uploader":
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, *args):
        await self._session.close()
