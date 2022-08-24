from abc import ABC, abstractmethod
from typing import Generator

import aiosqlite


class AbstractRepo(ABC):
    def __init__(self, database: str):
        self.database = database
        self._conn = None

    async def _ainit(self) -> "AbstractRepo":
        self._conn = await aiosqlite.connect(self.database)
        self._conn.row_factory = aiosqlite.Row
        await self._create_tables()
        return self

    def __await__(self) -> Generator[None, None, "AbstractRepo"]:
        return self._ainit().__await__()

    @abstractmethod
    async def _create_tables(self) -> None:
        pass

    async def add_row(self, table: str, row_data: dict, replace=False) -> None:
        """
        Add data from dictionary to table

        :param table: название таблицы
        :param row_data: словарь с полями таблицы
        :param replace:
        :return:
        """
        clause = "INSERT OR REPLACE INTO" if replace else "INSERT INTO"
        keys = ','.join(row_data.keys())
        values = ','.join([repr(row_data[x]) for x in row_data])
        sql = f"{clause} {table}({keys}) VALUES ({values})"

        await self.execute(sql)

    async def execute(self, sql: str, params: tuple = ()) -> aiosqlite.Cursor:
        """
        Execute sql statement
        :param sql: SQL statement
        :param params:
        :return: cursor
        """
        return await self._conn.execute(sql, params)

    async def executemany(self, sql: str, params: tuple) -> aiosqlite.Cursor:
        return await self._conn.executemany(sql, params)

    async def executescript(self, sql_script: str) -> aiosqlite.Cursor:
        return await self._conn.executescript(sql_script)

    async def commit(self) -> None:
        await self._conn.commit()
