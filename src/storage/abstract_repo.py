import sqlite3
from abc import ABC, abstractmethod


class AbstractRepo(ABC):
    def __init__(self, database: str = None):
        self.database = database
        self._conn = sqlite3.connect(database)
        self._conn.row_factory = sqlite3.Row

        self._cur = self._conn.cursor()
        self._create_tables()

    @abstractmethod
    def _create_tables(self) -> None:
        pass

    def add_row(self, table: str, row_data: dict, replace=False) -> None:
        """
        Add data from dictionary to table

        :param table: название таблицы
        :param row_data: словарь с полями таблицы
        :param replace:
        :return:
        """
        clause = "INSERT OR REPLACE INTO" if replace else "INSERT INTO"
        keys = ','.join(row_data.keys())
        values = ','.join([str(row_data[x]) for x in row_data])
        sql = f"{clause} {table}({keys}) VALUES ({values.__repr__()})"

        self.execute(sql)

    def execute(self, sql: str, params: tuple = (), new_cursor=False) -> sqlite3.Cursor:
        """
        Execute sql statement
        :param sql: SQL statement
        :param params:
        :param new_cursor: if True - create new cursor and return it
        :return: cursor
        """
        if new_cursor:
            return self.execute(sql, params)

        return self._cur.execute(sql, params)

    def executemany(self, sql: str, params: tuple, new_cursor=False) -> sqlite3.Cursor:
        if new_cursor:
            return self.executemany(sql, params)
        return self._cur.executemany(sql, params)

    def executescript(self, sql_script: str) -> sqlite3.Cursor:
        return self._cur.executescript(sql_script)

    def commit(self) -> None:
        self._conn.commit()
