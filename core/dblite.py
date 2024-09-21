from decimal import Decimal
import os
import sqlite3
import errno
from os.path import isfile
from functools import cache
import logging
from typing import Dict, Tuple, Any, Union, List, Set, Callable
import re

logger = logging.getLogger(__name__)


def gW(arr: Union[List, Tuple, Set]):
    arr = tuple(sorted(arr))
    if len(arr) == 0:
        raise ValueError(f"El campo arr no puede estar vacío")
    if len(arr) > 1:
        return f" in {arr}"
    if isinstance(arr[0], str):
        return f" = '{arr[0]}'"
    return f"={arr[0]}"



def mkAgregator(fnc: Callable):
    class MyAgregador:
        def __init__(self):
            self.valores = []

        def step(self, valor):
            if valor is not None:
                self.valores.append(valor)

        def finalize(self):
            if len(self.valores) == 0:
                return None
            return fnc(self.valores)

    return MyAgregador


def dict_factory(cursor: sqlite3.Cursor, row: Tuple):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def ResultIter(cursor: sqlite3.Cursor, size: int = 1000):
    while True:
        results = cursor.fetchmany(size)
        if not results:
            break
        for result in results:
            yield result


class DBLiteException(sqlite3.OperationalError):
    pass


class EmptyUpSertException(DBLiteException):
    pass

class SqlException(DBLiteException):
    pass


MEMORY = ":memory:"


class DBLite:
    @staticmethod
    def __format_sql(sql, vals):
        vls = []
        for v in vals:
            if isinstance(v, str):
                vls.append(f"'{v}'")
            elif v is None:
                vls.append('NULL')
            else:
                vls.append(str(v))

        for v in vls:
            sql = sql.replace('?', v, 1)

        return sql

    def __init__(
            self,
            file: str = MEMORY,
            extensions: Union[None, Tuple] = None,
            reload: bool = False,
            readonly: bool = False,
            trim_str: bool = True,
            empty_is_null: bool = True,
            commit_every_x_changes: int = 1000
    ):
        if reload and isfile(self.__file):
            os.remove(self.__file)
        self.__file = file
        self.__readonly = readonly
        self.__extensions = extensions or tuple()
        self.__inTransaction = False
        self.__changes = 0
        self.__trim_str = trim_str
        self.__empty_is_null = empty_is_null
        self.__commit_every_x_changes = commit_every_x_changes
        self.__con: sqlite3.Connection = self.__get__connection()

    def __get__connection(self):
        if self.__readonly:
            if self.__file == MEMORY:
                raise DBLiteException(
                    f"{MEMORY} and readonly={self.__readonly} doesn't make sense"
                )
            if not isfile(self.__file):
                raise FileNotFoundError(
                    errno.ENOENT,
                    os.strerror(errno.ENOENT),
                    self.__file
                )

        logger.info("sqlite: " + self.__file)
        if self.__readonly:
            file = "file:" + self.__file + "?mode=ro"
            con = sqlite3.connect(file, uri=True)
        else:
            con = sqlite3.connect(self.__file)
        if self.__extensions:
            con.enable_load_extension(True)
            for e in self.__extensions:
                con.load_extension(e)
        return con

    def __enter__(self, *args, **kwargs):
        return self

    def __exit__(self, *args, **kwargs):
        self.close()

    def openTransaction(self):
        if self.__inTransaction:
            self.__con.execute("END TRANSACTION")
        self.__con.execute("BEGIN TRANSACTION")
        self.__inTransaction = True

    def closeTransaction(self):
        if self.__inTransaction:
            self.__con.execute("END TRANSACTION")
            self.__inTransaction = False

    def execute(self, sql: str):
        try:
            self.__con.executescript(sql)
        except sqlite3.OperationalError as e:
            raise SqlException(sql) from e
        self.__con.commit()
        self.clear_cache()

    def clear_cache(self):
        self.get_cols.cache_clear()
        self.get_sql_table.cache_clear()

    @property
    def tables(self) -> Tuple[str]:
        return self.to_tuple("SELECT name FROM sqlite_master WHERE type='table' order by name")

    @property
    def indices(self):
        return self.to_tuple("SELECT name FROM sqlite_master WHERE type='index' order by name")

    @cache
    def get_sql_table(self, table: str):
        return self.one("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", table)

    @cache
    def get_cols(self, sql: str) -> Tuple[str]:
        _sql = sql.lower().split()
        if len(_sql) == 1:
            sql = f"select * from {sql} limit 0"
        elif _sql[-1] != "limit":
            sql = sql + " limit 0"
        cursor = self.__con.cursor()
        cursor.execute(sql)
        cols = tuple(col[0] for col in cursor.description)
        cursor.close()
        return cols

    def insert(self, table: str, insert_or="", **kwargs):
        insert_or = (insert_or or "").strip()
        if len(insert_or) > 0:
            insert_or = (" or "+insert_or).upper()

        data = self.__sanitize_row(table, kwargs, skip_null=True)

        keys = []
        vals = []
        for k, v in data.items():
            keys.append('"' + k + '"')
            vals.append(v)

        prm = ['?'] * len(vals)
        sql = f"INSERT{insert_or} INTO {table} {tuple(keys)} VALUES {tuple(prm)}"
        self.__upsert(sql, vals)

    def update(self, table: str, where: Union[None, Dict[str, Any]], **kwargs):
        data = self.__sanitize_row(table, kwargs, skip_null=False)
        if where:
            where = self.__sanitize_row(table, where, skip_null=False)

        updt = []
        whre = []
        vals = []
        for k, v in data.items():
            updt.append('"' + k + '" = ?')
            vals.append(v)

        for k, v in (where or {}).items():
            if v is None:
                whre.append('"' + k + '" is null')
            else:
                whre.append('"' + k + '" = ?')
            vals.append(v)

        sql = f"UPDATE {table} SET {', '.join(updt)}"
        if whre:
            sql = sql + f" where {' and '.join(whre)}"

        self.__upsert(sql, vals)

    def __upsert(self, sql: str, vals: List):
        try:
            self.__con.execute(sql, vals)
            self.__change_occurred()
        except sqlite3.OperationalError as e:
            sql = DBLite.__format_sql(sql, vals)
            raise EmptyUpSertException(sql) from e

    def __change_occurred(self):
        self.__changes = self.__changes + 1
        if self.__commit_every_x_changes < 0:
            return False
        if self.__changes % self.__commit_every_x_changes != 0:
            return False
        self.__con.commit()
        return True

    def __sanitize_row(self, table: str, kwargs: Dict[str, Any], skip_null=False):
        data = {}
        ok_keys = tuple(k.lower() for k in self.get_cols(table))
        for k, v in kwargs.items():
            k = k.lower()
            if k not in ok_keys:
                continue
            if isinstance(v, str):
                if self.__trim_str:
                    v = v.strip()
                if self.__empty_is_null and len(v) == 0:
                    v = None
            if isinstance(v, Decimal):
                v = float(v)
            if skip_null and v is None:
                continue
            data[k] = v

        if len(data) == 0:
            raise EmptyUpSertException(f"upsert into {table} malformed: give {kwargs}, needed {ok_keys}")

        return data

    def commit(self):
        self.__con.commit()

    def close(self, vacuum=True):
        if self.__readonly:
            self.__con.close()
            return
        self.closeTransaction()
        self.__con.commit()
        if vacuum:
            ic = self.get_integrity_check() or "¿?"
            logger.info(f"integrity_check = {ic}")
            fkc = self.get_foreign_key_check()
            logger.info("foreign_key_check = " + ("ko" if fkc else "ok"))
            for table, parent in fkc:
                logger.info(f"  {table} -> {parent}")
            self.__con.execute("VACUUM")
        self.__con.commit()
        self.__con.close()

    def get_integrity_check(self):
        return self.one("pragma integrity_check")

    def get_foreign_key_check(self):
        data: List[Tuple[str, str]] = []
        for i in self.select("pragma foreign_key_check"):
            tp = (i[0], i[2])
            if tp not in data:
                data.append(tp)
        return tuple(data)

    def select(self, sql: str, *args, row_factory=None, **kwargs):
        self.__con.row_factory = row_factory
        cursor = self.__con.cursor()
        if len(args):
            cursor.execute(sql, args)
        else:
            cursor.execute(sql)
        for r in ResultIter(cursor):
            yield r
        cursor.close()
        self.__con.row_factory = None

    def to_tuple(self, *args, **kwargs):
        arr = []
        for i in self.select(*args, **kwargs):
            if isinstance(i, (tuple, list)) and len(i)==1:
                i = i[0]
            arr.append(i)
        return tuple(arr)

    def getkv(self, *args, **kwargs):
        dct = {}
        for row in self.select(*args, **kwargs):
            if len(row) != 2:
                raise ValueError("Para formar un diccionario se necesita dos columnas")
            k, v = row
            if k in dct and dct[k] != v:
                raise ValueError("La consulta no da un valor único por clave")
            dct[k] = v
        return dct

    def one(self, sql: str, *args, row_factory=None):
        self.__con.row_factory = row_factory
        cursor = self.__con.cursor()
        if len(args):
            cursor.execute(sql, args)
        else:
            cursor.execute(sql)
        r = cursor.fetchone()
        cursor.close()
        self.__con.row_factory = None
        if not r:
            return None
        if isinstance(r, tuple) and len(r) == 1:
            return r[0]
        return r

    def iter_sql_backup(self, width_values=-1, multiple_limit=-1):
        re_insert = re.compile(r'^INSERT\s+INTO\s+(.+)\s+VALUES\s*\((.*)\);$')
        yield 'PRAGMA foreign_keys=OFF;'
        yield 'BEGIN TRANSACTION;'
        for lines in self.__con.iterdump():
            for line in lines.split("\n"):
                ln = line.strip().upper()
                if ln in ("", "COMMIT;", "BEGIN TRANSACTION;"):
                    continue
                if ln.startswith("INSERT INTO ") or ln.startswith("--"):
                    continue
                yield line
        table = None
        lsttb = None
        count = 0
        values: List[str] = []

        def val_to_str(vls: List[str], end: str):
            return ",".join(vls)+end

        for line in self.__con.iterdump():
            m = re_insert.match(line)
            if m is None:
                continue
            if multiple_limit == 1:
                yield line
                continue
            table = m.group(1).strip('"')
            if table != lsttb or count == 0:
                if values:
                    yield val_to_str(values, ";")
                    values = []
                yield f"INSERT INTO {table} VALUES"
                count = multiple_limit
            values.append("("+m.group(2)+")")
            if len(values) > 1 and len(",".join(values)) > width_values:
                yield val_to_str(values[:-1], ",")
                values = [values[-1]]
            count = count - 1
            lsttb = table
        if values:
            yield val_to_str(values, ";")
            values = []

        yield 'COMMIT;'
        yield 'VACUUM;'
        yield 'PRAGMA foreign_keys=ON;'
        yield 'pragma integrity_check;'
        yield 'pragma foreign_key_check;'

    def relocate(self, file: str):
        self.closeTransaction()
        self.__con.commit()

        self.__file = file
        if file == MEMORY:
            self.__readonly = True
        con = self.__get__connection()
        self.__con.backup(con)
        self.__con.close()
        self.__con = con

    def register_function(self, name: str, num_params: int, func: Callable, is_aggregate=False):
        if is_aggregate:
            self.__con.create_aggregate(name, num_params, mkAgregator(func))
        else:
            self.__con.create_function(name, num_params, func)


class LazzyDBLite:
    def __init__(self, *args, **kwargs):
        self.__args = args
        self.__kwargs = kwargs
        self.__db: Union[None, DBLite] = None

    @property
    def db(self) -> DBLite:
        if self.__db is None:
            self.__db = DBLite(*self.__args, **self.__kwargs)
        return self.__db
    
    def close(self):
        if self.__db is not None:
            self.__db.close()
            self.__db = None
