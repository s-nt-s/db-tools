from os.path import basename, join
from os import walk
import sqlite3
import pandas as pd
from unidecode import unidecode
import re

import logging
from core.dblite import DBLite
from core.dblite import MEMORY
from core.shell import Shell
import zipfile
import tempfile

logger = logging.getLogger(__name__)


def normalize_name(s: str, prefix: str):
    s = unidecode(s)
    s = s.strip()
    s = re.sub(r"[\s_\-\.\(\)]+", "_", s)
    s = s.strip("_ ")
    s = s.lower()
    if not s[0].isalpha():
        s = prefix + s
    return s


def can_be_int(s):
    if s is None:
        return True
    if isinstance(s, float):
        return int(s) == s
    if isinstance(s, int):
        return True
    if not isinstance(s, str):
        return False
    if s.isdigit():
        return True
    return False


def iter_zip(file: str):
    with tempfile.TemporaryDirectory() as temp_dir:
        with zipfile.ZipFile(file, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
            for root, subdirs, files in walk(temp_dir):
                for file in files:
                    yield join(root, file)


class NormLite(DBLite):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.register_function("can_be_int", 1, can_be_int)

    def count(self, table: str, where: str = None):
        sql = f'select count(*) from "{table}"'
        if where:
            sql = sql + " where " + where
        return self.one(sql)

    def notExists(self, table: str, where: str = None):
        return 0 == self.count(table, where)

    def get_new_type(self, current_type, table, column):
        if current_type == "TEXT":
            self.run_modify_query(f'UPDATE "{table}" SET "{column}" = TRIM("{column}") where "{column}" is not null;')
            self.run_modify_query(f'UPDATE "{table}" SET "{column}" = NULL where "{column}" = \'\';')
        if 0 == self.count(table, where=f'"{column}" is not null'):
            return current_type
        if current_type in ('REAL', 'TEXT') and self.notExists(table, where=f'not can_be_int("{column}")'):
            return 'INTEGER'
        if current_type == 'TEXT':
            min_length, max_length = self.one(f'select min(length("{column}")), max(length("{column}")) from "{table}" where "{column}" is not null')
            if min_length == max_length:
                return f"CHAR({max_length})"
            return f"VARCHAR({max_length})"
        return current_type

    def normalize(self):
        is_changed = False
        for original_table_name in self.tables:
            if self.__normalize(original_table_name):
                is_changed = True
        if is_changed:
            self.commit()
            self.execute('VACUUM;')
            self.execute('PRAGMA foreign_keys=ON;')
            self.execute('pragma integrity_check;')
            self.execute('pragma foreign_key_check;')

    def __normalize(self, original_table_name: str):
        need_normalize = False
        new_table_name = normalize_name(original_table_name, prefix="t")
        tmp_new_table_name = 'TMP_' + new_table_name

        if new_table_name != original_table_name:
            need_normalize = True

        columns_info = self.to_tuple(f'PRAGMA table_info("{original_table_name}")')

        columns_definitions = []
        columns_names = []
        for column in columns_info:
            old_column_name: str = column[1]
            new_column_name = normalize_name(old_column_name, prefix="c")
            column_type = self.get_new_type(column[2], original_table_name, old_column_name)
            if self.notExists(original_table_name, where=f'"{old_column_name}" is null'):
                column_type = f'{column_type} NOT NULL'
            columns_definitions.append(f"{new_column_name} {column_type}")
            columns_names.append((old_column_name, new_column_name))
            if new_column_name != old_column_name or column_type != column[2]:
                need_normalize = True

        if not need_normalize:
            return False

        new_columns_definitions_str = ", ".join(columns_definitions)
        create_new_table_sql = f'CREATE TABLE {tmp_new_table_name} ({new_columns_definitions_str});'

        # Execute the creation of the new table
        self.execute(create_new_table_sql)

        # Copy data from the original table to the new table
        old_columns_str = ", ".join(f'"{old_column_name}"' for old_column_name, _ in columns_names)
        new_columns_str = ", ".join(new_column_name for _, new_column_name in columns_names)
        copy_data_sql = f'INSERT INTO {tmp_new_table_name} ({new_columns_str}) SELECT {old_columns_str} FROM "{original_table_name}";'
        self.execute(copy_data_sql)

        # Drop the original table
        self.execute(f'DROP TABLE "{original_table_name}";')

        # Rename the new table to the old table name (normalized)
        self.execute(f"ALTER TABLE {tmp_new_table_name} RENAME TO {new_table_name};")

        return True


class MEMLite(DBLite):

    def _connect(self, file: str):
        ext = self.__get_ext(file)
        cnt = getattr(self, "_connect_" + ext, super()._connect)
        src = cnt(file)
        if file == MEMORY:
            return src
        con = sqlite3.connect(MEMORY)
        src.backup(con)
        src.close()
        return con

    def __get_ext(self, file: str) -> str:
        ext = file.rsplit(".", 1)[-1]
        ext = ext.lower()
        return {
            "accdb": "mdb",
            "xlsx": "xls",
        }.get(ext, ext)

    def _connect_mdb(self, file: str):
        def __get_schema():
            schema = Shell.get("mdb-schema", file, "sqlite")
            for line in schema.split("\n"):
                line = line.strip()
                if line.startswith("ALTER TABLE ") and "ADD CONSTRAINT" in line:
                    return Shell.get("mdb-schema", "--no-relations", file, "sqlite")
            return schema

        con = sqlite3.connect(MEMORY)

        schema = __get_schema()
        schema = re.sub(r"\bvarchar($|,)", r"TEXT\1", schema, flags=re.MULTILINE)
        con.executescript(schema)
        con.commit()
        for table in Shell.get("mdb-tables", "-1", file).split("\n"):
            if len(table.strip()) == 0:
                continue
            output = Shell.get("mdb-export", "-I", "sqlite", "-D", "%Y-%m-%d %H:%M", file, table)
            output = output.strip()
            if len(output) > 0:
                con.executescript(output)
                con.commit()

        return con

    def _connect_xls(self, file: str):
        def __normalize_col(c: str):
            return normalize_name(c, prefix='c')

        def __read_data():
            skiprows = 0
            while True:
                data = pd.read_excel(file, skiprows=skiprows)
                for c in data.columns:
                    if not re.match(r"^Unnamed: \d+$", c):
                        data.columns = tuple(map(__normalize_col, data.columns))
                        return data
                skiprows = skiprows + 1

        name = basename(file).rsplit(".", 1)[0]
        name = normalize_name(name, prefix="t")
        data = __read_data()

        con = sqlite3.connect(MEMORY)

        data.to_sql(name=name, index=False, con=con)

        return con

    def _connect_csv(self, file: str):
        def __normalize_col(c: str):
            return normalize_name(c, prefix='c')

        def __read_data():
            skiprows = 0
            while True:
                data = pd.read_csv(file, skiprows=skiprows)
                for c in data.columns:
                    if not re.match(r"^Unnamed: \d+$", c):
                        data.columns = tuple(map(__normalize_col, data.columns))
                        return data
                skiprows = skiprows + 1

        name = basename(file).rsplit(".", 1)[0]
        name = normalize_name(name, prefix="t")
        data = __read_data()

        con = sqlite3.connect(MEMORY)

        data.to_sql(name=name, index=False, con=con)

        return con

    def _connect_zip(self, file: str):
        con = sqlite3.connect(MEMORY)
        for f in iter_zip(file):
            with MEMLite(f) as db:
                db.backup(con)
        return con

    def _connect_sql(self, file: str):
        con = sqlite3.connect(MEMORY)
        with open(file, "r") as f:
            con.executescript(f.read())
        return con
