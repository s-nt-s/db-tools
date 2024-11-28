import argparse
from os.path import isfile
import sys
from core.dblite import DBLite, MEMORY
import logging
from typing import Dict, List, Union, Tuple
from collections import defaultdict
import hashlib
from functools import cache
import sqlite3
from textwrap import dedent
from math import ceil, floor
sqlite3.enable_callback_tracebacks(True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser("Anonimizar una base de datos sqlite")
    parser.add_argument('--verbose', '-v', action='count', help="Nivel de depuración", default=0)
    parser.add_argument('--anon', nargs='*', help="TABLA.CAMPO a anonimizar (todos por defecto)")
    parser.add_argument('db', help='Base de datos sqlite')
    pargs = parser.parse_args()

    levels = [logging.INFO, logging.DEBUG]
    level = min(len(levels) - 1, pargs.verbose)
    logging.basicConfig(
        level=levels[level],
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%M-%d %H:%M:%S'
    )

    if not isfile(pargs.db):
        sys.exit(pargs.db + " no existe")
    out = pargs.db.rsplit(".",1)[0]+".anon.sqlite"
    if isfile(out):
        sys.exit(out + " ya existe")
    
    def is_to_anon(tabla:str, col: str):
        if not pargs.anon:
            return True
        if tabla+"." in pargs.anon:
            return True
        if "."+col in pargs.anon:
            return True
        if table+"."+col in pargs.anon:
            return True
        return False

    str_values = set()
    num_values = set()
    to_anon: Dict[str, List[str]] = defaultdict(list)
    with DBLite(pargs.db, readonly=True) as db:
        for table in db.tables:
            for col in db.get_cols(table):
                if not is_to_anon(table, col):
                    continue
                vls = db.to_tuple(f'select distinct "{col}" from "{table}" where "{col}" is not null')
                if len(vls)==0:
                    continue
                if isinstance(vls[0], str):
                    str_values = str_values.union(vls)
                else:
                    num_values = num_values.union(vls)
                to_anon[table].append(col)

        if len(to_anon) == 0:
            sys.exit("No hay nada que anonimizar")

    SQL = []
    for table, cols in to_anon.items():
        cls = ", ".join(map(lambda c: f'"{c}"=mk_anon("{c}")', cols))
        SQL.append(f'UPDATE "{table}" SET {cls};')
    SQL = "\n".join(SQL)

    str_values = tuple(sorted(str_values))
    num_len = len(num_values)
    int_values = set()
    for i in num_values:
        int_values.add(int(i))
        int_values.add(int(ceil(i)))
        int_values.add(int(floor(i)))

    num_values_anom = {}
    for i, n in enumerate(sorted(num_values)):
        if i not in int_values:
            int_values.add(i)
            num_values_anom[n] = i
            continue
        while num_len in int_values:
            num_len = num_len + 1
        num_values_anom[n] = num_len
        int_values.add(num_len)

    def mk_anon(v: Union[str, float, None]):
        if v is None:
            return None
        if isinstance(v, str):
            return hex(str_values.index(v))[2:]
        return num_values_anom[v]


    with DBLite(out) as db:
        db.register_function("mk_anon", 1, mk_anon)
        with DBLite(pargs.db, readonly=True) as s:
            s.backup(db)
        db.executescript(dedent(f'''
            PRAGMA foreign_keys = OFF;
            PRAGMA recursive_triggers = OFF;
            PRAGMA synchronous = OFF;
            PRAGMA journal_mode = OFF;
        '''))
        db.executescript(SQL)
        db.executescript(dedent(f'''
            PRAGMA foreign_keys = ON;
            PRAGMA recursive_triggers = ON;
            PRAGMA synchronous = FULL;
            PRAGMA journal_mode = DELETE; -- o WAL
        '''))