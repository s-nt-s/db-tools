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
                if is_to_anon(table, col):
                    vls = db.to_tuple(f'select distinct "{col}" from "{table}" where "{col}" is not null order by "{col}"')
                    if len(vls):
                        if isinstance(vls[0], str):
                            str_values = str_values.union(vls)
                        else:
                            num_values = num_values.union(vls)
                    to_anon[table].append(col)

        if len(to_anon) == 0:
            sys.exit("No hay nada que anonimizar")

    SQL = []
    for table, cols in to_anon.items():
        cls = ", ".join(map(lambda c: f'"{c}"=to_anon("{c}")', cols))
        SQL.append(f'UPDATE "{table}" SET {cls};')

    sql = "\n".join(SQL)
    print(sql)


    str_values = tuple(sorted(str_values))
    num_values = tuple(sorted(num_values))

    @cache
    def mk_anon(v: Union[str, float, None]):
        if v is None:
            return None
        if isinstance(v, str):
            return hex(str_values.index(v))[2:]
        return num_values.index(v)


    with DBLite(out) as db:
        db.register_function("to_anon", 1, mk_anon)
        with DBLite(pargs.db, readonly=True) as s:
            s.backup(db)
        db.executescript(f"PRAGMA foreign_keys = false;\n{sql}\nPRAGMA foreign_keys = true;")