import argparse
from pathlib import Path
from os.path import realpath, isfile
from core.filemanager import FileManager
import pandas as pd
import sqlite3
from typing import List
from core.dblite import DBLite, dict_factory
import logging
import sys
from typing import Dict, Any


def to_integer_if_possible(series: pd.Series):
    try:
        result = series.astype(int)
        if (result == series).all():
            return result
        else:
            return series
    except (ValueError, TypeError):
        return series


def read_sql(sql: str, db: DBLite, size=100):
    try:
        return pd.read_sql(sql, db._con)
    except Exception:
        pass
    dfs: List[pd.DataFrame] = []
    r: Dict[str, Any]
    for r in db.select(sql, row_factory=dict_factory):
        aux = pd.DataFrame(r.values(), columns=r.keys())
        dfs.append(aux)
    df = pd.concat(dfs, ignore_index=True)
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser("Ejecuta varias select en una base de datos y guarda su resultado en excels")
    parser.add_argument('--verbose', '-v', action='count', help="Nivel de depuración", default=0)
    parser.add_argument('--sql', help="Directorio donde buscar las sql", required=True)
    parser.add_argument('--ow', action="store_true", help="Sobrescribir ficheros")
    parser.add_argument('db', help='Base de datos sqlite')
    pargs = parser.parse_args()

    if not isfile(pargs.db):
        sys.exit(pargs.db + " no existe")

    levels = [logging.INFO, logging.DEBUG]
    level = min(len(levels) - 1, pargs.verbose)
    logging.basicConfig(
        level=levels[level],
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%M-%d %H:%M:%S'
    )

    FM = FileManager.get()
    with DBLite(pargs.db, readonly=True) as db:
        for path in sorted(map(str, Path(pargs.sql).rglob('*.sql'))):
            if path.split("/")[-1][0] == "_":
                continue
            print(path)
            name = path.rsplit(".", 1)[0]
            if not(pargs.ow) and isfile(name+".csv") and isfile(name+".xlsx"):
                continue
            sql: str = FM.load(path)
            sql = sql.strip().strip(";").split(";")
            if len(sql) > 1:
                db.executescript((";\n".join(sql[:-1])+";"))
            df = read_sql(sql[-1], db)
            df = df.apply(to_integer_if_possible)
            FM.dump(name+".csv", df, index=False)
            FM.dump(name+".xlsx", df, index=False, prettify=True)
