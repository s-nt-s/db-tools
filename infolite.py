#!/usr/bin/env python3

from os.path import isfile, basename
from core.dblite import DBLite
import sys
import argparse
from textwrap import dedent
from typing import Dict, Union


class InfoDBLite(DBLite):
    def __init__(self, *args, **kwargs):
        kwargs['readonly'] = True
        super().__init__(*args, **kwargs)
        self.register_function("isdigit", 1, lambda s: s.isdigit())

    def describe(self, table: str, col: str) -> Dict[str, Union[str, int]]:
        r = dict(
            table=table,
            col=col,
            type=self.one(f'select typeof("{col}") FROM "{table}" where "{col}" is not null and "{col}"!=\'\' LIMIT 1'),
            min=self.one(f'select min("{col}") from "{table}"'),
            max=self.one(f'select max("{col}") from "{table}"'),
            vals=self.one(f'select count(distinct "{col}") from "{table}"'),
            nulls=self.one(f'select count(*) from "{table}" where "{col}" is Null or "{col}"=\'\'')
        )
        if r['vals'] > 0:
            if r['type'] == 'real':
                if self.one('select count(*) from "{table}" where "{col}" is not null and "{col}"!=round("{col}")') == 0:
                    r['type'] = 'int!'
            elif r['type'] == 'integer':
                r['type'] = 'int'
            elif r['type'] == 'text':
                if self.one('select count(*) from "{table}" where not("{col}" is null or "{col}"=\'\') and not isdigit("{col}")') == 0:
                    r['type'] = 'int?'
        for k, v in list(r.items()):
            if isinstance(v, float):
                r[k] = int(v)
        return r


if __name__ == "__main__":
    parser = argparse.ArgumentParser("Describe una base de datos .sqlite")
    parser.add_argument('sqlite', nargs='+', help='Base de datos sqlite')
    pargs = parser.parse_args()

    for file in pargs.sqlite:
        if not isfile(file):
            sys.exit("No existe el fichero %s" % file)

    print(dedent('''
        * `Tipo = int!`: el tipo de columna es `real` pero todos los valores son enteros
        * `Tipo = int?`: el tipo de columna es `text` pero todos los valores son códigos numericos
        * `Vals`: número de valores diferentes no nulos ni vacios que contiene el campo
        * `Nulos`: número de filas con valor null o vacio en esa columna
    '''))
    line_fmt = "| {col:<14} | {type:<4} | {min:>9} | {max:>9} | {vals:>6} | {nulls:>5} |"

    for file in pargs.sqlite:
        print("#", basename(file))
        with InfoDBLite(file) as db:
            for table, cols in sorted(db.tables.items()):
                print("\n##", table, "({} filas)".format(db.one(f'select count(*) from "{table}"')), end="\n\n")
                print(line_fmt.format(col="Columna", type="Tipo", min="MIN", max="MAX", vals="Vals", nulls="Nulos"))
                print(
                    line_fmt.format(col=":", type=":", min=":", max=":", vals=":", nulls=":")
                    .replace("| :", "|: ")
                    .replace(": |", " :|")
                    .replace(" ", "-")
                )
                for col in cols:
                    c = db.describe(table, col)
                    print(line_fmt.format(**{k: str(v) for k, v in c.items()}))
        print("\n")
