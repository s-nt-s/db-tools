#!/usr/bin/env python3

import os
from os.path import isfile
import sys
import argparse
from typing import Dict
from textwrap import dedent
from core.mklite import MEMLite

import logging
from core.source import Source

HOME = os.environ.get('HOME')
MDB = ("mdb", "accdb")
XLS = ("xls", "xlsx")


def rel_home(path: str):
    if path == HOME:
        return "~"
    if path.startswith(HOME + "/"):
        return "~" + path[len(HOME):]
    return path


class SourceLite(MEMLite):
    def __init__(self, src: Source):
        super().__init__(src.file)
        self.src = src
        self.exclude = ()
        self.selected_tables = ()

        all_tables = list(self.tables)
        if self.src.exclude:
            self.exclude = tuple(t for t in all_tables if t in self.src.exclude)
        if self.src.include:
            self.exclude = tuple(t for t in all_tables if t not in self.src.include)

        for t in self.exclude:
            self.execute(f'DROP TABLE IF EXISTS "{t}";')
        self.commit()

        self.selected_tables = list(self.tables)

        if len(self.src.rename) > 0:
            for old, new in zip(self.tables, self.src.rename):
                self.execute(f'ALTER TABLE "{old}" RENAME TO "{new}";')

        if len(self.src.prefix+self.src.sufix) > 0:
            for t in self.tables:
                self.execute(f'ALTER TABLE "{t}" RENAME TO "{self.src.prefix}{t}{self.src.sufix}";')

    def print_resume(self):
        print("*", rel_home(self.src.name))
        for t in sorted(self.selected_tables+self.exclude):
            s = t.replace("~", "\\~")
            if t in self.exclude:
                print(f"    * ~~{s}~~")
            else:
                print(f"    * {s}")


if __name__ == "__main__":
    EXT = MDB + XLS
    parser = argparse.ArgumentParser(
        "Convierte una base de datos Access ({}) a SQLite".format("|".join(EXT)),
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument('--sql', action='store_true', help="Guardar script sql")
    parser.add_argument('--normalize', action='store_true', help='Renombrar tablas y columnas para normalizarlas')
    parser.add_argument('--out', help="Fichero de salida")
    parser.add_argument('--verbose', '-v', action='count', help="Nivel de depuración", default=0)

    parser.add_argument('files', nargs='+',
        help=dedent(
        '''
            Base de datos Access o excel ({}), o
            precedido por !, tablas a excluir, o
            precedido por !!, excluir todo menos las tabla indicada
            acabando en _, un prefijo para las tablas
            empezando por  _, un sufijo para las tablas

            El operador + y ! se aplica a la base de datos más proxima.
            No se puede usar ambos operadores (! y +) en una misma base de datos
        '''
        ).format("|".join(EXT)).strip()
    )
    pargs = parser.parse_args()

    levels = [logging.INFO, logging.DEBUG]
    level = min(len(levels) - 1, pargs.verbose)

    logging.basicConfig(
        level=levels[level],
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%M-%d %H:%M:%S'
    )

    def parse_file(f: str):
        def __validate(s: str):
            if len(s) == 0:
                sys.exit(f"no se puede usar un modificador ({f}) sin un nombre")
            return s

        if isfile(f):
            return True, f
        if f.startswith("!!"):
            return "!!", __validate(f[2:])
        if f.startswith("!"):
            return "!", __validate(f[1:])
        if f.endswith("_"):
            return "^", __validate(f)
        if f.startswith("_"):
            return "$", __validate(f)
        if f.startswith("="):
            return "=", __validate(f[1:])
        sys.exit(file + " no existe")

    files: Dict[str, Source] = {}
    for file in pargs.files:
        if file in files:
            continue
        flag, word = parse_file(file)
        if flag is True:
            ext = file.split(".")[-1].lower()
            if ext not in EXT:
                sys.exit(file + " no termina en .mdb o .accdb")
            files[file] = Source(file=file)
            continue
        if len(files) == 0:
            sys.exit(f"no se puede usar un modificador ({file}) antes incluir una base de datos")
        lastk = list(files.keys())[-1]
        lastv = files[lastk]
        if flag == "!":
            files[lastk] = lastv.merge(exclude=tuple(sorted(set(lastv.exclude).union((word,)))))
        if flag == "!!":
            files[lastk] = lastv.merge(include=tuple(sorted(set(lastv.include).union((word,)))))
        if flag == "=":
            files[lastk] = lastv.merge(rename=(lastv.rename+(word, )))
        if flag == "^":
            files[lastk] = lastv.merge(prefix=word)
        if flag == "$":
            files[lastk] = lastv.merge(sufix=word)

    sources = tuple(files.values())

    if pargs.out is None:
        pargs.out = sources[0].file + ".sqlite"
    if not pargs.out.endswith(".sqlite"):
        sys.exit(pargs.out + " no termina en .sqlite")
    if isfile(pargs.out):
        sys.exit(pargs.out + " ya existe")

    with SourceLite(pargs.out) as db:
        with SourceLite(sources[0]) as s:
            s.backup(db)
            s.print_resume()
        for src in sources[1:]:
            with SourceLite(src) as s:
                db.executescript("\n".join(s.iter_sql_backup()))
                s.print_resume()
        if pargs.normalize:
            db.normalize()
        if pargs.sql:
            with open(pargs.out+".sql", "w") as f:
                for ln in db.iter_sql_backup():
                    f.write(ln+"\n")
