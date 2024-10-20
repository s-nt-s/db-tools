#!/usr/bin/env python3

import argparse
from os.path import isfile, abspath, dirname
import sys
import logging
from core.schemaspy import SchemasPy


if __name__ == "__main__":
    parser = argparse.ArgumentParser("Obtener diagrama de una base de una base de datos")
    parser.add_argument('--verbose', '-v', action='count', help="Nivel de depuración", default=0)
    parser.add_argument('--out', help="Diagrama de salida", required=True)
    parser.add_argument('--size', default="large", help="Tamaño de la imagen")
    parser.add_argument('-i', help="Expresión regular que deben cumplir las tablas a incluir")
    parser.add_argument('-I', help="Expresión regular que deben cumplir las tablas a excluir")
    parser.add_argument('-rows', action='store_true', help="Mostrar el número de registros")
    parser.add_argument('db', help='.properties oase de datos sqlite o .sql que genere un bd sqlite')
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

    s = SchemasPy(
        home=dirname(abspath(__file__))+"/schemaspy"
    )
    s.save_diagram(
        pargs.db,
        pargs.out,
        size=pargs.size,
        include=pargs.i,
        exclude=pargs.I,
        rows=pargs.rows
    )
