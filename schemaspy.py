#!/usr/bin/env python3

import argparse
from os.path import isfile
import sys
import logging
from core.schemaspy import SchemasPy


if __name__ == "__main__":
    parser = argparse.ArgumentParser("Obtener diagrama de una base de una base de datos")
    parser.add_argument('--verbose', '-v', action='count', help="Nivel de depuración", default=0)
    parser.add_argument('--out', help="Diagrama de salida", required=True)
    parser.add_argument('--size', default="large", help="Tamaño de la imagen")
    parser.add_argument('db', help='Base de datos sqlite o .properties')
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

    s = SchemasPy()
    s.save_diagram(
        pargs.db,
        pargs.out,
        size=pargs.size
    )
