#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
import sys
import gzip
import logging
import argparse

import numpy as np

LOG = logging.getLogger(__name__)
__version__ = "v1.0.0"
__author__ = ("Xingguo Zhang",)
__email__ = "invicoun@foxmail.com"
__all__ = []


def read_tsv(file, sep="\t"):

    if file.endswith(".gz"):
        fh = gzip.open(file)
    else:
        fh = open(file)

    for line in fh:
        if isinstance(line, bytes):
            line = line.decode("utf-8")
        line = line.strip()

        if not line or line.startswith("#"):
            continue

        yield line.split(sep)

    fh.close()


def get_expression_gene(file, mintpm=0.005):

    n = 0
    for line in read_tsv(file, "\t"):
        n += 1
        if n == 1:
            continue
        abunds = np.array(line[1::]).astype(float)
        if max(abunds) < mintpm:
            continue
        print(line[0])

    return 0


def add_hlep_args(parser):

    parser.add_argument("input", metavar="FILE", type=str,
        help="Input abundance file, gene_counts.tsv")
    parser.add_argument("--mintpm", metavar="FLOAT", type=float, default=0.005,
        help="Minimum expression level for filtering, default=0.005")

    return parser


def main():

    logging.basicConfig(
        stream=sys.stderr,
        level=logging.INFO,
        format="[%(levelname)s] %(message)s"
    )
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
    description='''
get_expression_gene.py:Get the expressed gene ID

For exmple:
    get_expression_gene.py gene_counts.tsv --mintpm 0.005 >expression_gene.id
version: %s
contact:  %s <%s>\
    ''' % (__version__, " ".join(__author__), __email__))

    args = add_hlep_args(parser).parse_args()
    get_expression_gene(args.input, args.mintpm)



if __name__ == "__main__":

    main()
