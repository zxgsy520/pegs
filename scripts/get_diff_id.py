#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
import os
import sys
import gzip
import logging
import argparse

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


def read_id(file):

    r = []

    for line in read_tsv(file, sep="\t"):
        r.append(line[0])

    return r


def get_diff_id(file1, file2):

    r1 = read_id(file1)
    r2 = read_id(file2)
    
    for i in r1:
        if i in r2:
            continue
        print(i) #输出r1中r2不存在的id

    return 0


def add_hlep_args(parser):

    parser.add_argument("input", metavar="FILE", type=str,
        help="Input the sequence ID file to be removed.")
    parser.add_argument("-k", "--keep", metavar="STR", type=str, required=True,
        help="Input the sequence ID file you want to keep.")

    return parser


def main():

    logging.basicConfig(
        stream=sys.stderr,
        level=logging.INFO,
        format="[%(levelname)s] %(message)s"
    )
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
        description='''
name:
    get_diff_id.py: Get the difference sequence ID to be deleted.

attention:
    get_diff_id.py Wrong.ids -k transcript.id >rm.id
''')
    args = add_hlep_args(parser).parse_args()

    get_diff_id(args.input, args.keep)


if __name__ == "__main__":

    main()
