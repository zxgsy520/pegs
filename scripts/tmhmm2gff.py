#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
import sys
import gzip
import logging
import argparse

LOG = logging.getLogger(__name__)

__version__ = "1.0.1"
__author__ = ("Xingguo Zhang",)
__email__ = "invicoun@foxmail.com"
__all__ = []


def read_tsv(file, sep=None):

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


def get_geneid(file):

    file = file.split("/")[-1]

    if "." in file:
        prefix = file.split(".summary")[0]
    else:
        prefix = file

    return prefix


def tmhmm2gff(files):

    print("#sequence-name\tsource\tfeature\tstart\tend\tscore\tPredHel\tExpAA\tY/N")
    for file in files:
        geneid = get_geneid(file)
        for line in read_tsv(file):
            start = int(line[0])
            end = int(line[1])
            feature = line[2]
            yn = "NO"
            if "transmembran" in feature:
                yn = "YES"
                feature = "transmembrane_helix"
            temp = [geneid, "TMHMM", feature, str(start+1), str(end+1), ".", ".", ".", yn]
            print("\t".join(temp))

    return 0


def add_tmhmm2gff_help(parser):

    parser.add_argument("input", nargs="+", metavar="FILE", type=str,
        help="Input tmhmm prediction result.")

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
    tmhmm2gff.py --Convert data format.

attention:
    tmhmm2gff.py  *.summary >tmhmm.gff
version: %s
contact:  %s <%s>\
        ''' % (__version__, ' '.join(__author__), __email__))

    args = add_tmhmm2gff_help(parser).parse_args()

    tmhmm2gff(args.input)


if __name__ == "__main__":

    main()
