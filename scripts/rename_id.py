#!/usr/bin/env python
#coding:utf-8

import os
import re
import sys
import gzip
import logging
import argparse

LOG = logging.getLogger(__name__)

__version__ = "1.2.0"
__author__ = ("Xingguo Zhang",)
__email__ = "invicoun@foxmail.com"
__all__ = []


def read_fasta(file):

    '''Read fasta file'''
    if file.endswith(".gz"):
        fp = gzip.open(file)
    else:
        fp = open(file)

    seq = []
    for line in fp:
        if isinstance(line, bytes):
            line = line.decode('utf-8')
        line = line.strip()

        if not line:
            continue
        if line.startswith(">"):
            line = line.strip(">")
            if len(seq) == 2:
                yield seq
            seq = []
            seq.append(line.split()[0])
            continue
        if len(seq) == 2:
            seq[1] += line
        else:
            seq.append(line)

    if len(seq) == 2:
        yield seq
    fp.close()


def raname_id(file, prefix):

    n = 0
    for seqid, seq in read_fasta(file):
        n += 1
        nseqid = '%s%s' % (prefix, n)
        LOG.info("%s\t%s" % (seqid, nseqid))
        print('>%s\n%s' % (nseqid, seq))


def add_args(parser):

    parser.add_argument('fasta', help='')
    parser.add_argument('-p', '--prefix', metavar='STR', type=str, default='OTU_',
        help='Input the prefix of the sequence id(default=OTU_).')

    return parser


def main():

    logging.basicConfig(
        stream=sys.stderr,
        level=logging.INFO,
        format="[%(levelname)s] %(message)s"
    )

    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="""
name:
    raname_id.py: Rename the sequence
attention:
    raname_id.py old.fasta >new.fasta
    raname_id.py old.fasta -p OTU_ >new.fasta

version: %s
contact:  %s <%s>\
    """ % (__version__, " ".join(__author__), __email__))

    parser = add_args(parser)
    args = parser.parse_args()

    raname_id(args.fasta, args.prefix)


if __name__ == "__main__":
    main()
