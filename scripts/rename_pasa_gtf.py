#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
import sys
import gzip
import logging
import argparse

from collections import OrderedDict

LOG = logging.getLogger(__name__)

__version__ = "1.0.0"
__author__ = ("invicoun@foxmail.com",)
__email__ = "113178210@qq.com"
__all__ = []


def read_tsv(file, sep=None):

    LOG.info("reading message from %r" % file)

    if file.endswith(".gz"):
        fp = gzip.open(file)
    else:
        fp = open(file)

    for line in fp:
        if isinstance(line, bytes):
            line = line.decode('utf-8')
        line = line.strip()

        if not line or line.startswith("#"):
            continue

        yield line.split(sep)

    fp.close()


def split_attr(attributes):

    r =  OrderedDict()
    for content in attributes.split(";"):
        content = content.strip()
        if not content:
            continue

        if ' "' not in content:
            print("%r is not a good formated attribute: no tag!")
            continue
        tag, value = content.split(' "', 1)
        r[tag] = value.strip('"')

    return r


def attr2str(attr_dict):

    attributes = ""
    for i in attr_dict:
        attributes += '%s "%s"; ' % (i, attr_dict[i])

    return attributes.strip()


def rename_pasa_gtf(file):

    r = {}

    for line in read_tsv(file, "\t"):
        attr = split_attr(line[-1])
        geneid = attr["gene_id"]
        if line[2] == "transcript":
            if geneid not in r:
                r[geneid] = [1]
            else:
                r[geneid].append(r[geneid][-1]+1)
            attr["transcript_id"] = "%s.%s" % (geneid, r[geneid][-1])
            line[-1] = attr2str(attr)
        else:
            attr["transcript_id"] = "%s.%s" % (geneid, r[geneid][-1])
            line[-1] = attr2str(attr)
        print("\t".join(line))

    return 0


def add_help_args(parser):

    parser.add_argument('input', metavar='FILE', type=str,
        help='Input file.')

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
    rename_pasa_gtf.py Rename transcript

attention:
    rename_pasa_gtf.py pasa_assemblies.gtf >pasa_assemblies.rename.gtf
version: %s
contact:  %s <%s>\
        ''' % (__version__, ' '.join(__author__), __email__))
    args = add_help_args(parser).parse_args()

    rename_pasa_gtf(args.input)


if __name__ == "__main__":
    main()
