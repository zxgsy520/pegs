#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
import sys
import gzip
import logging
import argparse

LOG = logging.getLogger(__name__)

__version__ = "1.1.0"
__author__ = ("Xingguo Zhang",)
__email__ = "invicoun@foxmail.com"
__all__ = []


def read_tsv(file):

    for line in  open(file, "r"):

        line = line.strip()

        if not line or line.startswith("#"):
            continue

        yield line.split("\t")


def stat_tmhmm_signalp(signalp, tmhmm, output):

    genes = set()
    tmhmm_genes = set()

    for line in read_tsv(tmhmm):
        genes.add(line[0])
        if line[2] not in ["transmembrane_helix", "transmembrane"]:
            continue
        if line[-1] == "YES":
            tmhmm_genes.add(line[0]) #有跨膜结构域的基因

    signalp_yes = 0
    secreted = 0

    print("#seq id\tsignal peptide\ttransmembrane domain\tsecreted protein")
    for line in read_tsv(signalp):
        signalp_yes += 1
        if line[0] not in tmhmm_genes:
            print("{0}\t{1}\t{2}\t{3}".format(line[0], "YES", "NO", "YES"))
        else:
            secreted += 1
            print("{0}\t{1}\t{2}\t{3}".format(line[0], "YES", "YES", "NO"))

    output = open(output, "w")

    output.write('''#type\tnumber
protein with signal peptide\t{0:,}
protein without transmembrane domain\t{1:,}
secreted protein\t{2:,}\n'''.format(signalp_yes,
        len(genes)-len(tmhmm_genes), secreted)
    )

    output.close()


def add_stat_secreted_help(parser):

    parser.add_argument("signalp", metavar="FILE", type=str,
        help="Input secreted protein prediction results")
    parser.add_argument("-t", "--tmhmm", metavar="FILE", type=str, required=True,
        help="Input protein sequence file")
    parser.add_argument("-o", "--output" , metavar="FILE", type=str,
        default="stat_secreted_protein.xls",
        help="Input the prediction result of transmembrane protein")

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
    stat_tmhmm_signalp.py --Integration of signalp and tmhmm results

attention:
    stat_tmhmm_signalp.py signalp.out -t tmhmm.out
    stat_tmhmm_signalp.py signalp.out -t tmhmm.out -o stat_secreted_protein.xls >secreted_protein.tsv
''')

    args = add_stat_secreted_help(parser).parse_args()

    stat_tmhmm_signalp(args.signalp, args.tmhmm, args.output)


if __name__ == "__main__":

    main()
