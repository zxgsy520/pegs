#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
import sys
import logging
import argparse
import matplotlib
matplotlib.use("Agg")

from matplotlib import pyplot as plt

LOG = logging.getLogger(__name__)

__version__ = "1.1.1"
__author__ = ("Xingguo Zhang",)
__email__ = "invicoun@foxmail.com"
__all__ = []


def read_busco_txt(files):

    busco_dict = {}
    n = 0

    for line in open(files, "r"):
        line = line.strip()

        if "dataset" in line:
            data = line.split("_odb", 1)[0].strip().split()[-1]
            continue
        if line.startswith("Assembly"):
            break

        if not line or line.startswith("#"):
            continue

        line = line.split("\t")

        if len(line)>=2:
            line = line[0:2]
            n += 1
            if "Total BUSCO" in line[1]:
                line[1] = data[0].upper() + data[1:]
                busco_dict[n] = line
            else:
                busco_dict[n] = line

    return  busco_dict


def plot_busco(busco_dict, out_name):

    color_list = ["#45AAB4", "#FE7A66", "#FBB45C", "#038DB2", "#206491", "#F9637C"]
    total = int(busco_dict[6][0])
    for i in busco_dict:
        busco_dict[i][0] = int(busco_dict[i][0])*100.0/total

    fig = plt.figure(figsize=[8, 2])
    ax=fig.add_subplot(1,1,1)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.grid(True, 'major', 'x', ls='--', lw=.5, c='black', alpha=.3)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(''.format)) 
    ax.yaxis.set_minor_formatter(plt.FuncFormatter(''.format)) 
    plt.subplots_adjust(left=0.02, right=0.5, top=0.95, bottom=0.25)

    ax.plot([0, busco_dict[4][0]+busco_dict[5][0]],[1.5,2.0],'k-',alpha=0.15)
    ax.plot([busco_dict[1][0], busco_dict[6][0]],[1.5,2.0],'k-',alpha=0.15)

    m = busco_dict[1][0]
    for i in [2,3]:
        label_name ='{} {}%'.format(busco_dict[i][1],round(busco_dict[i][0],2))
        ax.barh(1, m, height=1,label=label_name, color=color_list[i-1])
        m = m-int(busco_dict[i][0])

    n = busco_dict[6][0]
    for i in [1, 4, 5]:
        label_name = '{} {}%'.format(busco_dict[i][1],round(busco_dict[i][0],2))
        ax.barh(2.5, n, height=1, label=label_name, color=color_list[i-1])
        n = n-int(busco_dict[i][0])

    ax.yaxis.set_visible(False)
    plt.legend(loc='center right', bbox_to_anchor=(2.0, 0.64) ,frameon=False)
    plt.xlabel('Percent(%)' ,fontsize=12)
    plt.savefig("%s.busco.png" % out_name, dpi=700)
    plt.savefig("%s.busco.pdf" % out_name)


def out_busco_result(busco_dict, out_name):

    output = open(out_name+'.busco.tsv', 'w')

    output.write('#Type\tNumber\tPercent(%)\n')

    for line in busco_dict.values():
        bvalue = float(line[0])*100/float(busco_dict[6][0])
        output.write("{}\t{:,}\t{:.2f}\n".format(line[1], int(line[0]), bvalue))

    output.close()


def stat_busco_hlep(parser):

    parser.add_argument("-i", "--input", metavar="FILE", type=str, required=True,
        help="Input the BUSCO evaluation result.")
    parser.add_argument("-o", "--out", metavar="STR", type=str, default="out",
        help="Output file prefix,default=out.")

    return parser


def main():

    logging.basicConfig(
        stream=sys.stderr,
        level=logging.INFO,
        format="[%(levelname)s] %(message)s"
    )
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
        description="""
name:
    stat_busco.py  Statistics and plotting BUSCO results.

attention:
    stat_busco.py -i short_summary.BUSCO.txt
""")
    args = stat_busco_hlep(parser).parse_args()
    busco_dict = read_busco_txt(args.input)

    out_busco_result(busco_dict, args.out)
    plot_busco(busco_dict, args.out)


if __name__ == "__main__":

    main()
