#!/usr/bin/env python
#coding: utf-8
import argparse
import re
from collections import OrderedDict
def read_gff(gfffile):
    gff = OrderedDict()
    ID  = None
    mRNA_dict = OrderedDict()
    with  open(gfffile, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            if line.startswith('#'):
                continue
            cols = line.split('\t')
            feature = cols[2]
            if feature == "gene":
                ID = re.search(r'ID=([^;]+)',line).group(1)
                gff[ID]    = line + "\n"
            elif feature == "mRNA":
                mID = re.search(r'ID=([^;]+)',line).group(1)
                gff[ID]    += line + "\n"
                mRNA_dict[mID]  = ID
            else:
                gff[ID] += line + "\n"
    return gff, mRNA_dict

def filter_gff(gff, idlist, exp=False):
    idlist = [ i.strip() for i in open(idlist, 'r') ]
    gffdict,mRNAdict = read_gff(gff)
    target_ids  = idlist
    if exp:
        mids = set(mRNAdict.keys())
        idlist = set(idlist)
        target_ids = mids -  idlist
    for mid in mRNAdict: 
        if mid in target_ids:
            print(gffdict[mRNAdict[mid]])

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="filter gff by list")
    parser.add_argument('--gff',required=True,help='gff file, gene/mRNA/CDS/exon, one mRNA one gene')
    parser.add_argument('--idlist',required=True,help='idlist file, this is mRNA id list')
    parser.add_argument('--exp',action='store_true', help='except the list')
    args=parser.parse_args()
    filter_gff(args.gff, args.idlist, exp=args.exp)
 
