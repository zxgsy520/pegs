#!/usr/bin/env python 
#coding: utf-8
import sys
import re
import argparse
from collections import defaultdict

def read_gtf(gtffile):
    '''
    read gtf to dict, and 
    return gtf_dict trans_ids, scores dict;
    '''
    trans_ids = []
    scores    = dict()
    gtf_dict = defaultdict(list)
    with open(gtffile,'r') as g:
        for line in g:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            chrid, PB, ftype, start, end, score, strand, phase, attr = line.split('\t')
            mat=re.search(r'transcript_id \"([^";]+)\"',attr)
            trans_id = mat.group(1)
            if trans_id not in gtf_dict:
                trans_ids.append(trans_id)
            if score == '.':
                score = 0

            scores[trans_id] = float(score)
            gtf_dict[trans_id].append(line)

    return gtf_dict, trans_ids, scores

def read_list(listfile):
    trans_ids_for_include = []
    with open(listfile, 'r') as f :
        for line in f:
            line = line.strip()
            trans_ids_for_include.append(line)

    return trans_ids_for_include
    
def filter_by_list(gtf_dict,trans_ids, listfile):
    filtered_gtf_dict = defaultdict(list)
    filtered_trans_ids = []
    alist = read_list(listfile)
    for trans_id in trans_ids:
        if trans_id in alist and trans_id in gtf_dict:
                filtered_gtf_dict[trans_id] = gtf_dict[trans_id]
                if trans_id not in filtered_trans_ids:
                    filtered_trans_ids.append(trans_id)

    return filtered_gtf_dict,filtered_trans_ids

def filter_by_score(score_dict, trans_ids , topN = 2000):
    trans_ids_topN = []
    filtered_score_dict  = dict()
    for trans_id in trans_ids:
        if trans_id in score_dict:
            filtered_score_dict[trans_id] = score_dict[trans_id]
    
    sorted_score_dict=sorted(filtered_score_dict.items(),key = lambda d: abs(d[1]),reverse=True)
    topN_sorted_score_dict = dict(sorted_score_dict[0:topN])
    
    for trans_id in trans_ids:
        if trans_id in topN_sorted_score_dict and trans_id not in trans_ids_topN:
            trans_ids_topN.append(trans_id)
    
    return trans_ids_topN

def print_gtf(trans_ids, gtf_dict):
    for trans_id in trans_ids:
        if trans_id in gtf_dict:
            print("\n".join(gtf_dict[trans_id]))

def main():
    parser = argparse.ArgumentParser(description="filter gtf by list and score for gmst prediction")
    parser.add_argument('--gtf',required=True,help='gtf file')
    parser.add_argument('--idlist',required=True,help='gtf file')
    parser.add_argument('--topN',type = int,default=3000, help='topN score, default 3000')
    args=parser.parse_args()
    gtf_dict,trans_ids, score_dict = read_gtf(args.gtf)
    filtered_gtf_dict, filtered_trans_ids = filter_by_list(gtf_dict,trans_ids, args.idlist)
    trans_ids_topN = filter_by_score(score_dict, filtered_trans_ids , topN = 3000)

    print_gtf(trans_ids_topN, filtered_gtf_dict)

if __name__ == '__main__':
    main()
