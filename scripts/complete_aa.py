#!/usr/bin/env python
#coding: utf-8
import sys
import os
def generate_fasta(fasta):
    '''
        para: fasta
        return: generator a tuple (header,seq)
             header contain id and description.
             seq is oneline NO '\n'
    '''
    fh = open(fasta,'r')
    seq = ''
    header = ''
    for line in fh:
        line = line.strip()
        if not line:
            continue
        if line[0] == '>':
            if seq :
                yield (header,seq)
                seq = ''
            header = line[1:]
        else:
            seq = seq + line
    if seq:
        yield (header,seq)
def complete_aa(fastafile):
    
    for seqid, seq in generate_fasta(fastafile):
        if seq[0] == "M" and seq[-1] == '*' and '*' not in seq[1:-1]:
            print(">" + seqid)
            print(seq)

if __name__ == "__main__":
    complete_aa(sys.argv[1])
