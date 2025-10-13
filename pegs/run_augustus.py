#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import json
import logging
import argparse

from collections import OrderedDict

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "../"))
from dagflow import DAG, Task, ParallelTask, do_dag
from pegs.config import *
from pegs.common import check_path, check_paths, mkdir, read_fasta, get_version

LOG = logging.getLogger(__name__)

__author__ = ("Xingguo Zhang",)
__email__ = "invicoun@foxmail.com"
__version__ = "v1.0.0"


def create_aug_task(genome, introns_gff, prefix, species="",
                    job_type="local", work_dir="", out_dir="", thread=10):

    temp = ""
    temp1 = "autoAugPred_abinitio"
    if introns_gff:
        temp = "--hints=%s" % introns_gff
        temp1 = "autoAugPred_hints"

    task = Task(
        id="aug_predict",
        work_dir=work_dir,
        type="local",
        option="-pe smp 1 %s" % QUEUE,
        script="""
export PATH={augustus_bin}:$PATH
export PATH={braker2_bin}:$PATH
export AUGUSTUS_CONFIG_PATH={augustus_config}
autoAugPred.pl --continue --genome={genome} --species={species} --extrinsiccfg={extrinsic} {temp} --remote=all.q --cpus={thread}
cd {temp1}/shells
python {root}/pegs/daglist.py aug* --prefix Aug --work_dir ./ --concurrent 20 --refresh 20 --job_type {job_type} --env_path {augustus_bin}
cd -
autoAugPred.pl --useexisting --continue  --genome={genome} --species={species} --extrinsiccfg={extrinsic} {temp} --remote=all.q --cpus=10
export PATH={pasa_bin}:$PATH
perl {evm_bin}/EvmUtils/misc/augustus_GTF_to_EVM_GFF3.pl {temp1}/predictions/augustus.gtf > {prefix}.augustus.gff3
cp {prefix}.augustus.gff3 {out_dir}
#rm -rf autoAugPred_hints
""".format(root=ROOT,
           augustus_bin=AUGUSTUS_BIN,
           braker2_bin=BRAKER2_BIN,
           augustus_config=AUGUSTUS_CONFIG_PATH,
           pasa_bin=PASA_BIN,
           evm_bin=EVM_BIN,
           extrinsic=AUGUSTUS_EXT,
           genome=genome,
           species=species,
           temp=temp,
           temp1=temp1,
           prefix=prefix,
           thread=thread,
           job_type=job_type,
           out_dir=out_dir)
    )

    return task, os.path.join(out_dir, "%s.augustus.gff3" % prefix)



def run_augustus(genome, introns_gff, prefix, species, job_type,
                 thread=10, concurrent=10, refresh=30, work_dir="", out_dir=""):

    genome = check_path(genome)
    work_dir = mkdir(work_dir)
    out_dir = mkdir(out_dir)

    if introns_gff:
        introns_gff = check_path(introns_gff)
    aug_model = os.path.abspath("%s/species/%s" % (AUGUSTUS_CONFIG_PATH, species)) #augustus模型的路径
    if os.path.isdir(aug_model): #模型存在 
        dag = DAG("run_augustus")
        task, gff = create_aug_task(
            genome=genome,
            introns_gff=introns_gff,
            prefix=prefix,
            species=species,
            job_type=job_type,
            work_dir=work_dir,
            out_dir=out_dir,
            thread=thread
        ) 
        dag.add_task(task)
        do_dag(dag, concurrent_tasks=concurrent, refresh_time=refresh)
    else:
        LOG.info("Augustus model %s does not exist, please train the model first" % species)
        gff = os.path.join(out_dir, "%s.augustus.gff3" % prefix)
        fo = open(gff, "w")
        fo.write("##gff-version 3")
        fo.close()

    return gff


def add_hlep_args(parser):

    parser.add_argument("genome", metavar="STR", type=str,
        help="Input genome file(fasta).")
    parser.add_argument("-igff", "--introns_gff", metavar="FILE", type=str, default=False,
        help="Input gff file for introns(introns.f.gff)")
    parser.add_argument("-p", "--prefix", metavar="STR", type=str, default="ZXG",
        help="Input result file prefix(ZXG)")
    parser.add_argument("-s", "--species", metavar="STR", type=str,  default=False,
        help="Input the species name of the sample.")
    parser.add_argument("-t", "--thread", metavar="INT", type=int, default=4,
        help="Set the number of threads(default: 4)")
    parser.add_argument("--concurrent", metavar="INT", type=int, default=10,
        help="Maximum number of jobs concurrent  (default: 10)")
    parser.add_argument("--refresh", metavar="INT", type=int, default=30,
        help="Refresh time of log in seconds  (default: 30)")
    parser.add_argument("--job_type", choices=["sge", "local"], default="local",
        help="Jobs run on [sge, local]  (default: local)")
    parser.add_argument("--work_dir", metavar="DIR", type=str, default="work",
        help="Work directory (default: current directory)")
    parser.add_argument("--out_dir", metavar="DIR", type=str, default="./",
        help="Output directory (default: current directory)")

    return parser


def main():

    logging.basicConfig(
        stream=sys.stderr,
        level=logging.INFO,
        format="[%(levelname)s] %(message)s"
    )
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
    description='''
URL: https://github.com/zxgsy520/bypegs
name:
    run_augustus.py:Using augustus to predict genes

attention:
    run_augustus.py genome.fasta --species Prototheca_zopfii --prefix name 
    run_augustus.py genome.fasta --species Prototheca_zopfii --prefix name --introns_gff introns.f.gff
version: %s
contact:  %s <%s>\
        ''' % (__version__, ' '.join(__author__), __email__))

    args = add_hlep_args(parser).parse_args()

    run_augustus(genome=args.genome, introns_gff=args.introns_gff,
        prefix=args.prefix, species=args.species, thread=args.thread,
        job_type=args.job_type, work_dir=args.work_dir, out_dir=args.out_dir,
        concurrent=args.concurrent, refresh=args.refresh)


if __name__ == "__main__":

    main()
