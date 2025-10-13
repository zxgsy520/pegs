#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import json
import logging
import argparse

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "../"))
from dagflow import DAG, Task, ParallelTask, do_dag
from pegs.common import check_paths, mkdir, check_path, get_version, read_files

LOG = logging.getLogger(__name__)

__author__ = ("Xingguo Zhang",)
__email__ = "invicoun@foxmail.com"
__version__ = "1.0.0"
__all__ = []

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../")
BIN = os.path.join(ROOT, "bin")
SCRIPTS = os.path.join(ROOT, "scripts")

SIGNALP_BIN = "/Work/pipeline/software/meta/signalp/v5.0b/bin/"
TMHMM_BIN = "/Work/pipeline/software/meta/tmhmm/v1.1/bin/"
GNUPLOT_BIN = "/Work/pipeline/software/Base/gnuplot/v4.6.6/bin/"

def split_protein(protein, prefix, job_type, work_dir, size="100kb"):

    dag = DAG("split_protein")
    task = Task(
        id="split_protein",
        work_dir=work_dir,
        type=job_type,
        option="-pe smp 2",
        script="""
{bin}/biotool seqsplit {protein} --size {size} \\
  --prefix {prefix} --workdir {work_dir}
""".format(bin=BIN,
            protein=protein,
            prefix=prefix,
            size=size,
            work_dir=work_dir,
        )
    )
    dag.add_task(task)
    do_dag(dag, 8, 10)

    proteins = read_files(work_dir, "%s.part*.fa" % prefix)

    return proteins


def create_signalp_task(proteins, prefix, types="euk", thread=10,
                        job_type="sge", work_dir="", out_dir=""):

    prefixs = [os.path.basename(i) for i in proteins]
    id = "signalp"

    tasks = ParallelTask(
        id=id,
        work_dir="%s/{id}" % work_dir,
        type=job_type,
        option="-pe smp %s" % thread,
        script="""
export PATH={tmhmm}:{signalp}:{gnuplot}:$PATH
tmhmm -f {{proteins}} >tmhmm.log
rm -rf *.pdf *.annotation 
python {script}/tmhmm2gff.py *.summary >{{prefixs}}.tmhmm.gff
rm -rf *.summary
signalp -fasta {{proteins}} -org {types} -gff3 \\
  -prefix {{prefixs}} -plot png -mature
""".format(tmhmm=TMHMM_BIN,
           signalp=SIGNALP_BIN,
           gnuplot=GNUPLOT_BIN,
           script=SCRIPTS,
           types=types,
           thread=thread),
        proteins=proteins,
        prefixs=prefixs,
    )

    join_task = Task(
        id="merge_signalp",
        work_dir=work_dir,
        type=job_type,
        option="-pe smp 1",
        script="""
#经典分泌蛋白：满足有信号肽但无跨膜结构域的蛋白
cat {id}*/*.tmhmm.gff|grep -v "#seq" > {prefix}.tmhmm.gff
cat {id}*/*.gff3 |grep -v "##gff" >{prefix}.signalp.gff
{script}/stat_tmhmm_signalp.py {prefix}.signalp.gff -t {prefix}.tmhmm.gff \\
  -o {prefix}.stat_secreted_protein.xls >{prefix}.secreted_protein.tsv
cp {prefix}.tmhmm.gff {prefix}.signalp.gff {prefix}.secreted_protein.tsv {prefix}.stat_secreted_protein.xls {out_dir}
""".format(id=id,
           prefix=prefix,
           script=SCRIPTS,
           out_dir=out_dir)
    )

    join_task.set_upstream(*tasks)

    return tasks, join_task, os.path.join(work_dir, "%s.stat_secreted_protein.xls" % prefix)


def advanal(protein, prefix, types, thread, job_type, concurrent, refresh,
                work_dir="", out_dir=""):

    work_dir = mkdir(work_dir)
    out_dir = mkdir(out_dir)
    protein = check_path(protein)

    proteins = split_protein(protein=protein,
        prefix=prefix,
        job_type=job_type,
        work_dir=mkdir(os.path.join(work_dir, "data")),
        size="100kb"
    )

    dag = DAG("run_secreted_protein")
    signalp_tasks, signalp_join, signalp_stat = create_signalp_task(
        proteins=proteins,
        prefix=prefix,
        types=types,
        thread=thread,
        job_type=job_type,
        work_dir=work_dir,
        out_dir=out_dir)

    dag.add_task(*signalp_tasks)
    dag.add_task(signalp_join)
    
    do_dag(dag, concurrent_tasks=concurrent, refresh_time=refresh)

    return 0


def add_hlep_args(parser):

    parser.add_argument("protein", metavar="FILE", type=str,
        help="Input protein sequence.")
    parser.add_argument("-p", "--prefix", metavar="STR", type=str, default="out",
        help="output file prefix.")
    parser.add_argument("--types", choices=["gram-", "gram+", "arch", "euk"], default="euk",
        help="Choose the type of bacteria, default=euk")
    parser.add_argument("-t", "--thread", metavar="INT", type=int, default=4,
        help="Set the running thread, default=4")
    parser.add_argument("--concurrent", metavar="INT", type=int, default=10,
        help="Maximum number of jobs concurrent  (default: 10)")
    parser.add_argument("--refresh", metavar="INT", type=int, default=30,
        help="Refresh time of log in seconds  (default: 30)")
    parser.add_argument("--job_type", choices=["sge", "local"], default="local",
        help="Jobs run on [sge, local]  (default: local)")
    parser.add_argument("--work_dir", metavar="DIR", default=".",
        help="Work directory (default: current directory)")
    parser.add_argument("--out_dir", metavar="DIR", default=".",
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
name:
    advanal.py Genome personalized analysis.
attention:
    advanal.py --protein protein.fasta --genebank genome.gb
version: %s
contact:  %s <%s>\
        ''' % (__version__, ' '.join(__author__), __email__))

    args = add_hlep_args(parser).parse_args()

    advanal(args.protein, args.prefix, args.types, args.thread,
            args.job_type, args.concurrent,
            args.refresh, args.work_dir, args.out_dir)


if __name__ == "__main__":

    main()
