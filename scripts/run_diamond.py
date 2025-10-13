#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import logging
import argparse

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "../"))
from pegs.config import *
from pegs.common import check_paths, mkdir, check_path, get_version
from dagflow import DAG, Task, ParallelTask, do_dag
from seqkit.split import seq_split

LOG = logging.getLogger(__name__)
__author__ = ("Xingguo Zhang",)
__email__ = "invicoun@foxmail.com"
__version__ = "v1.0.0"

DIAMOND_BIN = "/Work/pipeline/software/Base/diamond/v2.0.3/"

def create_diamond_tasks(proteins, prefix, db, evalue, coverage, threads,
                         job_type, work_dir="", out_dir="", dbname="diamond"):

    prefixs = [os.path.basename(i) for i in proteins]
    id = dbname

    tasks = ParallelTask(
        id=id,
        work_dir="%s/{id}" % work_dir,
        type=job_type,
        option="-pe smp %s %s" % (threads, QUEUE),
        script="""
export PATH={diamond}:$PATH
time diamond blastp --query {{protein}} --db {db} \\
--outfmt 6 qseqid sseqid pident length mismatch gapopen qstart qend sstart send evalue bitscore qlen slen stitle \\
--max-target-seqs 5 --evalue 1e-05 --threads {threads} --out {{prefix}}.{id}.m6
""".format(diamond=DIAMOND_BIN,
           db=db,
           id=id,
           evalue=evalue,
           script=SCRIPTS,
           threads=threads),
        prefix=prefixs,
        protein=proteins,
    )

    join_task = Task(
        id="merge_%s" % id,
        work_dir=work_dir,
        type=job_type,
        option="-pe smp 1 %s" % QUEUE,
        script="""
cat {id}*/*.{id}.m6 > {prefix}.{id}.m6
time {script}/blast_filter.py {prefix}.{id}.m6 \\
--outfmt std qlen slen stitle --out qseqid sseqid qstart qend stitle evalue bitscore \\
--min_qcov {coverage} --min_scov 0 --evalue {evalue} --best > {out_dir}/{prefix}.{id}.out
""".format(id=id,
           prefix=prefix,
           script=SCRIPTS,
           evalue=evalue,
           coverage=coverage,
           out_dir=out_dir)
    )

    join_task.set_upstream(*tasks)

    return tasks, join_task


def run_diamond(proteins, prefix, db, evalue, coverage, dbname,
                threads, job_type, work_dir, out_dir, concurrent, refresh):

    proteins = check_paths(proteins)
    work_dir = mkdir(os.path.abspath(work_dir))
    out_dir = mkdir(os.path.abspath(out_dir))
    if db:
        pass
    else:
        db = SWISSPORT
        dbname = "SwissProt"
    if len(proteins) <= 2:
        data = mkdir(os.path.join(work_dir, "00_data"))
        proteins = seq_split(proteins, mode="length", num=5000000, output_dir=data)

    dag = DAG("run_diamond")

    tasks, join_task = create_diamond_tasks(
        proteins=proteins,
        prefix=prefix,
        db=db,
        evalue=evalue,
        coverage=coverage,
        threads=threads,
        job_type=job_type,
        work_dir=work_dir,
        out_dir=out_dir,
        dbname=dbname
    )

    dag.add_task(*tasks)
    dag.add_task(join_task)

    do_dag(dag, concurrent_tasks=concurrent, refresh_time=refresh)

    return 0


def add_hlep_args(parser):

    parser.add_argument("-p", "--proteins", nargs='+', metavar="STR", type=str,
        help="Input protein sequence file.")
    parser.add_argument("--prefix", metavar="STR", type=str, default="ZXG",
        help="Input sample name.")
    parser.add_argument("-db", "--database", metavar="STR", type=str, default="",
        help="Input Enter the database path.")
    parser.add_argument("-dn", "--dbname", metavar="STR", type=str, default="diamond",
        help="Input the database name.")
    parser.add_argument("--evalue", metavar="NUM", type=float, default=1e-05,
        help="Evalue cutoff of blastp for database(default: 1e-05).")
    parser.add_argument("--coverage", metavar="NUM", type=float, default=30,
        help="Coverage cutoff of blastp for database(default: 30).")
    parser.add_argument("--threads", metavar="INT", type=int, default=4,
        help="Threads used to run blastp (default: 4)")
    parser.add_argument("--concurrent", metavar="INT", type=int, default=10,
        help="Maximum number of jobs concurrent  (default: 10)")
    parser.add_argument("--refresh", metavar="INT", type=int, default=30,
        help="Refresh time of log in seconds  (default: 30)")
    parser.add_argument("--job_type", choices=["sge", "local"], default="local",
        help="Jobs run on [sge, local]  (default: local)")
    parser.add_argument("--work_dir", metavar="DIR", type=str, default="diamond.work",
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
URL: https://github.com/zxgsy520/gfungi
name:
    run_diamond.py Batch align protein sequence files.

attention:
    run_diamond.py -p project.fasta --database uniprot_sprot.fasta --dbname uniprot

version: %s
contact:  %s <%s>\
        ''' % (__version__, ' '.join(__author__), __email__))

    args = add_hlep_args(parser).parse_args()

    run_diamond(proteins=args.proteins, prefix=args.prefix, db=args.database,
                evalue=args.evalue, coverage=args.coverage, dbname=args.dbname,
                threads=args.threads, job_type=args.job_type, work_dir=args.work_dir,
                out_dir=args.out_dir, concurrent=args.concurrent, refresh=args.refresh)


if __name__ == "__main__":

    main()
