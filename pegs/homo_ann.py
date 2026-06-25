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
from pegs.common import check_paths, mkdir, check_path, get_version, read_files

LOG = logging.getLogger(__name__)
__author__ = ("Xingguo Zhang",)
__email__ = "invicoun@foxmail.com"
__version__ = "v2.0.1"


def split_genome(genome, prefix, job_type, work_dir, size="10m"):

    dag = DAG("split_genome")
    task = Task(
        id="split_genome",
        work_dir=work_dir,
        type=job_type,
        option="-pe smp 2 %s" % QUEUE,
        script="""
{bin}/biotool seqsplit {genome} --size {size} \\
  --prefix {prefix} --workdir {work_dir}
""".format(bin=BIN,
            genome=genome,
            prefix=prefix,
            size=size,
            work_dir=work_dir,
        )
    )
    dag.add_task(task)
    do_dag(dag, concurrent_tasks=1, refresh_time=20)

    genomes = read_files(work_dir, "%s.part*.fa" % prefix)

    return genomes


def create_metaeuk_tasks(genomes, prefix, protein, threads, job_type,
                      work_dir="", out_dir="", translation_table=1):

    id = "metaeuk"
    tasks = ParallelTask(
        id=id,
        work_dir="%s/{id}" % work_dir,
        type=job_type,
        option="-pe smp %s %s" % (threads, QUEUE),
        script="""
export PATH={metaeuk}:$PATH
metaeuk easy-predict {{genomes}} {protein} {prefix}.metaeuk tempFolder --threads {threads} --translation-table {translation_table}
rm -rf tempFolder
""".format(metaeuk=METAEUK_BIN,
            prefix=prefix,
            protein=protein,
            translation_table=translation_table,
            threads=threads),
        genomes=genomes,
    )

    join_task = Task(
        id="merge_%s" % id,
        work_dir=work_dir,
        type=job_type,
        option="-pe smp 1 %s" % QUEUE,
        script="""
cat {id}*/{prefix}.metaeuk.gff |{bin}/gffvert metaeuk2gff >{prefix}.metaeuk.gff
cp {prefix}.metaeuk.gff {out_dir}
""".format(bin=BIN,
           id=id,
           prefix=prefix,
           out_dir=out_dir)
    )

    join_task.set_upstream(*tasks)

    return tasks, join_task,  os.path.join(out_dir, "%s.metaeuk.gff" % prefix)


def create_miniprot_tasks(genomes, prefix, threads, job_type, work_dir="", out_dir="",
                          translation_table=1, protein=UNIPROT_DB):
    
    id = "miniprot"
    tasks = ParallelTask(
        id=id,
        work_dir="%s/{id}" % work_dir,
        type=job_type,
        option="-pe smp %s %s" % (threads, QUEUE),
        script="""
{miniprot}/miniprot -Iut16 --gff {{genomes}} {protein} -t {threads} > {prefix}.miniprot.gff
#使用高质量的蛋白
""".format(miniprot=MINIPROT_BIN,
            translation_table=translation_table,
            prefix=prefix,
            protein=protein,
            threads=threads),
        genomes=genomes,
    )

    join_task = Task(
        id="merge_%s" % id,
        work_dir=work_dir,
        type=job_type,
        option="-pe smp 1 %s" % QUEUE,
        script="""
{bin}/gffvert genemark2gff {id}*/{prefix}.miniprot.gff  >{prefix}.miniprot.gff
cp {prefix}.miniprot.gff {out_dir}
""".format(bin=BIN,
           id=id,
           prefix=prefix,
           out_dir=out_dir)
    )

    join_task.set_upstream(*tasks)

    return tasks, join_task,  os.path.join(out_dir, "%s.miniprot.gff" % prefix)


def create_eviann_tasks(genomes, protein, cds,  rna_list, prefix="out", threads=10, job_type="sge",
                        work_dir="", out_dir=""):

    x = 0
    if rna_list:
        x = "-r %s" % rna_list

    x1 = ""
    if cds:
        x1= "-e %s" % cds
    
    id = "eviann"
    tasks = ParallelTask(
        id=id,
        work_dir="%s/{id}" % work_dir,
        type=job_type,
        option="-pe smp %s %s" % (threads, QUEUE),
        script="""
export PATH={eviann}:$PATH
if [ -f "{{genomes}}.u.gff.tmp" ] && [ ! -s "{{genomes}}.u.gff.tmp" ]; then
  echo "没有预测到基因(文件存在且为空)"
  touch {{genomes}}.pseudo_label.gff
else
  echo "进行基因的预测"
  {eviann}/eviann.sh -t {threads} -g {{genomes}} {x} {x1} -p {protein}
fi
rm *.bam
""".format(eviann=EVIANN_BIN,
            prefix=prefix,
            x=x,
            x1=x1,
            protein=protein,
            threads=threads),
        genomes=genomes,
    )

    join_task = Task(
        id="merge_%s" % id,
        work_dir=work_dir,
        type=job_type,
        option="-pe smp 1 %s" % QUEUE,
        script="""
{bin}/gffvert genemark2gff {id}*/*.pseudo_label.gff  >{prefix}.eviann.gff
cp {prefix}.eviann.gff {out_dir}
#rm -rf {id}*
""".format(bin=BIN,
           id=id,
           prefix=prefix,
           out_dir=out_dir)
    )

    join_task.set_upstream(*tasks)

    return tasks, join_task, os.path.join(out_dir, "%s.eviann.gff" % prefix)


def homo_ann(genome, protein, cds, rna_list, prefix, job_type, work_dir, out_dir,
                 threads=10, concurrent=10, refresh=30, mprotein=UNIPROT_DB, translation_table=1):

    genome = check_path(genome)
    protein = check_path(protein)
    if cds:
        cds = check_path(cds)
    if rna_list:
        rna_list = check_path(rna_list)

    work_dir = mkdir(work_dir)
    out_dir = mkdir(out_dir)
        
    work_dict = {
        "genome": "00_genome",
        "metaeuk": "01_metaeuk",
        "miniprot": "02_miniprot",
        "eviann": "02_eviann"
    }
    for k, v in work_dict.items():
        mkdir(os.path.join(work_dir, v))
   
    genomes = split_genome(
        genome=genome,
        prefix=prefix,
        job_type=job_type,
        work_dir=os.path.join(work_dir, work_dict["genome"]),
        size="20m"
    )  #同源预测和转录组辅助预测可以使用非重复屏蔽的基因组数据


    dag = DAG("homo_ann")
    metaeuk_tasks, metaeuk_join_task, metaeuk_gff = create_metaeuk_tasks(
        genomes=genomes, #同源预测
        prefix=prefix,
        protein=protein,
        threads=threads,
        translation_table=translation_table,
        job_type=job_type,
        work_dir=os.path.join(work_dir, work_dict["metaeuk"]),
        out_dir=out_dir
    )

    dag.add_task(*metaeuk_tasks)
    dag.add_task(metaeuk_join_task)

    if cds or rna_list:
        eviann_tasks, eviann_join_task, eviann_gff = create_eviann_tasks(
            genomes=genomes,
            protein=protein,
            cds=cds,
            rna_list=rna_list,
            prefix=prefix,
            threads=threads, 
            job_type=job_type,
            work_dir=os.path.join(work_dir, work_dict["eviann"]),
            out_dir=out_dir
        )
        dag.add_task(*eviann_tasks)
        dag.add_task(eviann_join_task)
        miniprot_gff = eviann_gff
    else:
        miniprot_tasks, miniprot_join_task, miniprot_gff = create_miniprot_tasks(
            genomes=genomes,
            protein=mprotein,
            prefix=prefix,
            threads=threads,
            job_type=job_type,
            translation_table=translation_table,
            work_dir=os.path.join(work_dir, work_dict["miniprot"]),
            out_dir=out_dir
        )
        dag.add_task(*miniprot_tasks)
        dag.add_task(miniprot_join_task)

    do_dag(dag, concurrent_tasks=concurrent, refresh_time=refresh)

    return metaeuk_gff, miniprot_gff


def add_hlep_args(parser):

    parser.add_argument("genome", metavar="STR", type=str,
        help="Input genome file(fasta).")
    parser.add_argument("-p", "--protein", metavar="FILE", type=str, required=True,
        help="Input homologous species protein sequence.")
    parser.add_argument("--cds", metavar="FILE", type=str, default="",
        help="Input homologous species cds sequence.")
    parser.add_argument("--rna_list", metavar="FILE", type=str, default="",
        help="Input homologous species RNA sequence.")
    parser.add_argument("-mp", "--mprotein", metavar="FILE", type=str, default=UNIPROT_DB,
        help="Input uniprot protein sequence,default=%s" % UNIPROT_DB)
    parser.add_argument("-t", "--translation_table", metavar="INT", type=int, default=1,
        help="Set genetic code(https://www.ncbi.nlm.nih.gov/Taxonomy/taxonomyhome.html/index.cgi?chapter=cgencodes), default=1")
    parser.add_argument("--prefix", metavar="STR", type=str, default="ZXG",
        help="Input sample name.")
    parser.add_argument("--threads", metavar="INT", type=int, default=4,
        help="Threads used to run blastp (default: 4)")
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
    description="""
URL: https://github.com/zxgsy520/bypegs
name:
    homo_ann.py Gene homology annotation

attention:
    homo_ann.py genome.fasta -p homo.protein.fasta --prefix name

version: %s
contact:  %s <%s>\
        """ % (__version__, " ".join(__author__), __email__))

    args = add_hlep_args(parser).parse_args()

    homo_ann(genome=args.genome, protein=args.protein, cds=args.cds, rna_list=args.rna_list, 
        mprotein=args.mprotein, prefix=args.prefix, translation_table=args.translation_table,
        job_type=args.job_type, work_dir=args.work_dir, out_dir=args.out_dir,
        threads=args.threads, concurrent=args.concurrent, refresh=args.refresh
    )

if __name__ == "__main__":

    main()
