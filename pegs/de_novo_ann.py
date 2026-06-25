#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import json
import time
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
__version__ = "v1.1.0"


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
    do_dag(dag, concurrent_tasks=1, refresh_time=10)
    #time.sleep(10) #防止读写延迟
    
    genomes = read_files(work_dir, "%s.part*.fa" % prefix)

    return genomes


def create_gmes_tasks(genomes, prefix, threads, job_type,
                      work_dir="", out_dir="", kingdom="fungi"):
    temp = ""
    if kingdom in ["fungi", "eukaryota"]:
        temp = "--fungus --min_contig 10000 --max_intron 3000 --max_intergenic 10000 --min_gene_in_predict 120"

    id = "gmes"

    tasks = ParallelTask(
        id=id,
        work_dir="%s/{id}" % work_dir,
        type=job_type,
        option="-pe smp %s %s" % (threads, QUEUE),
        script="""
export PATH={braker}:$PATH
export PERL5LIB="/Work/pipeline/software/Base/perl_module/PERL5/lib/perl5/:/Work/pipeline/software/Base/perl_module/BioPerl-1.7.7/lib/"
#cp {gmes}/gm_key ~/.gm_key
if [ ! -e data/dna.fna ]; then
  perl {gmes}/gmes_petap.pl {temp} --ES --cores {threads} --sequence {{genomes}}
  {gffread}/gffread genemark.gtf -o- >genemark.gff
fi
rm -rf data info output run
""".format(braker=BRAKER_BIN,
            gmes=GMES_BIN,
            gffread=GFFREAD_BIN,
            temp=temp,
            threads=threads),
        genomes=genomes,
    )

    join_task = Task(
        id="merge_%s" % id,
        work_dir=work_dir,
        type=job_type,
        option="-pe smp 1 %s" % QUEUE,
        script="""
if [ ! -e {prefix}.genemark.gff ];then
  {bin}/gffvert genemark2gff {id}*/genemark.gff >{prefix}.genemark.gff
fi
cp {prefix}.genemark.gff {out_dir}
""".format(bin=BIN,
           id=id,
           prefix=prefix,
           out_dir=out_dir)
    )

    join_task.set_upstream(*tasks)

    return tasks, join_task, os.path.join(out_dir, "%s.genemark.gff" % prefix)


def create_train_glimmerhmm_task(homo, homogff, species, job_type, work_dir="", kingdom="fungi"):

    if kingdom == "fungi":
        temp = "--min_exon 2"
    else:
        temp = ""
    train_task = Task(
        id="trainGlimmerHMM",
        work_dir=work_dir,
        type=job_type,
        option="-pe smp 2 %s" % QUEUE,
        script="""
{bin}/gffvert gff2glimmer {homogff} {temp} >{species}.exon.tsv
cat {homo}| cut -d " " -f 1 >{species}.genome.fasta
{glimmerhmm}/train/trainGlimmerHMM {species}.genome.fasta {species}.exon.tsv -d {species}
cp -rf {species} {glimmerhmm}/trained_dir
""".format(bin=BIN,
            glimmerhmm=GLIMMERHMM,
            temp=temp,
            homogff=homogff,
            homo=homo,
            species=species,
            )
    )

    return train_task, os.path.join(GLIMMERHMM, "trained_dir/%s" % species)


def create_glimmerhmm_tasks(genomes, prefix, model, job_type,
                      work_dir="", out_dir=""):

    id = "glimmerhmm"
    tasks = ParallelTask(
        id=id,
        work_dir="%s/{id}" % work_dir,
        type=job_type,
        option="-pe smp 4 %s" % QUEUE,
        script="""
{glimmerhmm}/bin/glimmhmm.pl {glimmerhmm}/bin/glimmerhmm \\
  {{genomes}} {model} "-f -g -n 1" >{prefix}.glimmerhmm.gff

#{glimmerhmm}/bin/glimmerhmm {{genomes}} \\
#  -d {model} -o {prefix}.glimmerhmm.gff -n 1 -g
""".format(glimmerhmm=GLIMMERHMM,
            model=model,
            prefix=prefix
            ),
        genomes=genomes,
    )

    join_task = Task(
        id="merge_%s" % id,
        work_dir=work_dir,
        type=job_type,
        option="-pe smp 1 %s" % QUEUE,
        script="""
{bin}/gffvert glimmerhmm2gff3 {id}*/{prefix}.glimmerhmm.gff >{prefix}.glimmerhmm.gff
cp {prefix}.glimmerhmm.gff {out_dir}
""".format(bin=BIN,
           id=id,
           prefix=prefix,
           out_dir=out_dir)
    )

    join_task.set_upstream(*tasks)

    return tasks, join_task,  os.path.join(out_dir, "%s.glimmerhmm.gff" % prefix)


def de_novo_ann(genome, homo, homogff, prefix, species, job_type, work_dir, out_dir,
                threads=10, concurrent=10, refresh=30, kingdom="fungi"):

    genome = check_path(genome)
    work_dir = mkdir(work_dir)
    out_dir = mkdir(out_dir)
    if not species:
        species = prefix
        
    work_dict = {
        "genome": "00_genome",
        "genemark": "01_genemark",
        "glimmerhmm": "02_glimmerHMM",
    }
    for k, v in work_dict.items():
        mkdir(os.path.join(work_dir, v))

    genomes = split_genome(
        genome=genome,
        prefix=prefix,
        job_type=job_type,
        work_dir=os.path.join(work_dir, work_dict["genome"]),
        size="20m"
    )#从头预测必须使用重复屏蔽的基因组数据

    dag = DAG("run_de_novo_ann")
    gmes_tasks, gmes_join_task, gmes_gff = create_gmes_tasks(
        genomes=genomes, #从头预测
        prefix=prefix,
        threads=threads,
        job_type=job_type,
        work_dir=os.path.join(work_dir, work_dict["genemark"]),
        out_dir=out_dir,
        kingdom=kingdom
    )
    dag.add_task(*gmes_tasks)
    dag.add_task(gmes_join_task)
    
    if homo:
        homo = check_paths(homo)
        homogff = check_paths(homogff)
        model = os.path.join(GLIMMERHMM, "trained_dir/%s" % species)

        if not os.path.isdir(model):
            train_task, model = create_train_glimmerhmm_task(
                homo=" ".join(homo),
                homogff=" ".join(homogff),
                species=species,
                job_type=job_type,
                work_dir=mkdir(os.path.join(work_dir, "train_glimmerhmm")),
                kingdom=kingdom
            )
            dag.add_task(train_task)

        glimmer_tasks, glimmer_join_task, glimmer_gff= create_glimmerhmm_tasks(
            genomes=genomes,
            prefix=prefix,
            model=model,
            job_type=job_type,
            work_dir=os.path.join(work_dir, work_dict["glimmerhmm"]),
            out_dir=out_dir
        )
        if not os.path.isdir(model):
            train_task.set_downstream(*glimmer_tasks)
        dag.add_task(*glimmer_tasks)
        dag.add_task(glimmer_join_task)

    else:
        glimmer_gff = ""
        glimmer_join_task = ""

    do_dag(dag, concurrent_tasks=concurrent, refresh_time=refresh)

    return gmes_gff, glimmer_gff


def add_hlep_args(parser):

    parser.add_argument("genome", metavar="STR", type=str,
        help="Input genome file(fasta).")
    parser.add_argument("--homo", nargs="+", metavar="FILE", type=str, default=False,
        help="Input homologous species genome sequences.")
    parser.add_argument("-hf", "--homogff", nargs="+", metavar="FILE", type=str, default=False,
        help="Input gff file for homologous species annotation.")
    parser.add_argument("--prefix", metavar="STR", type=str, default="ZXG",
        help="Input sample name.")
    parser.add_argument("-s", "--species", metavar="STR", type=str,  default=False,
        help="Input the species name of the sample.")
    parser.add_argument("-k", "--kingdom", choices=["fungi", "plant", "animal", "eukaryota"],
        help="Kingdom of the sample (default: fungi)", default="fungi"),
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
    description='''
URL: https://github.com/zxgsy520/bypegs
name:
    de_novo_ann.py Rapid prediction of fungal genome genes

attention:
    de_novo_ann.py genome.fasta --prefix name
    de_novo_ann.py genome.fasta --homo homo.fasta --homogff homo.gff --prefix name
version: %s
contact:  %s <%s>\
        ''' % (__version__, ' '.join(__author__), __email__))

    args = add_hlep_args(parser).parse_args()

    de_novo_ann(genome=args.genome, homo=args.homo, homogff=args.homogff,
        prefix=args.prefix, species=args.species, threads=args.threads,
        job_type=args.job_type, work_dir=args.work_dir, out_dir=args.out_dir,
        concurrent=args.concurrent, refresh=args.refresh, kingdom=args.kingdom)


if __name__ == "__main__":

    main()
