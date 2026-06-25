#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import json
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
__version__ = "v1.1.0"


AUGUSTUS_CONFIG = "/Work/pipeline/software/Base/augustus/lastest/config"
AUGUSTUS_BIN = "/Work/pipeline/software/Base/augustus/lastest/bin/"
AUGUSTUS_SCRIPTS = "/Work/pipeline/software/Base/augustus/lastest/bin/"

def create_filter_gene_task(gtf, genome, species, job_type, work_dir, nmax=2000, min_exon=3, max_exon=20):

    task = Task(
        id="filter_gene",
        work_dir=work_dir,
        type=job_type,
        option="-pe smp 2 %s" % QUEUE,
        script="""
export AUGUSTUS_CONFIG_PATH={augustus_config}
export PATH={augustus}:$PATH
cp {gtf} genome.gff3
{gffread}/gffread genome.gff3 -g {genome} -x raw-cds.fa
#过滤异常的CDS
{bin}/gffvert cds2aa raw-cds.fa >bonafide.faa
#取冗余
#{cdhit}/cd-hit -i bonafide.faa -o uniclean.faa  -c 0.75 -n 5 -d 0 -T 6 -M 20000 -aS 0.80
#官方的脚本有问题，需要修改
perl {scripts}/aa2nonred.pl bonafide.faa uniclean.faa --maxid=0.75 --DIAMOND_PATH={augustus} --cores=8 --diamond
rm -rf bonafide.faa.nonreddb.*
grep ">" uniclean.faa|sed 's/>//g' >uniclean.id
{bin}/gffvert gff2keep genome.gff3 --keepids uniclean.id --mRNA >clean.gff
{bin}/filter2aug_gff.py clean.gff --addmrna --gene_num {nmax} --min_exon {min_exon} --max_exon {max_exon} >hclean.gff

#格式转换
perl {scripts}/gff2gbSmallDNA.pl hclean.gff {genome} 1000 genes.raw.gb
#尝试训练，捕捉错误
rm -rf {augustus_config}/species/{species}
perl {scripts}/new_species.pl --species={species}
etraining --species={species} --stopCodonExcludedFromCDS=false genes.raw.gb 2> train.err
#过滤掉可能错误掉基因结构
#cat train.err |sed 's/in sequence /*/' |sed 's/: /*/'|cut -d "*" -f2 > badgenes.lst
cat train.err |perl -pe 's/.*in sequence (\S+): .*/$1/' >badgenes.lst
perl {scripts}/filterGenes.pl badgenes.lst genes.raw.gb > bonafide.gb
rm -rf genome.gff3 raw-cds.fa genes.raw.gb 
rm -rf {augustus_config}/species/{species}
""".format(augustus=AUGUSTUS_BIN,
            augustus_config=AUGUSTUS_CONFIG,
            scripts=AUGUSTUS_SCRIPTS,
            bin=BIN,
            gffread=GFFREAD_BIN,
            cdhit=CDHIT,
            gtf=gtf,
            genome=genome,
            species=species,
            nmax=nmax,
            min_exon=min_exon,
            max_exon=max_exon
        )
    )

    return task, os.path.join(work_dir, "bonafide.gb")


def create_first_train_task(species, genbank, job_type, work_dir, ntest=200):

    task = Task(
        id="train1",
        work_dir=work_dir,
        type=job_type,
        option="-pe smp 2 %s" % QUEUE,
        script="""
export AUGUSTUS_CONFIG_PATH={augustus_config}
export PATH={augustus}:$PATH
cp {genbank} bonafide.f.gb
perl {scripts}/new_species.pl --species={species}
perl {scripts}/randomSplit.pl bonafide.f.gb {ntest}
#进行第一次训练
etraining --species={species} bonafide.f.gb.train >train.out
#修改CDS最后3个碱基不是终止密码子的文件。将parameters.cfg文件中stopCodonExcludedFromCDS从默认的false修改为true
{bin}/augtool rm_stop_codon train.out -gb bonafide.f.gb.train --species {species} --aug_config {augustus_config}
#进行检测
augustus --species={species} bonafide.f.gb.test > firsttest.out
""".format(augustus=AUGUSTUS_BIN,
            augustus_config=AUGUSTUS_CONFIG,
            scripts=AUGUSTUS_SCRIPTS,
            bin=BIN,
            species=species,
            genbank=genbank,
            ntest=ntest
        )
    )

    return task, os.path.join(work_dir, "bonafide.f.gb.train"), os.path.join(work_dir, "bonafide.f.gb.test")


def create_second_train_task(species, train_gb, test_gb, job_type, work_dir):

    task = Task(
        id="train2",
        work_dir=work_dir,
        type=job_type,
        option="-pe smp 8 %s" % QUEUE,
        script="""
export AUGUSTUS_CONFIG_PATH={augustus_config}
export PATH={augustus}:$PATH
ln -s {train_gb} genes.gb.train
perl {scripts}/randomSplit.pl genes.gb.train 135
ln -s genes.gb.train.train training.gb.onlytrin
perl {scripts}/optimize_augustus.pl --rounds=5 --kfold=8 --cpus=8 --species={species} genes.gb.train.test --onlytrain=training.gb.onlytrin > optimize.out
#第二次etraining, 正式的训练
etraining --species={species} genes.gb.train &> etrain.out
#根据训练结果重新修改参数,并检测一次准确度
{bin}/augtool edit_stop_freq etrain.out --species {species} --aug_config {augustus_config}
augustus --species={species} {test_gb} > firsttest.out
{bin}/augtool aug_report firsttest.out
""".format(augustus=AUGUSTUS_BIN,
            augustus_config=AUGUSTUS_CONFIG,
            scripts=AUGUSTUS_SCRIPTS,
            bin=BIN,
            species=species,
            train_gb=train_gb,
            test_gb=test_gb
        )
    )

    return task


def run_train2aug_model(genome, gff, species, ntest, nmax, work_dir,
                        job_type="sge", concurrent=10, refresh=15, min_exon=3, max_exon=20):

    genome = check_path(genome)
    gff = check_path(gff)
    work_dir = mkdir(work_dir)
    options = {
        "software": OrderedDict(),
        "database": OrderedDict()
    }

    #flank = check_flanking(gff)
    dag = DAG("training_aug")

    filter_gene_task, genbank = create_filter_gene_task(
        gtf=gff, #过滤异常的基因
        genome=genome,
        species=species,
        job_type=job_type,
        work_dir=os.path.join(work_dir, "01_filter_gene"),
        nmax=nmax,
        min_exon=min_exon,
        max_exon=max_exon
    )
    dag.add_task(filter_gene_task)

    train1_task, train_gb, test_gb = create_first_train_task(
        species=species,
        genbank=genbank,
        job_type=job_type,
        work_dir=os.path.join(work_dir, "02_train1"),
        ntest=ntest
    )
    dag.add_task(train1_task)
    train1_task.set_upstream(filter_gene_task)

    train2_task = create_second_train_task(
        species=species,
        train_gb=train_gb,
        test_gb=test_gb,
        job_type=job_type,
        work_dir=os.path.join(work_dir, "03_train2")
    )
    dag.add_task(train2_task)
    train2_task.set_upstream(train1_task)

    do_dag(dag, concurrent_tasks=concurrent, refresh_time=refresh)

    return options


def train2aug_model(args):

    options = run_train2aug_model(
        genome=args.genome,
        gff=args.gff,
        species=args.species,
        ntest=args.ntest,
        nmax=args.nmax,
        job_type=args.job_type,
        work_dir=args.work_dir,
        concurrent=args.concurrent,
        refresh=args.refresh,
        min_exon=args.min_exon,
        max_exon=args.max_exon
    )

    with open(os.path.join(args.work_dir, "training_aug.json"), "w") as fh:
        json.dump(options, fh, indent=2)

    return 0


def add_hlep_args(parser):

    parser.add_argument("genome", metavar="FILE", type=str,
        help="Input genome file(fata).")
    parser.add_argument("--gff", metavar="FILE", type=str, required=True,
        help="Good genemodel for train, gtf or gff.")
    parser.add_argument("-s", "--species", metavar="SRT", type=str, required=True,
        help="Augustus species name for train.")
    parser.add_argument("--nmax", metavar="INT", type=int, default=2000,
        help="Max number of gene for train(太多基因用于训练，消耗时间太久了), default=2000.")
    parser.add_argument("-mine", "--min_exon", metavar="INT", type=int, default=5,
        help="Filter the minimum number of exons, default=5")
    parser.add_argument("-maxe", "--max_exon", metavar="INT", type=int, default=15,
        help="Filter the maximum number of exons, default=15")
    parser.add_argument("--ntest", metavar="INT", type=int, default=100,
        help="Number of gene for test accuracy, default=100")
    parser.add_argument("--concurrent", metavar="INT", type=int, default=10,
        help="Maximum number of jobs concurrent  (default: 10)")
    parser.add_argument("--refresh", metavar="INT", type=int, default=30,
        help="Refresh time of log in seconds  (default: 30)")
    parser.add_argument("--job_type", choices=["sge", "local"], default="local",
        help="Jobs run on [sge, local]  (default: local)")
    parser.add_argument("-w", "--work_dir", metavar="DIR", type=str, default="work",
        help="Work directory (default: current directory)")

    return parser



def main():

    logging.basicConfig(
        stream=sys.stderr,
        level=logging.INFO,
        format="[%(levelname)s] %(message)s"
    )
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
    description="""
URL: https://github.com/zxgsy520/gfungi
name:
    train2aug_model.py: Train the Augustus model

attention:
    train2aug_model.py genome.fasta --gff train.gtf --species name

version: %s
contact:  %s <%s>\
        """ % (__version__, " ".join(__author__), __email__))

    args = add_hlep_args(parser).parse_args()

    train2aug_model(args)


if __name__ == "__main__":
    main()
