#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import json
import logging
import argparse

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "../"))
from gfungi.config import *
from gfungi.common import check_paths, mkdir, check_path, get_version
from thirdparty.dagflow import DAG, Task, ParallelTask, do_dag
from thirdparty.seqkit.split import seq_split

LOG = logging.getLogger(__name__)
__author__ = ("Xingguo Zhang",)
__email__ = "invicoun@foxmail.com"
__version__ = "v1.0.0"


AUGUSTUS_CONFIG = "/Work/pipeline/software/Base/augustus/lastest/config"
AUGUSTUS_BIN = "/Work/pipeline/software/Base/augustus/lastest/bin/"
AUGUSTUS_SCRIPTS = "/Work/pipeline/software/Base/augustus/lastest/bin/"


def check_flanking(gtf):

    cmd = """export PATH={augustus}:$PATH
perl {scripts}/computeFlankingRegion.pl {gtf}
""".format(augustus=AUGUSTUS_BIN,
            scripts=AUGUSTUS_SCRIPTS,
            gtf=gtf,
        )

    LOG.info(cmd)

    descrip = os.popen(cmd)
    flank = "0"
    for line in descrip.read().split("\n"):
        if "flanking_DNA value" not in line:
            continue
        LOG.info(line)
        flank = line.split(":")[-1].split("(")[0]

    return int(flank.strip())


def create_first_train_task(gtf, genome, flank, species, job_type, work_dir):

    task = Task(
        id="train1",
        work_dir=work_dir,
        type=job_type,
        option="-pe smp 2 %s" % QUEUE,
        script="""
export AUGUSTUS_CONFIG_PATH={augustus_config}
export PATH={augustus}:$PATH
perl {scripts}/gff2gbSmallDNA.pl {gtf} {genome} {flank} tmp.gb
cp tmp.gb bonafide.gb
#perl {scripts}/filterGenesIn_mRNAname.pl {gtf} tmp.gb > bonafide.gb
perl {scripts}/new_species.pl --species={species}
etraining --species={species} bonafide.gb &> bonafide.out
#第一次etraining, 确定gb文件是否包含终止密码子
{bin}/augtool rm_stop_codon bonafide.out -gb bonafide.gb --species {species} --aug_config {augustus_config}
""".format(augustus=AUGUSTUS_BIN,
            augustus_config=AUGUSTUS_CONFIG,
            scripts=AUGUSTUS_SCRIPTS,
            bin=BIN,
            gtf=gtf,
            genome=genome,
            flank=flank,
            species=species
        )
    )

    return task, os.path.join(work_dir, 'bonafide.gb')


def create_second_train_task(species, genbank, job_type, work_dir, ngene=600, nmax=2000):

    task = Task(
        id="train2",
        work_dir=work_dir,
        type=job_type,
        option="-pe smp 2 %s" % QUEUE,
        script="""
export AUGUSTUS_CONFIG_PATH={augustus_config}
export PATH={augustus}:$PATH
#第二次etraining, 排除bad gene
etraining --species={species} {genbank} 2>&1 | grep "n sequence" |grep -oP 'n sequence (\S+):' |sed 's/n sequence //g;s/://g' | sort -u > bad.lst
#in-frame stop codon, Initial exon has length < 3!, Initial exon does not begin with start codon but with tta
perl {scripts}/filterGenes.pl bad.lst {genbank} > bonafide.f.gb

gene=`{bin}/augtool check_genes bonafide.f.gb --gene {ngene}`
if [ $gene -gt {nmax} ];then
    perl {scripts}/randomSplit.pl bonafide.f.gb {nmax}
    mv bonafide.f.gb.test bonafide.f.gb
    rm bonafide.f.gb.train
 else
    echo "$gene not number"
fi
""".format(augustus=AUGUSTUS_BIN,
            augustus_config=AUGUSTUS_CONFIG,
            scripts=AUGUSTUS_SCRIPTS,
            bin=BIN,
            species=species,
            genbank=genbank,
            ngene=ngene,
            nmax=nmax
        )
    )

    return task, os.path.join(work_dir, 'bonafide.f.gb')


def create_third_train_task(species, genbank, ntest, job_type, work_dir):

    task = Task(
        id="train3",
        work_dir=work_dir,
        type=job_type,
        option="-pe smp 2 %s" % QUEUE,
        script="""
export AUGUSTUS_CONFIG_PATH={augustus_config}
export PATH={augustus}:$PATH
perl {scripts}/randomSplit.pl {genbank} {ntest}
mv {genbank}.test test.gb
mv  {genbank}.train train.gb
#第三次etraining, 正式的训练
etraining --species={species} train.gb &> etrain.out
#根据训练结果重新修改参数,并检测一次准确度
{bin}/augtool edit_stop_freq etrain.out --species {species} --aug_config {augustus_config}
""".format(augustus=AUGUSTUS_BIN,
            augustus_config=AUGUSTUS_CONFIG,
            scripts=AUGUSTUS_SCRIPTS,
            bin=BIN,
            species=species,
            genbank=genbank,
            ntest=ntest
        )
    )

    return task, os.path.join(work_dir, 'test.gb')


def create_polish1_model_task(species, genbank, job_type, work_dir):

    task = Task(
        id="polish1",
        work_dir=work_dir,
        type=job_type,
        option="-pe smp 2 %s" % QUEUE,
        script="""
export AUGUSTUS_CONFIG_PATH={augustus_config}
export PATH={augustus}:$PATH
augustus --species={species} {genbank} > test.out
#根据训练结果重新修改参数,并检测一次准确度
{bin}/augtool aug_report test.out
#报告准确度， 如果核酸水平低于0.6,或者外显子水平低于0.4，或者基因水平低于0.1，则warning
""".format(augustus=AUGUSTUS_BIN,
            augustus_config=AUGUSTUS_CONFIG,
            bin=BIN,
            species=species,
            genbank=genbank
        )
    )

    return task


def create_polish2_model_task(species, genbank, ntest, kfold, job_type, work_dir):

    task = Task(
        id="polish2",
        work_dir=work_dir,
        type=job_type,
        option="-pe smp 2 %s" % QUEUE,
        script="""
export AUGUSTUS_CONFIG_PATH={augustus_config}
export PATH={augustus}:$PATH
perl {scripts}/randomSplit.pl {genbank} {ntest}
perl {scripts}/optimize_augustus.pl --kfold={kfold} --cpus={kfold} --species={species} {genbank}.test --onlytrain={genbank}.train > optimize.out
etraining --species={species} {genbank}
augustus --species={species} {genbank} > test.opt.out
{bin}/augtool aug_report test.opt.out
""".format(augustus=AUGUSTUS_BIN,
            augustus_config=AUGUSTUS_CONFIG,
            scripts=AUGUSTUS_SCRIPTS,
            bin=BIN,
            species=species,
            genbank=genbank,
            ntest=ntest,
            kfold=kfold,
        )
    )

    return task


def run_training_aug(genome, gff, species, ntest, nmax, kfold, work_dir,
                job_type="sge", concurrent=10, refresh=15):

    genome = check_path(genome)
    gff = check_path(gff)
    work_dir = mkdir(work_dir)
    options = {
        "software": OrderedDict(),
        "database": OrderedDict()
    }

    flank = check_flanking(gff)
    dag = DAG("training_aug")

    train1_task, genbank = create_first_train_task(
        gtf=gff,
        genome=genome,
        flank=flank,
        species=species,
        job_type=job_type,
        work_dir=os.path.join(work_dir, "01_train1")
    )
    dag.add_task(train1_task)

    train2_task, genbank = create_second_train_task(
        species=species,
        genbank=genbank,
        job_type=job_type,
        work_dir=os.path.join(work_dir, "02_train2"),
        ngene=3*ntest,
        nmax=nmax
    )
    dag.add_task(train2_task)
    train2_task.set_upstream(train1_task)

    train3_task, genbank = create_third_train_task(
        species=species,
        genbank=genbank,
        ntest=ntest,
        job_type=job_type,
        work_dir=os.path.join(work_dir, "03_train3")
    )
    dag.add_task(train3_task)
    train3_task.set_upstream(train2_task)

    polish1_task = create_polish1_model_task(
        species=species,
        genbank=genbank,
        job_type=job_type,
        work_dir=os.path.join(work_dir, "04_polish1")
    )
    dag.add_task(polish1_task)
    polish1_task.set_upstream(train3_task)

    polish2_task = create_polish2_model_task(
        species=species,
        genbank=genbank,
        ntest=ntest,
        kfold=kfold,
        job_type=job_type,
        work_dir=work_dir
    )

    dag.add_task(polish2_task)
    polish2_task.set_upstream(polish1_task)

    do_dag(dag, concurrent_tasks=concurrent, refresh_time=refresh)

    return options


def training_aug(args):

    options = run_training_aug(
        genome=args.genome,
        gff=args.gff,
        species=args.species,
        ntest=args.ntest,
        nmax=args.nmax,
        kfold=args.kfold,
        job_type=args.job_type,
        work_dir=args.work_dir,
        concurrent=args.concurrent,
        refresh=args.refresh
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
        help="Max number of gene for train.")
    parser.add_argument("--ntest", metavar="INT", type=int, default=200,
        help="Number of gene for test accuracy, 3*ntest gene you must have, at least")
    parser.add_argument("--kfold", metavar="INT", type=int, default=8,
        help="kfold in optimize")
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
    description='''
URL: https://github.com/zxgsy520/gfungi
name:
    training_aug.py: Train the Augustus model

attention:
    training_aug.py genome.fasta --gff train.gtf --species name

version: %s
contact:  %s <%s>\
        ''' % (__version__, ' '.join(__author__), __email__))

    args = add_hlep_args(parser).parse_args()

    training_aug(args)


if __name__ == '__main__':
    main()
