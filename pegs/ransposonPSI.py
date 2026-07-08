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
from pegs.common import check_path, mkdir, read_tsv, read_files, get_version

LOG = logging.getLogger(__name__)

__author__ = ("Xingguo Zhang",)
__email__ = "invicoun@foxmail.com"
__version__ = "v1.1.0"


def split_protein(genome, prefix, gff, job_type, work_dir, size="5m"):

    dag = DAG("split_protein")
    task = Task(
        id="split_protein",
        work_dir=work_dir,
        type=job_type,
        option="-pe smp 2 %s" % QUEUE,
        script="""
{gffread}/gffread {gff} -g {genome} -y {prefix}.protein.fasta
{bin}/biotool seqsplit {prefix}.protein.fasta --size {size} \\
  --prefix {prefix} --workdir {work_dir}
""".format(bin=BIN,
            gffread=GFFREAD_BIN,
            genome=genome,
            gff=gff,
            prefix=prefix,
            size=size,
            work_dir=work_dir,
        )
    )
    dag.add_task(task)
    do_dag(dag, 8, 10)

    proteins = read_files(work_dir, "%s.part*.fa" % prefix)

    return proteins


def create_ransposonPSI_tasks(proteins, prefix, gff, genome, translation_table=1, job_type="sge",
                       work_dir="", out_dir=""):

    prefixs = [os.path.basename(i) for i in proteins]

    id = "ransposonPSI"
    tasks = ParallelTask(
        id=id,
        work_dir="%s/{id}" % work_dir,
        type=job_type,
        option="-pe smp 2 %s" % QUEUE,
        script="""
export BLOSUM62={PSI_bin}/BLOSUM62
export PATH={PSI_bin}:$PATH
cp {PSI_bin}/BLOSUM62 .
cp {{proteins}} {{prefixs}} #输入软件需要在运行路径中
perl {PSI_bin}/transposonPSI.pl {{prefixs}} prot
rm BLOSUM62 {{prefixs}}
rm -rf *.tmp
""".format(PSI_bin=PSI_BIN,
            ),
        proteins=proteins,
        prefixs=prefixs
    )

    join_task = Task(
        id="merge_PSI",
        work_dir=work_dir,
        type=job_type,
        option="-pe smp 2 %s" % QUEUE,
        script="""
cat {id}*/*.TPSI.topHits > {prefix}.TPSI.topHits
grep -v ^'//' {prefix}.TPSI.topHits | cut -f 6 > {prefix}.transoposonPSI.ids
#gffread 提取的CDS以RNA的id命名
{bin}/gffvert gff2rmgene {gff} --rmidlist {prefix}.transoposonPSI.ids --mRNA > {prefix}.noPSI.gff
#python {script}/filter_list_gff.py --gff {gff} --idlist {prefix}.transoposonPSI.ids --exp > {prefix}.noPSI.gff
{gffread}/gffread {prefix}.noPSI.gff -g {genome} -x {prefix}.noPSI.CDS.fasta
{bin}/gffvert cds2aa {prefix}.noPSI.CDS.fasta --check >{prefix}.noPSI.CDS.check
awk '$4*$5*$6==0' {prefix}.noPSI.CDS.check >{prefix}.checkWrong.ids
cut -f 1 {prefix}.checkWrong.ids |sed '1d' > {prefix}.Wrong.ids
{bin}/gffvert gff2rmgene {prefix}.noPSI.gff --rmidlist {prefix}.Wrong.ids --mRNA > {prefix}.filter.gff
{bin}/gffvert sort_gff {prefix}.filter.gff --locustag {prefix} >{prefix}.genome.gff3
{gffread}/gffread {prefix}.genome.gff3 -g {genome} -x {prefix}.CDS.fasta
{bin}/gffvert cds2aa {prefix}.CDS.fasta --transl_table {translation_table} --force >{prefix}.protein.fasta

{bin}/gffvert get_seq {prefix}.genome.gff3 --genome {genome} --dtype gene >{prefix}.gene.fasta
cut -d "." -f 1 {prefix}.protein.fasta >{out_dir}/{prefix}.protein.fasta
cut -d "." -f 1 {prefix}.CDS.fasta >{out_dir}/{prefix}.CDS.fasta
cp {prefix}.genome.gff3 {prefix}.gene.fasta {out_dir}
""".format(script=SCRIPTS,
           bin=BIN,
           gffread=GFFREAD_BIN,
           translation_table=translation_table,
           id=id,
           gff=gff,
           genome=genome,
           prefix=prefix,
           out_dir=out_dir)
    )

    join_task.set_upstream(*tasks)

    return tasks, join_task, os.path.join(out_dir, "%s.protein.fasta" % prefix), os.path.join(out_dir, "%s.genome.gff3" % prefix) 


def create_busco_task(protein, prefix, busco_database, thread, job_type,
                      work_dir="", out_dir=""):

    task = Task(
        id="busco",
        work_dir=work_dir,
        type=job_type,
        option="-pe smp %s %s" % (thread, QUEUE),
        script="""
export PATH="{busco_bin}:{augustus_bin}:$PATH"
export AUGUSTUS_CONFIG_PATH="{augustus_config}"
export BUSCO_CONFIG_FILE="{busco_config}"
busco --cpu {thread} --mode proteins -force --lineage_dataset {busco_database} \\
  --offline --in {protein} --out {prefix}
""".format(gffread=GFFREAD_BIN,
            busco_bin=BUSCO_BIN,
            augustus_bin=AUGUSTUS_BIN,
            augustus_config=AUGUSTUS_CONFIG_PATH,
            busco_config=BUSCO_CONFIG_FILE,
            busco_database=busco_database,
            protein=protein,
            prefix=prefix,
            thread=thread,
            out_dir=out_dir)
    )

    join_task = Task(
        id="stat_busco",
        work_dir=work_dir,
        type="local",
        option="-pe smp 1 %s" % QUEUE,
        script="""
python {scripts}/plot_busco.py -i {prefix}/short_summary.*.txt -o {prefix}
cp {prefix}.busco.pdf  {prefix}.busco.png  {prefix}.busco.tsv {out_dir}
rm -rf {prefix}.BUSCO
""".format(scripts=SCRIPTS,
           prefix=prefix,
           out_dir=out_dir)
    )

    join_task.set_upstream(task)

    return task, join_task


def run_ransposonPSI(genome, prefix, gff, translation_table=1, kingdom="eukaryota", busco_database="", job_type="sge",
                     work_dir="", out_dir="", concurrent=10, refresh=10):

    work_dir = mkdir(work_dir)
    out_dir = mkdir(out_dir)
    gff = check_path(gff)
    genome = check_path(genome)
 
    if busco_database == "no_busco":
        busco_database = ""
    elif busco_database:
        try:
            try:
                temp = check_path("%s_odb12/refseq_db.faa" % busco_database) #输入路径
            except:
                temp = check_path("%s_odb12/refseq_db.faa.gz" % busco_database) #输入路径
        except:
            try:
                temp = check_path("%s/%s_odb12/refseq_db.faa" % (BUSCO_DB, busco_database)) #输入分类名称
            except:
                temp = check_path("%s/%s_odb12/refseq_db.faa.gz" % (BUSCO_DB, busco_database)) #输入分类名称
            busco_database = "%s/%s" % (BUSCO_DB, busco_database)
    else:
        if kingdom == "plant":
            kingdom = "viridiplantae"
        temp = check_path("%s/%s_odb12/refseq_db.faa" % (BUSCO_DB, kingdom))
        busco_database = "%s/%s" % (BUSCO_DB, kingdom)   

    proteins = split_protein(genome=genome,
        prefix=prefix,
        gff=gff,
        job_type=job_type,
        work_dir=os.path.join(work_dir, "split"),
        size="400kb"   #ransposonPSI比较慢，需要耗时比对打，需要切分碎一点,默认200kb
    )

    dag = DAG("ransposonPSI")
    PSI_tasks, PSI_join_task, protein, gff = create_ransposonPSI_tasks(proteins=proteins,
        prefix=prefix,
        gff=gff,
        genome=genome,
        job_type=job_type,
        work_dir=work_dir,
        out_dir=out_dir
    )

    dag.add_task(*PSI_tasks)
    dag.add_task(PSI_join_task)

    if busco_database:
        busco_task, busco_join_task = create_busco_task(
            protein=protein,
            prefix=prefix,
            thread=8,
            job_type=job_type,
            work_dir=work_dir,
            out_dir=out_dir,
            busco_database=busco_database
        )
        dag.add_task(busco_task)
        dag.add_task(busco_join_task)
        busco_task.set_upstream(PSI_join_task)

    do_dag(dag, concurrent, refresh)

    return protein, gff


def ransposonPSI(args):

    protein, gff = run_ransposonPSI(genome=args.genome,
        prefix=args.prefix,
        gff=args.gff3,
        translation_table=args.translation_table,
        kingdom=args.kingdom,
        busco_database=args.busco_database,
        job_type=args.job_type,
        work_dir=args.work_dir,
        out_dir=args.out_dir,
        concurrent=args.concurrent,
        refresh=args.refresh
    )
   
    return 0


def add_ransposonPSI_args(parser):

    parser.add_argument("genome", metavar="FILE", type=str,
        help="Input genome file(genome.fasta).") 
    parser.add_argument("-p", "--prefix", metavar="STR", type=str, default="PEGS",
        help="Set File Prefix, default=PEGS.")
    parser.add_argument("-gff", "--gff3", metavar="FILE", type=str, required=True,
        help="Input genome annotation gff file(genome.gff)")
    parser.add_argument("--translation_table", metavar="INT", type=int, default=1,
        help="Set genetic code(https://www.ncbi.nlm.nih.gov/Taxonomy/taxonomyhome.html/index.cgi?chapter=cgencodes), default=1")
    parser.add_argument("-k", "--kingdom", metavar="STR", type=str, default="fungi",
        choices=["fungi", "plant", "animal", "eukaryota", "actinopterygii"],
        help="Kingdom of the sample (fungi, plant, animal, eukaryota, actinopterygii), default=fungi")
    parser.add_argument("-db", "--busco_database", metavar="STR", type=str, default="",
        help="Set up Busco database,default=fungi")
    parser.add_argument("-c", "--concurrent", metavar="INT", type=int, default=10,
        help="Maximum number of jobs concurrent  (default: 10)")
    parser.add_argument("-r", "--refresh", metavar="INT", type=int, default=30,
        help="Refresh time of log in seconds  (default: 30)")
    parser.add_argument("-j", "--job_type", choices=["sge", "local"], default="local",
        help="Jobs run on [sge, local]  (default: local)")
    parser.add_argument("-w", "--work_dir", metavar="DIR", type=str, default="work",
        help="Work directory (default: work)")
    parser.add_argument("-o", "--out_dir", metavar="DIR", type=str, default="out",
        help="Output directory (default: out)")

    return parser


def main():

    logging.basicConfig(
        stream=sys.stderr,
        level=logging.INFO,
        format="[%(levelname)s] %(message)s"
    )

    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="""
name:
    ransposonPSI: ransposonPSI involves a PSI-blast search of a protein or nucleotide sequence against a set of profiles of proteins corresponding to major clades/families of transposon Open Reading Frames.
   
version: %s
contact:  %s <%s>\
    """ % (__version__, " ".join(__author__), __email__))

    parser = add_ransposonPSI_args(parser)
    args = parser.parse_args()
    ransposonPSI(args)


if __name__ == "__main__":
    main()
