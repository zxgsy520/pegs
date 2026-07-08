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
from pegs.common import mkdir, check_path, get_uid
from pegs.ransposonPSI import create_busco_task

LOG = logging.getLogger(__name__)
__author__ = ("Xingguo Zhang",)
__email__ = "invicoun@foxmail.com"
__version__ = "v1.0.0"


PASA_CONFIG="""
#PASA admin settings
#emails sent to admin on job launch, success, and failure
PASA_ADMIN_EMAIL=invicoun@foxmail.com
#database to manage pasa jobs; required for daemon-based processing.
PASA_ADMIN_DB=PASA2_admin
DATABASE={path}/pasa.sqlite
#######################################################
# Parameters to specify to specific scripts in pipeline
# create a key = "script_name" + ":" + "parameter"
# assign a value as done above.
 
#script validate_alignments_in_db.dbi
validate_alignments_in_db.dbi:--MIN_PERCENT_ALIGNED=75
validate_alignments_in_db.dbi:--MIN_AVG_PER_ID=85
validate_alignments_in_db.dbi:--NUM_BP_PERFECT_SPLICE_BOUNDARY=0
 
#script subcluster_builder.dbi
subcluster_builder.dbi:-m=50
"""


PASA_ANNO_CONFIG="""
# PASA admin settings
#emails sent to admin on job launch, success, and failure
PASA_ADMIN_EMAIL=invicoun@foxmail.com
#database to manage pasa jobs; required for daemon-based processing.
PASA_ADMIN_DB=PASA2_admin
DATABASE={path}/pasa.sqlite
#设置的数据与前面的数据一致
#######################################################
# Parameters to specify to specific scripts in pipeline
# create a key = "script_name" + ":" + "parameter"
# assign a value as done above.
RUN_TRANS_DECODER=1
 
#script cDNA_annotation_comparer.dbi
cDNA_annotation_comparer.dbi:--MIN_PERCENT_OVERLAP=<__MIN_PERCENT_OVERLAP__>
cDNA_annotation_comparer.dbi:--MIN_PERCENT_PROT_CODING=<__MIN_PERCENT_PROT_CODING__>
cDNA_annotation_comparer.dbi:--MIN_PERID_PROT_COMPARE=<__MIN_PERID_PROT_COMPARE__>
cDNA_annotation_comparer.dbi:--MIN_PERCENT_LENGTH_FL_COMPARE=<__MIN_PERCENT_LENGTH_FL_COMPARE__>
cDNA_annotation_comparer.dbi:--MIN_PERCENT_LENGTH_NONFL_COMPARE=<__MIN_PERCENT_LENGTH_NONFL_COMPARE__>
cDNA_annotation_comparer.dbi:--MIN_FL_ORF_SIZE=<__MIN_FL_ORF_SIZE__>
cDNA_annotation_comparer.dbi:--MIN_PERCENT_ALIGN_LENGTH=<__MIN_PERCENT_ALIGN_LENGTH__>
cDNA_annotation_comparer.dbi:--MIN_PERCENT_OVERLAP_GENE_REPLACE=<__MIN_PERCENT_OVERLAP_GENE_REPLACE__>
cDNA_annotation_comparer.dbi:--STOMP_HIGH_PERCENTAGE_OVERLAPPING_GENE=<__STOMP_HIGH_PERCENTAGE_OVERLAPPING_GENE__>
cDNA_annotation_comparer.dbi:--TRUST_FL_STATUS=<__TRUST_FL_STATUS__>
cDNA_annotation_comparer.dbi:--MAX_UTR_EXONS=<__MAX_UTR_EXONS__>
cDNA_annotation_comparer.dbi:--GENETIC_CODE=<__GENETIC_CODE__>
"""

def create_add_utr_task(genome, gff, transcript, pasa_db, prefix="out",
                        thread=10, job_type="sge", work_dir="./", out_dir="./"):

    fo1 = open(os.path.join(work_dir, "pasa.config"), "w")
    fo1.write(PASA_CONFIG.format(path=work_dir))
    fo1.close()
    fo2 = open(os.path.join(work_dir, "pasa_anno.config"), "w")
    fo2.write(PASA_ANNO_CONFIG.format(path=work_dir))
    fo2.close()

    task = Task(
        id="pass_add_utr",
        work_dir=work_dir,
        type=job_type,
        option="-pe smp %s %s" % (thread, QUEUE),
        script="""
#注意，早期的gff版本需要使用gffvert转换一下格式（需要输入gff3格式）
#{bin}/gffvert sort_gff {gff} > {prefix}.gff3
#检查注释文件的格式
#{pasa_main}/misc_utilities/pasa_gff3_validator.pl {prefix}.genome_sort.gff3
export PATH={pasa}:$PATH
export PATH={pasa_main}/scripts:$PATH
export PATH={pasa_main}/bin/:$PATH
rm -rf pasa.sqlite {prefix}.gff3 #必须删除
cp {pasa_db} pasa.sqlite

#加载更新gff3文件到数据中
ln -s {gff} {prefix}.gff3
{pasa_main}/scripts/Load_Current_Gene_Annotations.dbi -c pasa.config -g {genome} -P {prefix}.gff3
#更新gff3文件
{pasa}/Launch_PASA_pipeline.pl --CPU {thread} -c pasa_anno.config -A -g {genome} -t {transcript}
cat pasa.sqlite.gene_structures_post_PASA_updates.*.gff3 |grep -v "^#" >{prefix}.temp.gff3
{bin}/gffvert sort_gff {prefix}.temp.gff3 >{prefix}.sort_temp.gff3
""".format(pasa=PASA_BIN,
            pasa_main=PASA_MAIN,
            bin=BIN,
            pasa_db=pasa_db,
            prefix=prefix,
            genome=genome,
            gff=gff,
            transcript=transcript,
            thread=thread,
            out_dir=out_dir
        )
    )

    return task, os.path.join(work_dir, "%s.sort_temp.gff3" % prefix)
 

def create_agat_task(gff, genome, translation_table=1, prefix="out", work_dir="./", out_dir="./"):

    task = Task(
        id="agat",
        work_dir=work_dir,
        type="local",
        option="-pe smp 1",
        script="""
docker run --rm -v {map_path} -w {work_dir} \\
  -u {userid} {agat_docker} agat_sp_keep_longest_isoform.pl \\
  --gff {gff} --output {prefix}.genome.gff3
{gffread}/gffread {prefix}.genome.gff3 -g {genome} -x {prefix}.CDS.fasta
{bin}/gffvert cds2aa {prefix}.CDS.fasta --transl_table {translation_table} --force >{prefix}.protein.fasta

{bin}/gffvert get_seq {prefix}.genome.gff3 --genome {genome} --dtype gene >{prefix}.gene.fasta
cut -d "." -f 1 {prefix}.protein.fasta >{out_dir}/{prefix}.protein.fasta
cut -d "." -f 1 {prefix}.CDS.fasta >{out_dir}/{prefix}.CDS.fasta
cp {prefix}.genome.gff3 {prefix}.gene.fasta {out_dir}
""".format(agat_docker=AGAT_DOCKRE,
             map_path=MAP_PATH,
             userid=get_uid(),
             gffread=GFFREAD_BIN,
             bin=BIN,
             genome=genome,
             prefix=prefix,
             gff=gff,
             translation_table=translation_table, 
             work_dir=work_dir,
             out_dir=out_dir
        )
    )

    return task, os.path.join(out_dir, "%s.protein.fasta" % prefix), os.path.join(out_dir, "%s.genome.gff3" % prefix)


def run_add_utr(genome, gff, transcript, pasa_db, translation_table=1, prefix="out", busco_database="", thread=10, job_type="sge",
                work_dir="./", out_dir="./", concurrent=10, refresh=15):

    work_dir = mkdir(work_dir)
    out_dir = mkdir(out_dir)
    gff = check_path(gff)
    genome = check_path(genome)
    transcript = check_path(transcript)
    pasa_db = check_path(pasa_db)

    if busco_database:
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

    dag = DAG("run_add_utr")
    add_utr_task, gff = create_add_utr_task(genome=genome,
        gff=gff,
        transcript=transcript,
        pasa_db=pasa_db,
        prefix=prefix,
        thread=thread,
        job_type=job_type,
        work_dir=work_dir,
        out_dir=out_dir)
    dag.add_task(add_utr_task)

    agat_task, protein, gff = create_agat_task(gff=gff,
        genome=genome,
        translation_table=translation_table,
        prefix=prefix,
        work_dir=work_dir,
        out_dir=out_dir)
    dag.add_task(agat_task)
    agat_task.set_upstream(add_utr_task)

    busco_task, busco_join_task = create_busco_task(
        protein=protein,
        prefix=prefix,
        thread=thread,
        job_type=job_type,
        work_dir=work_dir,
        out_dir=out_dir,
        busco_database=busco_database
    )
    dag.add_task(busco_task)
    dag.add_task(busco_join_task)
    busco_task.set_upstream(agat_task)

    do_dag(dag, concurrent_tasks=concurrent, refresh_time=refresh)

    return 0


def add_hlep_args(parser):

    parser.add_argument("genome", metavar="STR", type=str,
        help="Input genome file(fata).")
    parser.add_argument("-gff", "--gff", metavar="FILE", type=str, required=True,
        help="Input genome annotation file(genome.gff3)")
    parser.add_argument("-tran", "--transcript", metavar="FILE", type=str, required=True,
        help="Input transcriptome assembly results(transcript.fasta).")
    parser.add_argument("-pdb", "--pasa_db", metavar="FILE", type=str, required=True,
        help="Input pasa DB(pasa.sqlite).")
    parser.add_argument("--translation_table", metavar="INT", type=int, default=1,
        help="Set genetic code(https://www.ncbi.nlm.nih.gov/Taxonomy/taxonomyhome.html/index.cgi?chapter=cgencodes), default=1")
    parser.add_argument("-p", "--prefix", metavar="FILE", type=str, default="out",
        help="Input sample name, default=out")
    parser.add_argument("-db", "--busco_database", metavar="STR", type=str, default="",
        help="Set up Busco database,default=fungi")
    parser.add_argument("-t", "--thread", metavar="INT", type=int, default=4,
        help="Threads used to run stra (default: 4)")
    parser.add_argument("--concurrent", metavar="INT", type=int, default=10,
        help="Maximum number of jobs concurrent  (default: 10)")
    parser.add_argument("--refresh", metavar="INT", type=int, default=30,
        help="Refresh time of log in seconds  (default: 30)")
    parser.add_argument("--job_type", choices=["sge", "local"], default="local",
        help="Jobs run on [sge, local]  (default: local)")
    parser.add_argument("-w", "--work_dir", metavar="DIR", type=str, default="work",
        help="Work directory (default: current directory)")
    parser.add_argument("--out_dir", metavar="DIR", default="NPGAP.out",
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
    add_utr.py Add UTR information to genome annotation information

attention:
    add_utr.py genome.fasta --gff genome.gff3 --transcript transcript.fasta

version: %s
contact:  %s <%s>\
        """ % (__version__, " ".join(__author__), __email__))

    args = add_hlep_args(parser).parse_args()

    run_add_utr(genome=args.genome, gff=args.gff, transcript=args.transcript, pasa_db=args.pasa_db,
                translation_table=args.translation_table,  prefix=args.prefix, busco_database=args.busco_database,
                thread=args.thread, job_type=args.job_type, work_dir=args.work_dir, out_dir=args.out_dir,
                concurrent=args.concurrent, refresh=args.refresh)


if __name__ == "__main__":

    main()
