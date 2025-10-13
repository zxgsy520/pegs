#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import json
import glob
import logging
import argparse

from collections import OrderedDict
from dagflow import DAG, Task, ParallelTask, do_dag
from pegs.config import *
from pegs.common import check_paths, mkdir, check_path, get_version, read_files
from pegs.homo_ann import *
from pegs.de_novo_ann import *


LOG = logging.getLogger(__name__)
__author__ = ("Xingguo Zhang",)
__email__ = "invicoun@foxmail.com"
__version__ = "v1.2.1"


def create_homo_ann_task(genome, protein, cds, rna_list, prefix, work_dir, out_dir,
                         job_type="local", thread=10, concurrent=10, refresh=30):
    if cds:
        cds = "--cds %s" % cds
    if rna_list:
        rna_list = "--rna_list %s" % rna_list

    task = Task(
        id="homo_ann",
        work_dir=work_dir,
        type="local",
        option="-pe smp 1 %s" % QUEUE,
        script="""
python {root}/pegs/homo_ann.pyc {genome} --protein {protein} \\
  --prefix {prefix} {cds} {rna_list} --threads {thread} --concurrent {concurrent} \\
  --refresh {refresh} --job_type {job_type} --work_dir {work_dir} \\
  --out_dir {out_dir}
rm -rf 00_genome
""".format(root=ROOT,
           genome=genome,
           protein=protein,
           cds=cds,
           rna_list=rna_list,
           prefix=prefix,
           thread=thread,
           concurrent=concurrent,
           refresh=refresh,
           job_type=job_type,
           work_dir=work_dir,
           out_dir=out_dir)
    )

    if cds or rna_list:
        miniprot_gff = os.path.join(out_dir, "%s.eviann.gff" % prefix)
    else:
        miniprot_gff = os.path.join(out_dir, "%s.miniprot.gff" % prefix)

    return task, os.path.join(out_dir, "%s.metaeuk.gff" % prefix), miniprot_gff


def create_de_novo_ann_task(genome, homo, homogff, prefix, species,
                            job_type, work_dir, out_dir, thread=10,
                            concurrent=10, refresh=30, kingdom="fungi", minlen="50kb"):
    temp = ""
    if homo:
       temp = "--homo %s --homogff %s" % (" ".join(homo), " ".join(homogff))

    task = Task(
        id="de_novo_ann",
        work_dir=work_dir,
        type="local",
        option="-pe smp 1 %s" % QUEUE,
        script="""
python {root}/pegs/de_novo_ann.pyc {genome} --prefix {prefix} \\
  --species {species} --kingdom {kingdom} \\
  --minlen {minlen} {temp} \\
  --threads {thread} --concurrent {concurrent} --refresh {refresh} --job_type {job_type} \\
  --work_dir {work_dir} --out_dir {out_dir}
rm -rf 00_genome
""".format(root=ROOT,
           genome=genome,
           temp=temp,
           prefix=prefix,
           species=species,
           kingdom=kingdom,
           minlen=minlen,
           thread=thread,
           concurrent=concurrent,
           refresh=refresh,
           job_type=job_type,
           work_dir=work_dir,
           out_dir=out_dir)
    )

    return task, os.path.join(out_dir, "%s.genemark.gff" % prefix), os.path.join(out_dir, "%s.glimmerhmm.gff" % prefix)


def create_augustus_task(genome, introns_gff, prefix, species,
                         job_type, work_dir, out_dir, thread=10):
    temp = ""
    if introns_gff:
       temp = "--introns_gff %s" % introns_gff

    task = Task(
        id="run_augustus",
        work_dir=work_dir,
        type="local",
        option="-pe smp 1 %s" % QUEUE,
        script="""
python {root}/pegs/run_augustus.py {genome}\\
  --prefix {prefix} --species {species} {temp}\\
  --thread {thread} --job_type {job_type} --work_dir {work_dir} --out_dir {out_dir}
""".format(root=ROOT,
           genome=genome,
           temp=temp,
           prefix=prefix,
           species=species,
           thread=thread,
           job_type=job_type,
           work_dir=work_dir,
           out_dir=out_dir)
    )

    return task, os.path.join(out_dir, "%s.augustus.gff3" % prefix)


def create_evm_task(genome, predict_gffs, protein_gffs, transcripts, pasa, prefix, busco_database="", job_type="local",
                      work_dir="", out_dir="", kingdom="fungi", thread=10, no_split=False):

    temp1 = ""
    temp2 = ""
    temp3 = ""
    if len(protein_gffs) >= 1:
        temp1 = "--protein_gffs {protein_gffs}".format(protein_gffs=" ".join(protein_gffs))
    if pasa:
        temp2 = "--pasa_gffs {pasa}".format(pasa=pasa)
    if len(transcripts) >= 1:
        temp3 = "--transcript_gffs {transcript}".format(transcript=" ".join(transcripts))

    x, x1 = "", ""
    if no_split:
        x = "--no_split"
    if busco_database:
        x1 = "--busco_database %s" % busco_database

    task = Task(
        id="run_evm",
        work_dir=work_dir,
        type="local",
        option="-pe smp 1 %s" % QUEUE,
        script="""
python {root}/pegs/EVidenceModeler.py {genome} \\
  --predict_gffs {predict_gffs} {temp1} {temp2} \\
  {temp3} --prefix {prefix} --kingdom {kingdom} {x1} \\
  --thread {thread} --job_type {job_type} {x}\\
  --work_dir {work_dir} --out_dir {out_dir}
""".format(root=ROOT,
           genome=genome,
           predict_gffs=" ".join(predict_gffs),
           temp1=temp1,
           temp2=temp2,
           temp3=temp3,
           x=x,
           x1=x1,
           prefix=prefix,
           kingdom=kingdom,
           thread=thread,
           job_type=job_type,
           work_dir=work_dir,
           out_dir=out_dir)
    )

    return task, os.path.join(out_dir, "%s.evm.gff3" % prefix)


def create_PSI_task(genome, gff, prefix, kingdom, busco_database="", job_type="local",
                    work_dir="", out_dir=""):

    x = ""
    if busco_database:
        x = "--busco_database %s" % busco_database
 
    task = Task(
        id="run_PSI",
        work_dir=work_dir,
        type="local",
        option="-pe smp 1 %s" % QUEUE,
        script="""
python {root}/pegs/ransposonPSI.py {genome}\\
  --gff3 {gff} --prefix {prefix} --kingdom {kingdom} {x}\\
  --job_type {job_type} --work_dir {work_dir} --out_dir {out_dir}
""".format(root=ROOT,
           genome=genome,
           gff=gff,
           prefix=prefix,
           kingdom=kingdom,
           x=x,
           job_type=job_type,
           work_dir=work_dir,
           out_dir=out_dir)
    )

    return task


def get_gff_class(gffs, work_dir):

    gffs = glob.glob(gffs)
 
    r = {}
    for g in gffs:
        if g.endswith("augustus.gff3") or g.endswith("augustus.gff"):
            r["Denovo1"] = g
        elif g.endswith("evm.gff3") or g.endswith("evm.gff"):
            r["EVM"] = g
        elif g.endswith("genemark.gff3") or g.endswith("genemark.gff"):
            r["Denovo2"] = g
        elif g.endswith("genome.gff") or g.endswith("genome.gff3"):
            r["Finalset"] = g
        elif g.endswith("glimmerhmm.gff") or g.endswith("glimmerhmm.gff3"):
            r["Denovo3"] = g
        elif g.endswith("metaeuk.gff") or g.endswith("metaeuk.gff3"):
            r["Homology"] = g
        elif g.endswith("transcript.gff3") or g.endswith("transcript.gff"):
            r["RNA"] = g
        else:
            continue

    fo = open("%s/structure.list" % work_dir, "w")
    for i in r:
        fo.write("%s\t%s\n" % (i, r[i]))

    fo.close()
    
    return os.path.join(work_dir, "structure.list")
   

def stat_gff(prefix, gffs, work_dir="", out_dir=""):

    gff_list = get_gff_class(gffs, work_dir)

    dag = DAG("stat_gff")
    task = Task(
        id="gff_stat",
        work_dir=work_dir,
        type="local",
        option="-pe smp 1 %s" % QUEUE,
        script="""
{gff_stat} --config {gff_list} 
cp gffStat.out/gff.stat {out_dir}/{prefix}.stat_gff.tsv
rm -rf gffStat.out
""".format(gff_stat=GFFSTAT,
           prefix=prefix,
           gff_list=gff_list,
           out_dir=out_dir)
    )
    dag.add_task(task)

    do_dag(dag, concurrent_tasks=2, refresh_time=10)

    return 0


def run_fpegs(genome, masked, protein, cds, rna_list, homo, homogff, introns_gff, prefix, species,
              busco_database, job_type, work_dir, out_dir, thread=10, concurrent=10,
              refresh=30, kingdom="fungi", minlen="50kb", no_split=False):

    genome = check_path(genome)
    protein = check_path(protein)
    if introns_gff:
        introns_gff = check_path(introns_gff)
    if rna_list:
        rna_list = check_path(rna_list)
    if cds:
        cds = check_path(cds)

    work_dir = mkdir(work_dir)
    out_dir = mkdir(out_dir)
    if not species:
        species = prefix

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
            tax = "viridiplantae"
        else:
            tax = kingdom
        temp = check_path("%s/%s_odb12/refseq_db.faa" % (BUSCO_DB, tax))
        busco_database = "%s/%s" % (BUSCO_DB, tax)
    print("BUSCO数据库路径:%s" % temp)
 
    work_dict = {
        "homo": "01_homo",
        "de_novo": "02_de_novo",
        "aug": "03_augustus",
        "evm": "04_EVM",
        "psi": "05_PSI",
    }

    for k, v in work_dict.items():
        mkdir(os.path.join(work_dir, v))
    if masked:   
        masked = check_path(masked)
    else:
        masked = genome

    dag = DAG("run_fast_predict_gene")
    homo_task, metaeuk_gff, miniprot_gff = create_homo_ann_task(
        genome=genome,
        protein=protein,
        cds=cds,
        rna_list=rna_list,
        prefix=prefix,
        work_dir=os.path.join(work_dir, work_dict["homo"]),
        out_dir=out_dir,
        job_type=job_type,
        thread=thread,
        concurrent=concurrent,
        refresh=refresh
    )
    dag.add_task(homo_task)

    if homo:
        homo = check_paths(homo)
        homogff = check_paths(homogff)

    de_novo_task, gmes_gff, glimmer_gff = create_de_novo_ann_task(
        genome=genome,
        homo=homo,
        homogff=homogff,
        prefix=prefix,
        species=species,
        job_type=job_type,
        work_dir=os.path.join(work_dir, work_dict["de_novo"]),
        out_dir=out_dir,
        thread=thread,
        concurrent=concurrent,
        refresh=refresh,
        kingdom=kingdom,
        minlen=minlen
    )
    dag.add_task(de_novo_task)

    aug_task, aug_gff= create_augustus_task(
        genome=genome,
        introns_gff=introns_gff,
        prefix=prefix,
        species=species,
        job_type=job_type,
        work_dir=os.path.join(work_dir, work_dict["aug"]),
        out_dir=out_dir,
        thread=thread
    )
    dag.add_task(aug_task)

    predict_gffs = [aug_gff, gmes_gff, glimmer_gff]
    transcripts = []
    try:
        temp = check_path(os.path.join(out_dir, "%s.transcript.gff3" % prefix))
        transcripts.append(temp)
    except:
        LOG.info("No files:%s.transcript.gff3" % prefix)

    try:
        temp = check_path(os.path.join(out_dir, "%s.gmst.gff3" % prefix))
        transcripts.append(temp)
    except:
        LOG.info("No files:%s.gmst.gff3" % prefix)

    try:
        pasa = check_path(os.path.join(out_dir, "%s.pasa.gff3" % prefix))
    except:
        pasa = ""

    protein_gffs = [metaeuk_gff]
    if rna_list:
        transcripts.append(miniprot_gff)
    else:
        protein_gffs.append(miniprot_gff)

    try:
        busco_gff = check_path(os.path.join(out_dir, "%s.busco.gff" % prefix)) 
        protein_gffs.append(busco_gff)
    except:
        pasa = ""

    evm_task, evm_gff = create_evm_task(
        genome=genome,
        predict_gffs=predict_gffs,
        protein_gffs=protein_gffs, 
        transcripts=transcripts,
        pasa=pasa,
        prefix=prefix,
        job_type=job_type,
        work_dir=os.path.join(work_dir, work_dict["evm"]),
        out_dir=out_dir,
        kingdom=kingdom,
        busco_database=busco_database,
        thread=thread,
        no_split=no_split,
    )
    evm_task.set_upstream(homo_task)
    evm_task.set_upstream(de_novo_task)
    evm_task.set_upstream(aug_task)
    dag.add_task(evm_task)

    PSI_task = create_PSI_task(genome=genome,
        gff=evm_gff,
        prefix=prefix,
        kingdom=kingdom,
        busco_database=busco_database,
        job_type=job_type,
        work_dir=os.path.join(work_dir, work_dict["psi"]),
        out_dir=out_dir
    )
    PSI_task.set_upstream(evm_task)
    dag.add_task(PSI_task)

    do_dag(dag, concurrent_tasks=concurrent, refresh_time=refresh)

    stat_gff(prefix=prefix, gffs=os.path.join(out_dir, "*.gff*"), work_dir=work_dir, out_dir=out_dir)

    return 0


def add_hlep_args(parser):

    parser.add_argument("genome", metavar="STR", type=str,
        help="Input genome file(fasta).")
    parser.add_argument("-m", "--masked", metavar="FILE", type=str,  default=False,
        help="Input the repeat masked genome(fasta).")
    parser.add_argument("-p", "--protein", metavar="FILE", type=str, required=True,
        help="Input homologous species protein sequence.")
    parser.add_argument("--cds", metavar="FILE", type=str, default="",
        help="Input homologous species cds sequence(也可输入组装的转录本).")
    parser.add_argument("--rna_list", metavar="FILE", type=str, default="",
        help="Input homologous species RNA sequence(也可输入测序的转绿组数据).")
    parser.add_argument("--homo", nargs="+", metavar="FILE", type=str, default=False,
        help="Input homologous species genome sequences.")
    parser.add_argument("-hf", "--homogff", nargs="+", metavar="FILE", type=str, default=False,
        help="Input gff file for homologous species annotation.")
    parser.add_argument("-igff", "--introns_gff", metavar="FILE", type=str, default=False,
        help="Input gff file for introns(introns.f.gff)")
    parser.add_argument("--prefix", metavar="STR", type=str, default="ZXG",
        help="Input sample name.")
    parser.add_argument("-s", "--species", metavar="STR", type=str,  default=False,
        help="Input the species name of the sample.")
    parser.add_argument("-k", "--kingdom", metavar="STR", type=str, default="fungi",
        choices=["fungi", "plant", "animal", "eukaryota"],
        help="Kingdom of the sample (fungi, plant, animal, eukaryota), default=fungi")
    parser.add_argument("-db", "--busco_database", metavar="STR", type=str, default="",
        help="Specify the name of the BUSCO lineage to be used(/Work/database/busco_db/odb12/fungi). default=fungi")
    parser.add_argument("-ml", "--minlen", metavar="STR", type=str,  default="10kb",
        help="Input filtering preserves the shortest read length, default=10kb")
    parser.add_argument("--no_split", action="store_true", default=False,
        help="Set not to split and integrate the genome")
    parser.add_argument("--thread", metavar="INT", type=int, default=4,
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
    fpegs.py Rapid prediction of fungal genome genes

attention:
    fpegs.py genome.fasta -p homo.protein.fasta --prefix name
    fpegs.py *genome.fasta -p homo.protein.fasta --prefix name
#如果没有转录组数据，可以使用gffread提取近源物种的转绿本
gffread -W -y proteins.faa -w transcripts.fa -g genome.fa genome.gff  #从近源物种中提取蛋白质转绿本数据

version: %s
contact:  %s <%s>\
        """ % (__version__, " ".join(__author__), __email__))

    args = add_hlep_args(parser).parse_args()

    run_fpegs(genome=args.genome, masked=args.masked, protein=args.protein,
              cds=args.cds, rna_list=args.rna_list, homo=args.homo,
              homogff=args.homogff, introns_gff=args.introns_gff, prefix=args.prefix,
              species=args.species, busco_database=args.busco_database, thread=args.thread,
              job_type=args.job_type, work_dir=args.work_dir, out_dir=args.out_dir,
              concurrent=args.concurrent, refresh=args.refresh, kingdom=args.kingdom, 
              minlen=args.minlen, no_split=args.no_split)


if __name__ == "__main__":

    main()
