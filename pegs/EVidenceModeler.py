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
__version__ = "v1.2.2"


CONFIG = """\
PROTEIN\tMetaEuk\t2
PROTEIN\tminiprot\t2
PROTEIN\tbusco\t10
ABINITIO_PREDICTION\tGeneMark\t1
ABINITIO_PREDICTION\tGlimmerHMM\t2
ABINITIO_PREDICTION\tAugustus\t4
TRANSCRIPT\tassembler-pasa.sqlite\t12
OTHER_PREDICTION\ttransdecoder\t8
OTHER_PREDICTION\tGMST\t8
OTHER_PREDICTION\tEviAnn\t10
"""

#CONFIG = """\
"""
PROTEIN\tMetaEuk\t1
PROTEIN\tminiprot\t1
PROTEIN\tbusco\t5
ABINITIO_PREDICTION\tGeneMark\t1
ABINITIO_PREDICTION\tGlimmerHMM\t1
ABINITIO_PREDICTION\tAugustus\t8
TRANSCRIPT\tassembler-pasa.sqlite\t12
OTHER_PREDICTION\ttransdecoder\t5
OTHER_PREDICTION\tGMST\t5
OTHER_PREDICTION\tEviAnn\t10
"""


def create_evm_task(genome, predict_gffs, protein_gffs, transcript_gffs, pasa_gffs,
                    prefix, job_type="local", work_dir="", out_dir="",
                    kingdom="fungi", thread=10):

    weight = os.path.join(work_dir, "weights.txt")
    with open(weight, "w") as fh:
        fh.write(CONFIG)

    #不对序列进行拆分运行
    x = ""
    if kingdom=="fungi":
        x = "--min_intron_length 0 --terminal_intergenic_re_search 500"
        
    tran1 = ""
    if transcript_gffs:
        tran1 = "cat {transcript_gffs} >> predict.gff".format(" ".join(transcript_gffs=transcript_gffs))

    pasa1 = ""
    pasa2 = ""
    if pasa_gffs: #参考官网的输入，第9列要有需要ID和Target信息
        pasa1 = 'cat {pasa_gffs} |sed "s/cDNA_match/EST_match/g" >transcript_alignments.gff3'.format(pasa_gffs=" ".join(pasa_gffs))
        pasa2 = "--transcript_alignments transcript_alignments.gff3"
    protein1 = ""
    protein2 = ""
    if protein_gffs: #参考官网的输入，第9列要有需要ID和Target信息
        protein1 = 'cat {protein_gffs} |grep "CDS"|sed "s/Target_ID=/Target=/g" > alignment.gff'.format(protein_gffs=" ".join(protein_gffs))
        protein2 = "--protein_alignments alignment.gff"

    task = Task(
        id="evm",
        work_dir=work_dir,
        type=job_type,
        option="-pe smp %s %s" % (thread, QUEUE),
        script="""
cat {genome} |cut -f 1 > genome.fasta
cat {predict_gffs} > predict.gff
{protein1}
{tran1}
{pasa1}
export PATH={evm_bin}:$PATH
if [ ! -f "{prefix}.EVM.gff3" ]; then
  EVidenceModeler --sample_id {prefix} --genome genome.fasta\\
    --weights {weight} --gene_predictions predict.gff \\
    {protein2} {pasa2} --segmentSize 100000\\
    --overlapSize 10000 --CPU {thread} {x}
else
  touch evm_done
fi
cp {prefix}.EVM.gff3 {out_dir}/{prefix}.evm.gff3
rm -rf genome.fasta {prefix}.partitions __{prefix}-EVM_chckpts
""".format(evm_bin=EVM_BIN,
            genome=genome,
            predict_gffs=" ".join(predict_gffs),
            weight=weight,
            protein1=protein1,
            protein2=protein2,
            tran1=tran1,
            pasa1=pasa1,
            pasa2=pasa2,
            x=x,
            prefix=prefix,
            thread=thread,
            out_dir=out_dir)
    )

    return task, os.path.join(out_dir, "%s.evm.gff3" % prefix)


def create_evm_tasks(genome, predict_gffs, protein_gffs, transcript_gffs, pasa_gffs,
                     prefix, job_type="local", work_dir="", out_dir="", kingdom="fungi", thread=10):

    weight = os.path.join(work_dir, "weights.txt")
    with open(weight, "w") as fh:
        fh.write(CONFIG)

    x = ""
    if kingdom=="fungi":
        x = "--min_intron_length 0 --terminal_intergenic_re_search 500"
 
    tran1 = ""
    if transcript_gffs:
        tran1 = "cat {transcript_gffs} >> predict.gff".format(transcript_gffs=" ".join(transcript_gffs))

    pasa1 = ""
    pasa2 = ""
    if pasa_gffs:
        pasa1 = 'cat {pasa_gffs} |sed "s/cDNA_match/EST_match/g" >transcript_alignments.gff3'.format(pasa_gffs=" ".join(pasa_gffs))
        pasa2 = "--transcript_alignments transcript_alignments.gff3"
    protein1 = ""
    protein2 = ""        
    if protein_gffs:
        protein1 = 'cat {protein_gffs} |grep "CDS"|sed "s/Target_ID=/Target=/g" > alignment.gff'.format(protein_gffs=" ".join(protein_gffs))
        protein2 = "--protein_alignments alignment.gff"

    task = Task(
        id="evm",
        work_dir=work_dir,
        type="local",
        option="-pe smp 1 %s" % QUEUE,
        script="""
cat {genome} |cut -f 1 > genome.fasta
cat {predict_gffs} > predict.gff
{protein1}
{tran1}
{pasa1}
export PATH={evm_bin}:$PATH
perl {evm_bin}/EvmUtils/partition_EVM_inputs.pl --genome genome.fasta \\
  --partition_dir {work_dir}/evm \\
  --gene_predictions predict.gff {protein2} {pasa2} \\
  --segmentSize 10000000 --overlapSize 200000 --partition_listing partitions.list
perl {evm_bin}/EvmUtils/write_EVM_commands.pl --genome genome.fasta {pasa2} \\
  --weights {weight} {x} \\
  --gene_predictions predict.gff {protein2} \\
  --output_file_name {prefix}.evm --partitions partitions.list > commands.list
python {root}/pegs/daglist.py commands.list --work_dir shell --prefix evm --job_type {job_type} \\
  --env_path {evm_bin}
perl {evm_bin}/EvmUtils/recombine_EVM_partial_outputs.pl --partitions partitions.list --output_file_name {prefix}.evm
perl {evm_bin}/EvmUtils/convert_EVM_outputs_to_GFF3.pl --partitions partitions.list --output {prefix}.evm \\
   --genome genome.fasta
cat evm/*/*.evm.gff3 > {prefix}.evm.gff3
cp {prefix}.evm.gff3 {out_dir}
rm -rf evm shell genome.fasta
""".format(evm_bin=EVM_BIN,
           root=ROOT,
           genome=genome,
           predict_gffs=" ".join(predict_gffs),
           weight=weight,
           x=x,
           protein1=protein1,
           protein2=protein2,
           tran1=tran1,
           pasa1=pasa1,
           pasa2=pasa2,
           prefix=prefix,
           job_type=job_type,
           work_dir=work_dir,
           out_dir=out_dir)
    )

    return task, os.path.join(out_dir, "%s.evm.gff3" % prefix)


def create_busco_task(genome, gff, prefix, busco_database="", thread=10, job_type="sge",
                      work_dir="", out_dir=""):

    task = Task(
        id="evm_busco",
        work_dir=work_dir,
        type=job_type,
        option="-pe smp %s %s" % (thread, QUEUE),
        script="""
{gffread}/gffread {gff} -g {genome} -y {prefix}.protein.fasta
export PATH="{busco_bin}:{augustus_bin}:$PATH"
export AUGUSTUS_CONFIG_PATH="{augustus_config}"
export BUSCO_CONFIG_FILE="{busco_config}"
busco --cpu {thread} --mode proteins -force --lineage_dataset {busco_database} \\
  --offline --in {prefix}.protein.fasta --out {prefix}
cat {prefix}/short_summary.specific.*.txt > {prefix}.evm_busco.tsv
rm -rf busco_downloads {prefix}.protein.fasta
""".format(gffread=GFFREAD_BIN,
            busco_bin=BUSCO_BIN,
            augustus_bin=AUGUSTUS_BIN,
            augustus_config=AUGUSTUS_CONFIG_PATH,
            busco_config=BUSCO_CONFIG_FILE,
            busco_database=busco_database,
            gff=gff,
            genome=genome,
            prefix=prefix,
            thread=thread,
            out_dir=out_dir)
    )

    return task


def run_EVidenceModeler(genome, prefix, predict_gffs, protein_gffs, transcript_gffs, pasa_gffs,
                        busco_database, job_type, work_dir, out_dir, thread=10, concurrent=10,
                        refresh=30, kingdom="fungi", no_split=False):
    
    genome = check_path(genome)
    LOG.info("检测基因组文件")
    predict_gffs = check_paths(predict_gffs)
    LOG.info("检测基因重头预测结果")
    if protein_gffs:
        protein_gffs = check_paths(protein_gffs)
        LOG.info("检测基因同源注释结果")
    if transcript_gffs:
        transcript_gffs = check_paths(transcript_gffs)
        LOG.info("检测转录组辅助预测结果")
    if pasa_gffs:
        pasa_gffs = check_paths(pasa_gffs)
        LOG.info("检测PASS辅助预测结果")

    work_dir = mkdir(work_dir)
    out_dir = mkdir(out_dir)

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

    #seq_num = 0
    #LOG.info("统计基因组的序列数目")
    #for seqid, seq in read_fasta(genome):
    #    seq_num += 1

    dag = DAG("EVidenceModeler")
    #if seq_num >= 1000:
    if no_split:
        evm_task, gff = create_evm_task(
            genome=genome,
            predict_gffs=predict_gffs,
            protein_gffs=protein_gffs,
            transcript_gffs=transcript_gffs,
            pasa_gffs=pasa_gffs,
            prefix=prefix,
            job_type=job_type,
            work_dir=work_dir,
            out_dir=out_dir,
            kingdom=kingdom,
            thread=thread)
    else:
        evm_task, gff = create_evm_tasks(
            genome=genome,
            predict_gffs=predict_gffs,
            protein_gffs=protein_gffs,
            transcript_gffs=transcript_gffs,
            pasa_gffs=pasa_gffs,
            prefix=prefix,
            job_type=job_type,
            work_dir=work_dir,
            out_dir=out_dir,
            kingdom=kingdom,
            thread=thread)

    dag.add_task(evm_task)
    busco_task = create_busco_task(
        genome=genome,
        gff=gff,
        prefix=prefix,
        thread=thread,
        job_type=job_type,
        work_dir=work_dir,
        out_dir=out_dir,
        busco_database=busco_database)
    dag.add_task(busco_task)
    busco_task.set_upstream(evm_task)

    do_dag(dag, concurrent_tasks=concurrent, refresh_time=refresh)

    return 0


def add_hlep_args(parser):

    parser.add_argument("genome", metavar="STR", type=str,
        help="Input genome file(fasta).")
    parser.add_argument("-dgff", "--predict_gffs", nargs="+", metavar="FILE", type=str, required=True,
        help="Input gene de-novo prediction results(gff).")
    parser.add_argument("-hgff", "--protein_gffs", nargs="+", metavar="FILE", type=str, required=False,
        help="Input gene homology annotation results(gff).")
    parser.add_argument("-tgff", "--transcript_gffs", nargs="+", metavar="FILE", type=str, default=False,
        help="Input transcriptome assisted annotation results(gff).")
    parser.add_argument("-pgff", "--pasa_gffs", nargs="+", metavar="FILE", type=str, default=False,
        help="Input pasa annotation results(gff).")
    parser.add_argument("--prefix", metavar="STR", type=str, default="ZXG",
        help="Input sample name.")
    parser.add_argument("-k", "--kingdom", metavar="STR", type=str, default="fungi",
        choices=["fungi", "plant", "animal", "eukaryota", "actinopterygii"],
        help="Kingdom of the sample (fungi, plant, animal, eukaryota, actinopterygii), default=fungi")
    parser.add_argument("-db", "--busco_database", metavar="STR", type=str, default="",
        help="Set up Busco database,default=fungi")
    parser.add_argument("--no_split", action="store_true", default=False,
        help="Set not to split and integrate the genome")
    parser.add_argument("-t", "--thread", metavar="INT", type=int, default=4,
        help="Set the number of threads used to run EVM, default=4")
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
    fpegs.py Rapid prediction of fungal genome genes

attention:
    fpegs.py genome.fasta -p homo.protein.fasta --prefix name
    fpegs.py *genome.fasta -p homo.protein.fasta --prefix name

version: %s
contact:  %s <%s>\
        ''' % (__version__, ' '.join(__author__), __email__))

    args = add_hlep_args(parser).parse_args()

    run_EVidenceModeler(genome=args.genome, prefix=args.prefix, predict_gffs=args.predict_gffs,
                        protein_gffs=args.protein_gffs, transcript_gffs=args.transcript_gffs,
                        pasa_gffs=args.pasa_gffs, busco_database=args.busco_database,
                        job_type=args.job_type, work_dir=args.work_dir,
                        out_dir=args.out_dir, thread=args.thread, concurrent=args.concurrent,
                        refresh=args.refresh, kingdom=args.kingdom, no_split=args.no_split)


if __name__ == "__main__":

    main()
