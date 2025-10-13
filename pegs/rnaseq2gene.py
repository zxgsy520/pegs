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
from pegs.common import check_paths, mkdir, check_path, read_tsv

LOG = logging.getLogger(__name__)
__author__ = ("Xingguo Zhang",)
__email__ = "invicoun@foxmail.com"
__version__ = "v1.0.0"

PYTHON_BIN ="/Work/pipeline/software/Base/miniconda/v4.10.3/bin/"
FASTP_BIN = "/Work/pipeline/software/Base/fastp/v0.23.1/"
STAR_BIN = "/Work/pipeline/software/RNAseq/STAR-2.7.9a/bin/Linux_x86_64/"
SAMTOOLS_BIN = "/Work/pipeline/software/Base/samtools/samtools/"
AUGUSTUS_BIN = "/Work/pipeline/software/Base/augustus/v3.4.0/bin/"
CDHIT_BIN = "/Work/pipeline/software/meta/cdhit/v4.8.1/"
TRANSDECODER_BIN = "/Work/pipeline/software/Base/TransDecoder/v5.7.1/bin/"
DIAMOND = "/Work/pipeline/software/Base/diamond/v2.0.3/diamond"
UNIPROT_DB = "/Work/database/SwissProt/202502/swissprot.dmnd"
UNIVEC = "/Work/database/UniVec/UniVec_Core.fasta,/Work/database/UniVec/UniVec.fasta"
MINIMAP_BIN = "/Work/pipeline/software/Base/minimap2/"
GMAP_BIN = "/Work/pipeline/software/Base/gmap/lastest/bin/"#tgs rna
PASA_BIN = "/Work/pipeline/software/Base/pasa/v2.5.2/bin/"
GMST_BIN = "/Work/pipeline/software/meta/gmst/v1.0.0/"
BLAST_BIN = "/Work/pipeline/software/Base/blast+/bin/"

PASA_CONFIG = """
# PASA admin settings
#emails sent to admin on job launch, success, and failure
PASA_ADMIN_EMAIL=invicoun@foxmail.com
#database to manage pasa jobs; required for daemon-based processing.
PASA_ADMIN_DB=PASA2_admin
DATABASE={work_dir}/pasa.sqlite
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

def create_star_index_task(genome, job_type, work_dir, thread=4):

    """输入基因组构建数据库"""
    task = Task(
        id="star_index",
        work_dir=work_dir,
        type=job_type,
        option="-pe smp %s %s" % (thread, QUEUE),
        script="""
export PATH={star}:$PATH
{bin}/biotool filter_seq {genome} --minlen 2kb >filter.genome.fasta
STAR --runMode genomeGenerate --genomeDir {work_dir}/index \\
  --genomeFastaFiles filter.genome.fasta --runThreadN {thread}
""".format(star=STAR_BIN,
            bin=BIN,
            genome=genome,
            thread=thread,
            work_dir=work_dir
        )
    )

    return task, os.path.join(work_dir, "index")


def create_ngs_task(read1, read2, prefix="out", trim=3, thread=10, job_type="sge", work_dir="", out_dir=""):

    task = Task(
        id="ngs_qc_%s" % prefix,
        work_dir=work_dir,
        type=job_type,
        option="-pe smp %s" % thread,
        script="""
export PATH={fastp}:{python_bin}:$PATH
fastp --thread {thread} -i {read1} -o {prefix}.clean_R1.fq.gz \\
  -I {read2} -O {prefix}.clean_R2.fq.gz \\
  -w {thread} -n 0 -f {trim} -F {trim} -t {trim} -T {trim} -q 20 --json {prefix}_fastp.json
python {scripts}/stat_fastp_json.py {prefix}_fastp.json >{prefix}.ngs_qc.xls
rm -rf {prefix}_fastp.json {prefix}_fastp.html
#cp {prefix}.ngs_qc.xls {out_dir}
""".format(fastp=FASTP_BIN,
            python_bin=PYTHON_BIN,
            scripts=SCRIPTS,
            read1=read1,
            read2=read2,
            prefix=prefix,
            thread=thread,
            trim=trim,
            out_dir=out_dir)
    )
    clean1 = os.path.join(work_dir, "%s.clean_R1.fq.gz" % prefix)
    clean2 = os.path.join(work_dir, "%s.clean_R2.fq.gz" % prefix)

    return task, clean1, clean2


def create_star_task(read1, read2, index, prefix="out", job_type="sge", work_dir="", thread=8):

    task = Task(
        id="star_%s" % prefix,
        work_dir=work_dir,
        type=job_type,
        option="-pe smp %s" % thread,
        script="""
export PATH={star}:$PATH
STAR --runThreadN {thread} --genomeDir {index} \\
  --readFilesIn {read1} {read2}\\
  --readFilesCommand zcat --outWigType bedGraph --outSAMtype BAM SortedByCoordinate --outSAMstrandField intronMotif
export PATH={samtools}:$PATH
samtools view --threads {thread} -Sb Aligned.sortedByCoord.out.bam | samtools sort --threads 10 -m 4G -o {prefix}.sorted.bam
export PATH={stringtie}:$PATH
stringtie -o {prefix}.stringtie.gtf -p {thread} {prefix}.sorted.bam
rm -rf Aligned.sortedByCoord.out.bam Signal.Unique*
""".format(star=STAR_BIN,
            samtools=SAMTOOLS_BIN,
            stringtie=STRINGTIE_BIN,
            thread=thread,
            index=index,
            read1=read1,
            read2=read2,
            prefix=prefix
        )
    )

    return task, os.path.join(work_dir, "%s.sorted.bam" % prefix), os.path.join(work_dir, "%s.stringtie.gtf" % prefix)


def create_merge_bam_task(bams, prefix="out", job_type="sge", work_dir="", thread=8):

    task = Task(
        id="merge_bam",
        work_dir=work_dir,
        type=job_type,
        option="-pe smp %s" % thread,
        script="""
export PATH=/Work/pipeline/software/Base/samtools/samtools/:$PATH
samtools merge --threads {thread} -h {bam1} {prefix}.merge.bam {bams}
samtools sort --threads {thread} -m 4G -n {prefix}.merge.bam -o {prefix}.sorted.bam
rm -rf {prefix}.merge.bam */*.sorted.bam
""".format(samtools=SAMTOOLS_BIN,
            thread=thread,
            bam1=bams[0],
            bams=" ".join(bams),
            prefix=prefix
        )
    )

    return task, os.path.join(work_dir, "%s.sorted.bam" % prefix)


def create_rna2intron_task(bam, genome, prefix="out", job_type="sge", work_dir="", out_dir="", thread=2):

    task = Task(
        id="rna2intron",
        work_dir=work_dir,
        type=job_type,
        option="-pe smp %s" % thread,
        script="""
export PATH={augustus}:$PATH
filterBam --uniq --in {bam} --out {prefix}.clean.bam
bam2hints --intronsonly --in={prefix}.clean.bam --out={prefix}.introns.gff
perl {scripts}/filterIntronsFindStrand.pl {genome} {prefix}.introns.gff --score > {prefix}.introns.f.gff
cp {prefix}.introns.f.gff {out_dir}
rm {prefix}.clean.bam
""".format(augustus=AUGUSTUS_BIN,
            scripts=SCRIPTS,
            thread=thread,
            bam=bam,
            genome=genome,
            prefix=prefix,
            out_dir=out_dir
        )
    )

    return task, os.path.join(work_dir, "%s.introns.f.gff" % prefix)


def create_stringtie_task(genome, stringtie_gtfs, prefix="out", job_type="sge", work_dir="./", thread=4):

    task = Task(
        id="stringtie",
        work_dir=work_dir,
        type=job_type,
        option="-pe smp %s %s" % (thread, QUEUE),
        script="""
export PATH={stringtie}:{gffread}:$PATH
ls {stringtie_gtfs} > gtf.list
stringtie --merge -p {thread} -o {prefix}.merged.gtf gtf.list
gffread -w {prefix}.transcript.fasta -g {genome} {prefix}.merged.gtf
{cdhit}/cd-hit-est -i {prefix}.transcript.fasta -o {prefix}.unitranscript.fasta -c 0.98b -d 0 -T {thread} -M 64000
""".format(stringtie=STRINGTIE_BIN,
            gffread=GFFREAD_BIN,
            cdhit=CDHIT_BIN,
            genome=genome,
            stringtie_gtfs=stringtie_gtfs,
            prefix=prefix,
            thread=thread,
            work_dir=work_dir
        )
    )

    return task, os.path.join(work_dir, "%s.unitranscript.fasta" % prefix),  os.path.join(work_dir, "%s.merged.gtf" % prefix)


def create_transdecoder_task(rna, gtf, prefix="", thread=10, job_type="sge", work_dir="", out_dir=""):

    task = Task(
        id="transdecoder",
        work_dir=work_dir,
        type=job_type,
        option="-pe smp %s %s" % (thread, QUEUE),
        script="""
export PATH={TransDecoder}:$PATH
perl {TransDecoder}/../opt/transdecoder/util/gtf_to_alignment_gff3.pl {gtf} > {prefix}.stringtie.gff
TransDecoder.LongOrfs -t {rna} --output_dir ./temp 
cp temp/*.transdecoder_dir/longest_orfs.* .
{diamond} blastp --query longest_orfs.pep --db {uniprot_db} \\
  --outfmt 6 --max-target-seqs 1 --evalue 1e-05 --threads {thread} --out {prefix}.RNA_result.m6
TransDecoder.Predict -t {rna} --retain_blastp_hits {prefix}.RNA_result.m6 --output_dir ./temp
cat temp/*.transdecoder.gff3 >{prefix}.temp.gff3
perl {TransDecoder}/../opt/transdecoder/util/cdna_alignment_orf_to_genome_orf.pl {prefix}.temp.gff3 \\
  {prefix}.stringtie.gff {rna} > {prefix}.transdecoder.gff3
cp {prefix}.transdecoder.gff3 {out_dir}
rm -rf temp
""".format(TransDecoder=TRANSDECODER_BIN,
            diamond=DIAMOND,
            uniprot_db=UNIPROT_DB,
            rna=rna,
            gtf=gtf,
            prefix=prefix,
            thread=thread,
            out_dir=out_dir
        )
    )

    return task


def create_seqclean_task(rna_ngs, rna_tgs, database, job_type="sge", work_dir="./", thread=4):

    if rna_tgs:
        temp = """
python {script}/rename_id.py {rna_tgs}  -p transngs >trans.rename_tgs.fasta
seqclean trans.rename_tgs.fasta  -c {thread} -v {database}
less trans.rename_tgs.fasta.clean | grep \'>\'|sed \'s/>//g\'|awk \'{{print $1}}\' >tgs.acc
""".format(script=SCRIPTS, rna_tgs=rna_tgs, thread=thread, database=UNIVEC)
        rnatgs = os.path.join(work_dir, "tgs.acc")
    else:
        temp = ""
        rnatgs = ""

    task = Task(
        id="seqclean",
        work_dir=work_dir,
        type=job_type,
        option="-pe smp %s %s" % (thread, QUEUE),
        script="""
export PATH={seqclean}:$PATH
python {script}/rename_id.py {rna_ngs} -p transngs >trans.rename.fasta
seqclean trans.rename.fasta  -c {thread} -v {database}
{temp}
""".format(seqclean=SEQCLEAN_BIN,
            database=UNIVEC,
            script=SCRIPTS,
            rna_ngs=rna_ngs,
            rna_tgs=rna_tgs,
            thread=thread,
            temp=temp
        )
    )
    rnaseq = os.path.join(work_dir, "trans.rename.fasta")
    clean_rnaseq = os.path.join(work_dir, "trans.rename.fasta.clean")

    return task, rnaseq, clean_rnaseq, rnatgs


def create_pasa_task(genome, rnaseq, clean_rnaseq, rnatgs, prefix="out", job_type="sge", work_dir="./", out_dir="./",
                     thread=4):

    if rnatgs:
        temp = "-f %s" % rnatgs
    else:
        temp = ""
    fo = open(os.path.join(work_dir, "pasa.config"), 'w')
    fo.write(PASA_CONFIG.format(work_dir=work_dir))
    fo.close()
    task = Task(
        id="pasa",
        work_dir=work_dir,
        type=job_type,
        option="-pe smp %s %s" % (thread, QUEUE),
        script="""
export PATH={pasa}:$PATH
#export PATH={pasa}:{gmap}:{minimap2}:{samtools}:$PATH #用conda安装的自带配套的软件
rm -rf pasa.sqlite #必须删除
cut -f 1 {genome} >genome.fasta
{pasa}/Launch_PASA_pipeline.pl -c pasa.config \\
  -C -R -g genome.fasta -T\\
  -u {rnaseq} \\
  -t {clean_rnaseq} \\
  {temp} --CPU {thread} --ALIGNERS minimap2 #gmap大基因不能使用gmap
python {script}/rename_pasa_gtf.py pasa.sqlite.pasa_assemblies.gtf >pasa_assemblies.rename.gtf
cp pasa.sqlite.pasa_assemblies.gff3 {out_dir}/{prefix}.pasa.gff3
""".format(pasa=PASA_BIN,
            gmap=GMAP_BIN,
            minimap2=MINIMAP_BIN,
            samtools=SAMTOOLS_BIN,
            script=SCRIPTS,
            prefix=prefix,
            genome=genome,
            rnaseq=rnaseq,
            clean_rnaseq=clean_rnaseq,
            temp=temp,
            thread=thread,
            out_dir=out_dir
        )
    )
    gff = os.path.join(work_dir, "%s.pasa.gff3" % prefix)
    gtf = os.path.join(work_dir, "pasa_assemblies.rename.gtf")

    return task, gff, gtf


def create_gmst_task(genome, pass_gtf, prefix="out", thread=10, job_type="sge", work_dir="", out_dir=""):

    task = Task(
        id="gmst",
        work_dir=work_dir,
        type=job_type,
        option="-pe smp %s %s" % (thread, QUEUE),
        script="""
export PATH={gffread}:$PATH
gffread {pass_gtf} -g {genome} -w {prefix}.transcript.fasta
{gmst}/gmst.pl --output {prefix}.predict.gtf --format GFF {prefix}.transcript.fasta
{bin}/gffvert gmst2gff {prefix}.predict.gtf --pasagtf {pass_gtf} --keep_longest > {prefix}.rna_long.gtf
perl {script}/GMST_GTF_to_EVM_GFF3.pl {prefix}.rna_long.gtf > {prefix}.gmst.gff3
cp {prefix}.gmst.gff3 {out_dir}

perl {script}/gtf2aa.pl {genome} {prefix}.rna_long.gtf {prefix}.rna_aa.fasta
python3 {script}/complete_aa.py {prefix}.rna_aa.fasta  > {prefix}.aa.complete.fasta
rm -rf split_blast/
export PATH={augustus}:$PATH
perl {augustus}/../scripts/aa2nonred.pl --BLAST_PATH={blast} --cores={thread} {prefix}.aa.complete.fasta {prefix}.aa.nonred.fasta  
mkdir -p ./swissport_filter
{script}/run_diamond.py --proteins {prefix}.aa.nonred.fasta --database {uniprot_db} --prefix OUT --dbname SWISS --threads {thread}
cut -f 1 OUT.SWISS.out >RNA.aa.nonred.swiss.id
python3 {script}/FilterGenomeGff_m1.py --gtf {prefix}.rna_long.gtf --idlist RNA.aa.nonred.swiss.id > {prefix}.trainset.aug.gtf
sed 's/transcript/mRNA/' {prefix}.trainset.aug.gtf > {prefix}.trainset.gli.gtf
{bin}/gffvert gmst2gff {prefix}.predict.gtf --pasagtf {pass_gtf} >{prefix}.gmst_new.gtf
""".format(gffread=GFFREAD_BIN,
            gmst=GMST_BIN,
            bin=BIN,
            augustus=AUGUSTUS_BIN,
            script=SCRIPTS,
            blast=BLAST_BIN,
            uniprot_db=UNIPROT_DB,
            genome=genome,
            pass_gtf=pass_gtf,
            prefix=prefix,
            thread=thread,
            out_dir=out_dir
        )
    )

    return task


def rnaseq2gene(genome, rnaseq, rna_tgs, prefix="out", thread=10, job_type="sge", work_dir="", out_dir="", trim=3,
                concurrent=10, refresh=15):

    work_dir = mkdir(work_dir)
    out_dir = mkdir(out_dir)
    rnaseq = check_path(rnaseq)
    genome = check_path(genome)
    if rna_tgs:
        rna_tgs = check_path(rna_tgs)

    work_dict = {
        "star": "01_Star",
        "stringtie": "02_Stringtie",
        "transdecoder": "03_Transdecoder",
        "seqclean": "04_Seqclean",
        "pasa": "05_Pasa",
        "gmst": "06_Gmst"
    }

    for k, v in work_dict.items():
        mkdir(os.path.join(work_dir, v))

    data = {}
    for line in read_tsv(rnaseq, "\t"):
        if line[0] in data:
            LOG.info("Sample duplication:%s" % line[0])
            print("Sample duplication, please merge data")
            sys.exit(1)
        data[line[0]] = [check_path(line[1]), check_path(line[2])]
    
    dag = DAG("run_star")
    index_task, index = create_star_index_task(
        genome=genome,
        job_type=job_type,
        work_dir=os.path.join(work_dir, work_dict["star"]),
        thread=thread
    )
    dag.add_task(index_task)

    bams = []
    for i in data:
        read1, read2 = data[i]
        ngs_task, clean1, clean2  = create_ngs_task(read1=read1,
            read2=read2,
            prefix=i,
            trim=trim,
            thread=thread,
            job_type=job_type,
            work_dir=os.path.join(work_dir, "%s/%s" % (work_dict["star"], i)),
            out_dir=out_dir)
        dag.add_task(ngs_task)
        
        star_task, bam, gtf = create_star_task(read1=clean1,
            read2=clean2,
            index=index,
            prefix=i,
            job_type=job_type,
            work_dir=os.path.join(work_dir, "%s/%s" % (work_dict["star"], i)),
            thread=thread)
        dag.add_task(star_task)
        index_task.set_downstream(star_task)
        ngs_task.set_downstream(star_task)
        bams.append(bam)

    do_dag(dag, concurrent_tasks=concurrent, refresh_time=refresh)

    stringtie_gtfs = os.path.join(work_dir, "%s/*/*.stringtie.gtf" % work_dict["star"])

    dag = DAG("run_pass")
    merge_bam_task, bam = create_merge_bam_task(bams=bams,
        prefix=prefix,
        job_type=job_type,
        work_dir=os.path.join(work_dir, work_dict["star"]),
        thread=thread)
    dag.add_task(merge_bam_task)

    rna2intron_task, intron_gff = create_rna2intron_task(bam=bam,
        genome=genome,
        prefix=prefix,
        job_type=job_type,
        work_dir=os.path.join(work_dir, work_dict["star"]),
        out_dir=out_dir,
        thread=2)
    dag.add_task(rna2intron_task)
    merge_bam_task.set_downstream(rna2intron_task)

    stringtie_task, rna, gtf =  create_stringtie_task(genome=genome,
        stringtie_gtfs=stringtie_gtfs,
        prefix=prefix,
        job_type=job_type,
        work_dir=os.path.join(work_dir, work_dict["stringtie"]),
        thread=thread)
    dag.add_task(stringtie_task)

    transdecoder_task = create_transdecoder_task(rna=rna,
        gtf=gtf,
        prefix=prefix,
        thread=thread,
        job_type=job_type,
        work_dir=os.path.join(work_dir, work_dict["transdecoder"]),
        out_dir=out_dir)
    dag.add_task(transdecoder_task)
    stringtie_task.set_downstream(transdecoder_task)

    seqclean_task, rnaseq, clean_rnaseq, rnatgs = create_seqclean_task(
        rna_ngs=rna,
        rna_tgs=rna_tgs,
        database=UNIVEC,
        job_type=job_type,
        work_dir=os.path.join(work_dir, work_dict["seqclean"]),
        thread=thread
    )
    dag.add_task(seqclean_task)
    seqclean_task.set_upstream(stringtie_task)

    pasa_task, gff, gtf = create_pasa_task(
        prefix=prefix,
        genome=genome,
        rnaseq=rnaseq,
        clean_rnaseq=clean_rnaseq,
        rnatgs=rnatgs,
        job_type=job_type,
        work_dir=os.path.join(work_dir, work_dict["pasa"]),
        out_dir=out_dir,
        thread=thread
    )
    dag.add_task(pasa_task)
    pasa_task.set_upstream(seqclean_task)

    gmst_task = create_gmst_task(genome=genome,
        pass_gtf=gtf,
        prefix=prefix,
        thread=thread,
        job_type=job_type,
        work_dir=os.path.join(work_dir, work_dict["gmst"]),
        out_dir=out_dir)
    dag.add_task(gmst_task)
    gmst_task.set_upstream(pasa_task)

    do_dag(dag, concurrent_tasks=concurrent, refresh_time=refresh)

    return 0

 
def add_hlep_args(parser):

    parser.add_argument("genome", metavar="STR", type=str,
        help="Input genome file(fata).")
    parser.add_argument("-rna", "--rnaseq", metavar="FILE", type=str, required=True,
        help="Input the RNA-seq data.")
    parser.add_argument("--rna_tgs",  metavar="FILE", type=str, default="",
        help="Input the processed third-generation RNA seq sequence.")
    parser.add_argument("-p", "--prefix", metavar="FILE", type=str, default="out",
        help="Input sample name, default=out")
    parser.add_argument("--trim", metavar="INT", type=int, default=3,
        help="Set trim length, default=3")
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
    rnaseq2gene.py Transcriptome assisted gene prediction

attention:
    rnaseq2gene.py genome.fasta --rnaseq rna.list

version: %s
contact:  %s <%s>\
        """ % (__version__, " ".join(__author__), __email__))

    args = add_hlep_args(parser).parse_args()

    rnaseq2gene(genome=args.genome, rnaseq=args.rnaseq, rna_tgs=args.rna_tgs,  prefix=args.prefix, thread=args.thread, job_type=args.job_type,
                work_dir=args.work_dir, out_dir=args.out_dir, trim=args.trim, concurrent=args.concurrent,
                refresh=args.refresh)


if __name__ == "__main__":

    main()
