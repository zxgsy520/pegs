Pegs manual
===========

Table of Contents
-----------------

- [Quick usage](#quickusage)
- [Examples](#examples)

## <a name="quickusage"></a> Quick usage
```
name= ${name}
./fpegs.py ${name}.genome.fasta \
  --protein homo.faa \
  --homo homo_genomic.fasta \
  --homogff homo_genomic.gff3 \
  --prefix ${name} --specie candida_albicans --translation_table 12 \
  --kingdom fungi --thread 8 --concurrent 10 --refresh 30 \
  --job_type sge --work_dir fast_work --out_dir fast_out --no_split
```

## <a name="examples"></a> Examples
### Preparation of Homologous Genes(Required)
* (1) Download protein sequences of closely related species from NCBI or UniProt.

* (2) Remove protein sequences of mitochondrial and chloroplast origin.

* (3) Protein Sequence Deduplication:
```
name=Yarrowia
biotool filter_seq Yarrowia_uniprotkb.fasta --minlen 10 >${name}.faa  #过过短的蛋白质序列
cd-hit -i ${name}.faa -o ${name}.uniprotein.fasta  -c 0.95 -n 5 -d 0 -T 6 -M 20000 -aS 0.95  #根据相似度95%和覆盖度95%进行聚类去冗余
pedigree filter_protein ${name}.uniprotein.fasta >${name}.faa #过滤异常的蛋白质序列，如提前终止的
```
### Transcriptome Assembly and Gene Prediction
```
name=Yarrowia
./pegs/rnaseq2gene.py ${name}.genome.fasta --rnaseq rna.list \
  --prefix ${name} --thread 10 --trim 2 --work_dir rna_work --out_dir rna_out --job_type sge
```
### AUGUSTUS Model Training
```
name=Yarrowia
./pegs/train_rna2aug.py ${name}.genome.fasta --gff ${name}.transdecoder.gff3 --species Yarrowia_lipolytica --job_type sge --work_dir aug_work   #(Input can be: transcriptome-predicted GFF or existing species annotation GFF.)
```

