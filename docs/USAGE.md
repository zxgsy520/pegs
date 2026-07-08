Pegs manual
===========

Table of Contents
-----------------

- [Quick usage](#quickusage)
- [Examples](#examples)

## <a name="quickusage"></a> Quick usage
### Preparation of Homologous Genes(Required)
*(1) Download protein sequences of closely related species from NCBI or UniProt.

*(2) Remove protein sequences of mitochondrial and chloroplast origin.

*(3) Protein Sequence Deduplication:
```
name=Yarrowia
biotool filter_seq Yarrowia_uniprotkb.fasta --minlen 10 >${name}.faa  #过过短的蛋白质序列
cd-hit -i ${name}.faa -o ${name}.uniprotein.fasta  -c 0.95 -n 5 -d 0 -T 6 -M 20000 -aS 0.95  #根据相似度95%和覆盖度95%进行聚类去冗余
pedigree filter_protein ${name}.uniprotein.fasta >${name}.faa #过滤异常的蛋白质序列，如提前终止的
```
