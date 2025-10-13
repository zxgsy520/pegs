# pegs
Eukaryotic gene structure prediction

Third-party
-----------

pegs package includes some third-party software:
* [python](https://www.python.org/) (使用python3运行较多，使用python2也是可以运行的，撰写的充分考虑了兼容问题)
* * Three-party python package
  * [pysam](https://pypi.org/project/pysam/)
  * [matplotlib](https://matplotlib.org/)
  * [numpy](https://numpy.org/doc/stable/index.html)
  * [scipy](https://github.com/scipy/scipy)
* [R](https://www.r-project.org/)
* [biotool](https://github.com/zxgsy520/biotool)
* [gffvert](https://github.com/zxgsy520/gffvert)
* [metaeuk](https://github.com/soedinglab/metaeuk)
* [miniprot](https://github.com/lh3/miniprot)
* [eviann](https://github.com/alekseyzimin/EviAnn_release)
* [braker2](https://github.com/Gaius-Augustus/BRAKER)
* [GeneMark-ES](http://topaz.gatech.edu/GeneMark/license_download.cgi)
* [GlimmerHMM](https://ccb.jhu.edu/software/glimmerhmm/man.shtml)
* [gffread](https://github.com/gpertea/gffread)
* [stringtie](https://github.com/gpertea/stringtie)
* [seqclean](https://sourceforge.net/projects/seqclean/)
* [pasa](https://github.com/PASApipeline/PASApipeline)
* [EVidenceModeler](https://github.com/EVidenceModeler/EVidenceModeler)
* [busco](https://gitlab.com/ezlab/busco)
* [augustus](http://bioinf.uni-greifswald.de/augustus/)
* [ransposonPSI](http://transposonpsi.sourceforge.net/)
* [STAR](https://github.com/alexdobin/STAR)

## Example1
```
fpegs.py genome.fasta \ #输入预测的基因组
  --protein homo/Syrphidae.faa \ #输入去冗余后的近源蛋白（同种或者属；如果没有同种或者属，可以输入同科的，同时需要转绿组数据否则预测的结果比较差）
  --homo homo.fasta \ #近源物种的基因组序列（一般是同一个种的，使用一个就可以了）
  --homogff homo.gff \ #近源物种注释的gff文件
  --rna_list rna.list \ #转录组数据列表
  --prefix BF --specie BF \ #设置输出结果前缀和物种名称
  --kingdom animal --thread 8 --concurrent 10 --refresh 30 \  #设置基因组的物种类型，这里设置的是动物
  --busco_database diptera \  #设置用于评估的busco数据
  --introns_gff star_work/introns.f.gff \ #输入star比对的基因结构文件
  --job_type sge --work_dir fast_work --out_dir fast_out 
```

## References
[1] CN115881217A，张兴国、程圣启、李三，[一种快速预测真核生物基因结构的方法和系统](https://wenku.baidu.com/view/a7f8a891757f5acfa1c7aa00b52acfc788eb9f9f.html?fr=aladdin266&ind=1&aigcsid=0&qtype=0&lcid=1&queryKey=%E7%9C%9F%E6%A0%B8%E7%94%9F%E7%89%A9%E5%9F%BA%E5%9B%A0%E7%BB%93%E6%9E%84%E9%A2%84%E6%B5%8B&verifyType=undefined&_wkts_=1760340272493&bdQuery=%E7%9C%9F%E6%A0%B8%E7%94%9F%E7%89%A9%E5%9F%BA%E5%9B%A0%E7%BB%93%E6%9E%84%E9%A2%84%E6%B5%8B)，中国，2025-10-03S
