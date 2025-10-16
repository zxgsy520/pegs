# pegs
Eukaryotic gene structure prediction
## 技术特点
（1）可以在集群上多节点运行，优化了部分比对方法，使之前需要几天注释的基因组，只需要几十小时甚至几小时完成注释；

（2）任务可以续投，防止某个环节任务报错，又需要从头计算的资源浪费；

（3）有很多课调节的接口，方便优化注释结果。

目前完成了几十个基因组的注释。欢迎大家测试和提问。
## Third-party
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

## Model Website
Augustus Model:[https://github.com/zxgsy520/augustus_model](https://github.com/zxgsy520/augustus_model)  #训练好的模型，可以下载使用（目前没有上传完，有需要可联系我们）
GlimmerHMM  Model: （待上传）

## Installation
```
git clone https://github.com/zxgsy520/pegs.git
mkdir bin
cd bin
cp /home/zhangxg/biotool /home/zhangxg/gffvert .
chmod 755 *
cd ..
chmod 755 fpegs.py
cd pegs
#Modify software configuration files, or add related software to environment variables.
vi config.py #编辑文献，配置相关软件和数据库
```
## Quick usage
```
./fpegs.py -h
usage: fpegs.py [-h] [-m FILE] -p FILE [--cds FILE] [--rna_list FILE]
                [--homo FILE [FILE ...]] [-hf FILE [FILE ...]] [-igff FILE]
                [--prefix STR] [-s STR] [-k STR] [-db STR] [-ml STR]
                [--no_split] [--thread INT] [--concurrent INT] [--refresh INT]
                [--job_type {sge,local}] [--work_dir DIR] [--out_dir DIR]
                STR

URL: https://github.com/zxgsy520/bypegs
name:
    fpegs.py Rapid prediction of fungal genome genes

attention:
    fpegs.py genome.fasta -p homo.protein.fasta --prefix name
    fpegs.py *genome.fasta -p homo.protein.fasta --prefix name
#如果没有转录组数据，可以使用gffread提取近源物种的转绿本
gffread -W -y proteins.faa -w transcripts.fa -g genome.fa genome.gff  #从近源物种中提取蛋白质转绿本数据

version: v1.2.1
contact:  Xingguo Zhang <invicoun@foxmail.com>        

positional arguments:
  STR                   Input genome file(fasta). #输入组装的基因组文件

optional arguments:
  -h, --help            show this help message and exit
  -m FILE, --masked FILE #输入屏蔽重复注释后的基因组文件（非必须）
                        Input the repeat masked genome(fasta).
  -p FILE, --protein FILE #输入去冗余后的近源物种蛋白
                        Input homologous species protein sequence.
  --cds FILE            Input homologous species cds #输入转录本或者近源物种的CDS（非必须）
                        sequence(也可输入组装的转录本).
  --rna_list FILE       Input homologous species RNA #输入转录组测序数据（非必须）
                        sequence(也可输入测序的转绿组数据).
  --homo FILE [FILE ...] #输入近源物种基因组文件
                        Input homologous species genome sequences.
  -hf FILE [FILE ...], --homogff FILE [FILE ...] #输入近源物种gff文件
                        Input gff file for homologous species annotation.
  -igff FILE, --introns_gff FILE #输入内含子的位置信息（非必须）
                        Input gff file for introns(introns.f.gff)
  --prefix STR          Input sample name. #输出结果前缀
  -s STR, --species STR  #设置分析的物种名称
                        Input the species name of the sample.
  -k STR, --kingdom STR #选择分析物种的类型（对于藻类，选择plant比fungi注释的效果更好）
                        Kingdom of the sample (fungi, plant, animal,
                        eukaryota), default=fungi
  -db STR, --busco_database STR 设置busco评估的数据库名称或者路径
                        Specify the name of the BUSCO lineage to be
                        used(/Work/database/busco_db/odb12/fungi).
                        default=fungi
  -ml STR, --minlen STR #过滤短序列，提升注释的速度
                        Input filtering preserves the shortest read length,
                        default=10kb
  --no_split            Set not to split and integrate the genome #设置不对基因组进行分割注释
  --thread INT          Threads used to run blastp (default: 4) #设置线程数目
  --concurrent INT      Maximum number of jobs concurrent (default: 10) #设置并行任务数据
  --refresh INT         Refresh time of log in seconds (default: 30) #设置任务检测刷新的时间
  --job_type {sge,local} #设置使用sge系统进行分析，或者local本地单节点分析
                        Jobs run on [sge, local] (default: local)
  --work_dir DIR        Work directory (default: current directory) #设置工作路径
  --out_dir DIR         Output directory (default: current directory) #设置最终输出结果路径
```

## Example
### Transcriptome assisted gene prediction（转绿组辅助基因组预测）
```
python pegs/rnaseq2gene.py genome.fasta --rnaseq  rna.list \
 --prefix S1 --thread 10 --trim 3 --work_dir work --out_dir out --job_type sge
#rna.list为二代测序的转录组数据，第一列和二列为reads的R1和R2。
#输出结果out中存在S1.gmst.gff3、S1.pasa.gff3和S1.transdecoder.gff3上个文件，将其拷贝到fpegs后续输出结果中，在运行EVM的时候可以自动利用相关的结果进行整合。
#工作路径中的work/06_Gmst/S1.trainset.aug.gtf用于后续augustus训练
```
### Augustus training model（Augustus训练模型）
```
python pegs/train_rna2aug.py genome.fasta --gff work/06_Gmst/S1.trainset.aug.gtf --species Phaffia_rhodozyma --job_type sge --work_dir aug_work
```

### Genomic prediction
Slow analysis：使用同源蛋白和转录组数据进行基因注释（需要前面处理的结果）
```
fpegs.py genome.fasta \ #输入预测的基因组
  --protein homo/Syrphidae.faa \ #输入去冗余后的近源蛋白（同种或者属；如果没有同种或者属，可以输入同科的，同时需要转绿组数据否则预测的结果比较差）
  --homo homo.fasta \ #近源物种的基因组序列（一般是同一个种的，使用一个就可以了）
  --homogff homo.gff \ #近源物种注释的gff文件
  --rna_list rna.list \ #转录组数据列表
  --prefix BF --specie BF \ #设置输出结果前缀和物种名称
  --kingdom animal --thread 8 --concurrent 10 --refresh 30 \  #设置基因组的物种类型，这里设置的是动物
  --busco_database diptera \  #设置用于评估的busco数据
  --job_type sge --work_dir fast_work --out_dir fast_out 
```
Chinese analysis：使用同源蛋白和转录组数据进行基因注释（需要前面处理的结果，贝到fast_out中）
```
fpegs.py genome.fasta \ #输入预测的基因组
  --protein homo/Syrphidae.faa \ #输入去冗余后的近源蛋白（同种或者属；如果没有同种或者属，可以输入同科的，同时需要转绿组数据否则预测的结果比较差）
  --homo homo.fasta \ #近源物种的基因组序列（一般是同一个种的，使用一个就可以了）
  --homogff homo.gff \ #近源物种注释的gff文件
  --prefix BF --specie BF \ #设置输出结果前缀和物种名称
  --kingdom animal --thread 8 --concurrent 10 --refresh 30 \  #设置基因组的物种类型，这里设置的是动物
  --busco_database diptera \  #设置用于评估的busco数据
  --job_type sge --work_dir fast_work --out_dir fast_out 
```

quick analysis：使用同源蛋白注释（不需要前面处理的结果，）
```
fpegs.py genome.fasta \ #输入预测的基因组
  --protein homo/Syrphidae.faa \ #输入去冗余后的近源蛋白（同种或者属；如果没有同种或者属，可以输入同科的，同时需要转绿组数据否则预测的结果比较差）
  --homo homo.fasta \ #近源物种的基因组序列（一般是同一个种的，使用一个就可以了）
  --homogff homo.gff \ #近源物种注释的gff文件
  --prefix BF --specie BF \ #设置输出结果前缀和物种名称
  --kingdom animal --thread 8 --concurrent 10 --refresh 30 \  #设置基因组的物种类型，这里设置的是动物
  --busco_database diptera \  #设置用于评估的busco数据
  --job_type sge --work_dir fast_work --out_dir fast_out 
```
## References
-----------
[1] CN115881217A，张兴国、程圣启、李三，[一种快速预测真核生物基因结构的方法和系统](https://wenku.baidu.com/view/a7f8a891757f5acfa1c7aa00b52acfc788eb9f9f.html?fr=aladdin266&ind=1&aigcsid=0&qtype=0&lcid=1&queryKey=%E7%9C%9F%E6%A0%B8%E7%94%9F%E7%89%A9%E5%9F%BA%E5%9B%A0%E7%BB%93%E6%9E%84%E9%A2%84%E6%B5%8B&verifyType=undefined&_wkts_=1760340272493&bdQuery=%E7%9C%9F%E6%A0%B8%E7%94%9F%E7%89%A9%E5%9F%BA%E5%9B%A0%E7%BB%93%E6%9E%84%E9%A2%84%E6%B5%8B)，中国，2022-12-13

## Application results
-----------
[1] Luo X, Shua Z, Zhao D, Liu B, Luo H, Chen Y, Meng D, Song Z, Yang Q, Wang Z, Tang D, Zhang X, Zhang J, Ma K, Yao W. Genome assembly of pomegranate highlights structural variations driving population differentiation and key loci underpinning cold adaption. Hortic Res. 2025 Jan 21;12(5):uhaf022. doi: [10.1093/hr/uhaf022](https://pmc.ncbi.nlm.nih.gov/articles/PMC11979328/). PMID: 40206514; PMCID: PMC11979328.

[2] Chen P, Huang Z, Yin M, Wen YX, Jiang Q, Huang P, Qian R, Hong X, Zhu K, Xiao B, Chen M, Li S, Huang F, Han LT. The Hedyotis diffusa chromosome-level genome and multi-omics analysis provide new insights into the iridoids biosynthetic pathway. Front Plant Sci. 2025 Jun 19;16:1607226. doi: [10.3389/fpls.2025.1607226](https://pmc.ncbi.nlm.nih.gov/articles/PMC12222090/). PMID: 40612608; PMCID: PMC12222090.


## Forecast results
|物种中文名称|注释方法|线程数目（CPU/h）|基因组大小（Mb）|congtig序列数目|基因数目|平均CDS长度|蛋白质BUSCOs完整性（%）|	蛋白质功能注释率（%）|
|  ----  |  ----  | ----  |  ----  | ----  |  ----  | ----  |  ----  | ----  |
|草酸青霉|pegs	|196|	32.98|11|11,946|1,410.99|99.34|95.83|
|草酸青霉|已发表|	499|32.98|11|9,718|1,414.50|99.00|96.21|
|桑黄|pegs|160|35.83 |24|11,880|1,525.23|92.88|91.56|
|桑黄|已发表|450	|35.83|24|8,455|1,710.90|89.70|90.39|
|着色霉|pegs|172	|35.32|7|12,661|1,391.23|95.65|97.05|
|着色霉|已发表|474|35.32|7|11,984|1,525.50|97.60|96.12|
