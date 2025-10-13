#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os.path
from collections import OrderedDict

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../")
BIN = os.path.join(ROOT, "bin")
SCRIPTS = os.path.join(ROOT, "scripts")
GFFSTAT = os.path.join(ROOT, "other/gffStat/gffStat.py")
QUEUE = "-q all.q"

#同源注释软件和数据库
METAEUK_BIN = "/Work/pipeline/software/meta/metaeuk/v5.34/bin/"
MINIPROT_BIN = "/Work/pipeline/software/Base/miniprot/v0.12/"
UNIPROT_DB = "/Work/database/SwissProt/202102/uniprot_sprot.fasta"
EVIANN_BIN = "/Work/pipeline/software/Base/eviann/v2.0.3/bin/"

#从头预测需要的软件
BRAKER_BIN = "/Work/pipeline/software/Base/braker2/v2.1.6/bin/"    #输入依赖的软件路径
GMES_BIN = "/Work/pipeline/software/meta/gmes/v4.68/"
GLIMMERHMM = "/Work/pipeline/software/Base/GlimmerHMM/v3.0.4"

GFFREAD_BIN = "/Work/pipeline/software/Base/gffread/v0.12.7/"
STRINGTIE_BIN = "/Work/pipeline/software/meta/stringtie/v2.2.0/"
SEQCLEAN_BIN = "/Work/pipeline/software/Base/seqclean/v1.0.0/"
#STAR_BIN = "/Work/pipeline/software/meta/STAR/v2.7.9a/"
#SAMTOOLS_BIN = "/Work/pipeline/software/Base/samtools/samtools/bin/"
PASA_BIN = "/Work/pipeline/software/Base/pasa/v2.5.2/bin/"
PASA_UTILITIES = "/Work/pipeline/software/Base/pasa/v2.5.2/opt/pasa-2.5.2/misc_utilities/"
TRANSDECODER_BIN = "/Work/pipeline/software/Base/pasa/v2.5.2/opt/transdecoder/"

#EVM_SCRIPT = "/Work/pipeline/software/Base/EVidenceModeler/v1.1.1/EvmUtils/"
EVM_BIN = "/Work/pipeline/software/Base/EVidenceModeler/v2.1.0/bin/"
BUSCO_BIN = "/Work/pipeline/software/Base/busco/lastest/bin/"
BUSCO_DB = "/Work/database/busco_db/odb12" #后面不能有斜杠
AUGUSTUS_BIN = "/Work/pipeline/software/Base/augustus/lastest/bin/"
AUGUSTUS_CONFIG_PATH = "/Work/pipeline/software/Base/augustus/lastest/config"
AUGUSTUS_EXT = os.path.join(ROOT, "pegs/extrinsic.cfg")
BRAKER2_BIN = "/Work/pipeline/software/Base/braker2/v2.1.6/bin/"
BUSCO_CONFIG_FILE = "/Work/pipeline/software/Base/busco/lastest/bin/config/myconfig.ini"
PSI_BIN = "/Work/pipeline/software/Base/ransposonPSI/v1.0.1/bin/"

