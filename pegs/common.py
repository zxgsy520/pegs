#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import

import os
import re
import sys
from glob import glob
import logging
from glob import glob
from time import sleep

try:
    reload(sys)
    sys.setdefaultencoding("utf8")
except:
    pass
try:
    from ConfigParser import ConfigParser
except:
    from configparser import ConfigParser

#from thirdparty.seqkit.FastaReader import open_fasta


LOG = logging.getLogger(__name__)
__version__ = "v1.0.3"


def cat(fns, outfn):
    """
    cat files together
    NOT USED NOW
    :param fns:
    :param outfn:
    :return:
    """
    LOG.debug("cat %s >%s" % (" ".join(fns), outfn))
    with open(outfn, "w") as out:
        for fn in fns:
            out.write(open(fn).read())

    return outfn


def cd(newdir):
    """
    from FALCON_KIT
    :param newdir:
    :return:
    """
    newdir = os.path.abspath(newdir)
    prevdir = os.getcwd()
    LOG.debug('CD: %r <- %r' % (newdir, prevdir))
    os.chdir(os.path.expanduser(newdir))
    return newdir


def rm(files):

    for file in files:
        if os.path.exists(file):
            os.remove(file)
        else:
            LOG.warning("%r not exists" % file)

    return 0


def rm_rf(d):

    d = os.path.abspath(d)
    if os.path.isdir(d):
        shutil.rmtree(d)
    else:
        LOG.warning("%r not exists" % d)

    return 0


def read_files(files, name):
    
    if not os.path.exists(files):
        msg = "File not found '{files}'".format(**locals())
        LOG.error(msg)
        raise Exception(msg)
    else:
        file_list = glob(os.path.join(files,name))

    return file_list    


def check_path(path):

    path = os.path.abspath(path)

    if not os.path.exists(path):
        msg = "File not found '{path}'".format(**locals())
        LOG.error(msg)
        raise Exception(msg)

    return path


def check_paths(obj):
    """
    check the existence of paths
    :param obj:
    :return: abs paths
    """

    if isinstance(obj, list):
        r = []
        for path in obj:
            r.append(check_path(path))

        return r
    else:
        return check_path(obj) 


def read_line(file):

    n = 0

    for line in open(file):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        n += 1

    return n 


def check_paths_lines(obj, line=2):

    r = ""
    if isinstance(obj, list): 
        r = []
        for path in obj:
            path = check_path(path)
            if read_line(path) < line:
                continue
            r.append(check_path(path))
    else:
        path = check_path(obj)
        if read_line(path) >= line:
            r = path
    return r


def check_status(fns, sleep_time):
    """
    check the existence of a list of done file until all done
    NOT USED
    :param fns:
    :param sleep_time:
    :return:
    """
    while 1:
        LOG.info("sleep %s" % sleep_time)
        sleep(sleep_time)
        done_num = 0
        for fn in fns:
            if os.path.exists(fn):
                done_num += 1
        if done_num == len(fns):
            LOG.info("all done")
            break
        else:
            LOG.info("%s done, %s running" % (done_num, len(fns) - done_num))

    return 1


def link(source, target, force=False):
    """
    link -s
    :param source:
    :param target:
    :param force:
    :return:
    """
    source = check_paths(source)

    # for link -sf
    if os.path.exists(target):
        if force:
            os.remove(target)
        else:
            raise Exception("%r has been exist" % target)

    LOG.info("ln -s {source} {target}".format(**locals()))
    os.symlink(source, target)

    return os.path.abspath(target)


def mkdir(d):
    """
    from FALCON_KIT
    :param d:
    :return:
    """
    d = os.path.abspath(d)
    if not os.path.isdir(d):
        LOG.debug('mkdir {!r}'.format(d))
        os.makedirs(d)
    else:
        LOG.debug('mkdir {!r}, {!r} exist'.format(d, d))

    return d


def touch(*paths):
    """
    touch a file.
    from FALCON_KIT
    """

    for path in paths:
        if os.path.exists(path):
            os.utime(path, None)
        else:
            open(path, 'a').close()
            LOG.debug('touch {!r}'.format(path))


def str2dict(string):
    """
    transform string "-a b " to dict {"a": "b"}
    :param string:
    :return:
    """
    assert isinstance(string, str)
    r = {}

    for p in string.split("-"):

        if not p:
            continue

        tmp = p.split(None, 1)
        param = tmp[0]

        if len(tmp) == 1:
            value = True
        else:
            value = tmp[1]

        if isinstance(value, str):
            value = value.strip()

        r[param] = value

    return r


def read_config(cfg):
    """
    read config fron ini
    :param cfg:
    :return:
    """
    check_paths(cfg)

    r = {}
    config = ConfigParser()
    config.read(cfg)

    for section in config.sections():
        r[section] = {}

        for option in config.options(section):
            value = config.get(section, option).strip().encode('utf-8').decode('utf-8')
            r[section][option] = value

    return r


def read_fofn(fofn):
    """

    :param fofn:
    :return:
    """

    fofn = check_paths(fofn)
    r = []

    for line in open(fofn):
        line = line.strip()

        if line.startswith("#") or not line:
            continue

        r.append(line)

    return r


def read_tsv(file, sep="\t"):

    for line in open(file):
        line = line.strip()

        if not line or line.startswith("#"):
            continue

        yield line.split(sep)


def get_genome_size(genome):
    """
    return genome size in M
    :param genome:
    :return:
    """
    r = 0

    for seq in open_fasta(genome):
        r += len(seq)

    return r / 1000000.0


def get_version(tool):

    _version = os.popen(tool["GETVER"]).read().strip()

    g = re.search("(%s)" % tool["REGEXP"], _version)
    if g:
        return g.group(1)
    else:
        raise Exception(_version)


def stat_max_count(klist):

    max_key = ""
    max_count = 0

    for i in set(klist):
        kcount = klist.count(i)
        if kcount >= max_count:
            max_count = kcount
            max_key = i

    return max_key, max_count


def sort_names(names):

    d = {}
    for i in names:
        if re.search(r"\d+", i): #样本中存在数字的情况下
            prefix = "".join(re.findall(r"\D+", i))
            num = float("".join(re.findall(r"\d+", i)))
        else:
            prefix = i
            num = 0
        d[i] = [prefix, num]

    r = []
    for sample, temp in sorted(d.items(), key=lambda x:(x[1][0], x[1][1]), reverse=False):
        r.append(sample)

    return r


def read_fasta(file):

    '''Read fasta file'''
    if file.endswith(".gz"):
        fp = gzip.open(file)
    elif file.endswith(".fasta") or file.endswith(".fa") or file.endswith(".faa"):
        fp = open(file)
    else:
        raise Exception("%r file format error" % file)

    r = ""
    for line in fp:
        if isinstance(line, bytes):
            line = line.decode("utf-8")
        line = line.strip()

        if not line:
            continue
        if line.startswith(">"):
            if r:
                yield r.split("\n", 1)
            r = "%s\n" % line.strip(">") #去除大于号，没有去除末尾的注释
            continue
        r += line.upper()

    if r:
        yield r.split("\n", 1)

    fp.close() 
