#!/usr/bin/env python
#coding:utf-8

import os
import sys
import re
import time
import argparse
import logging

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "../"))

from dagflow import DAG, Task, do_dag


LOG = logging.getLogger(__name__)

__version__ = "1.1.0"
__author__ = ("Xingguo Zhang",)
__email__ = "invicoun@foxmail.com"
__all__ = []


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


def mkdir(d):
    """
    from FALCON_KIT
    :param d:
    :return:
    """
    d = os.path.abspath(d)
    if not os.path.isdir(d):
        LOG.debug("mkdir {!r}".format(d))
        os.makedirs(d)
    else:
        LOG.debug("mkdir {!r}, {!r} exist".format(d, d))

    return d


def read_tsv(file, sep=None):

    if file.endswith(".gz"):
        fh = gzip.open(file)
    else:
        fh = open(file)

    for line in fh:
        if isinstance(line, bytes):
            line = line.decode("utf-8")
        line = line.strip()

        if not line:
            continue

        yield line.split(sep)

    fh.close()



def read_task(file, rows=1):

    n = 0
    temp = []
    
    for line in read_tsv(file, "\n"):
        line = line[0].strip()

        if line.startswith("#"):
            continue
        n += 1
        temp.append(line)
        if n >= rows:
            yield "\n".join(temp)
            temp = []
            n = 0

    if temp:
        yield "\n".join(temp)


def read_task_txt(file):

    r = []

    for line in read_tsv(file, "\n"):
        line = line[0].strip()
        if line.startswith("#"):
            continue
        r.append(line)

    return "\n".join(r)


def daglist(files, prefix, work_dir="", concurrent=20, refresh=30,
            job_type="sge", rows=1, env_path=""):

    work_dir = mkdir(work_dir)
    if env_path:
        env_path = check_path(env_path)

    files = check_paths(files)
    if len(files) == 1:
        tasks = read_task(files[0], rows=rows)
    else:
        tasks = []
        for file in files:
            if file.endswith(".out"):
                continue
            tasks.append(read_task_txt(file))

    n = 0
    dag = DAG("run_%s_tasks" % prefix)
    for line in tasks:
        script = ""
        if env_path:
            script = "export PATH=%s:$PATH\n" % env_path
        script += line
        n += 1
        task = Task(id="%s_%s" % (prefix, n),
                    type=job_type,
                    work_dir=work_dir,
                    option="-pe smp 2",
                    script=script
                )
        dag.add_task(task)

    do_dag(dag, concurrent_tasks=concurrent, refresh_time=refresh)

    return 0


def add_hlep_args(parser):

    parser.add_argument("input", nargs="+",  metavar="STR", type=str,
        help="Input input script file.")
    parser.add_argument("-p", "--prefix", metavar="STR", type=str, default="evm",
        help="Input sample name, default=evm.")
    parser.add_argument("-w", "--work_dir", metavar="DIR", type=str, default="work",
        help="Work directory (default: current directory)")
    parser.add_argument("--concurrent", metavar="INT", type=int, default=40,
        help="Maximum number of jobs concurrent  (default: 10)")
    parser.add_argument("--refresh", metavar="INT", type=int, default=30,
        help="Refresh time of log in seconds (default: 30)")
    parser.add_argument("--job_type", choices=["sge", "local"], default="sge",
        help="Jobs run on [sge, local]  (default: sge)")
    parser.add_argument("--rows", metavar="INT", type=int, default=1,
        help="Input the number of lines in the script (default: 1)")
    parser.add_argument("-ep", "--env_path", metavar="STR", type=str, default="",
        help="Input the environment path that the script depends on.")

    return parser


def main():

    logging.basicConfig(
        stream=sys.stderr,
        level=logging.INFO,
        format="[%(levelname)s] %(message)s"
    )
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
    description='''
name:
    daglist.py Run tasks in batches based on scripts

attention:
    daglist.py commands.list --prefix evm --workdir shell
    daglist.py aug* --prefix aug --workdir ./
version: %s
contact:  %s <%s>\
        ''' % (__version__, " ".join(__author__), __email__))

    args = add_hlep_args(parser).parse_args()

    daglist(args.input, args.prefix, args.work_dir, args.concurrent,
            args.refresh, args.job_type, args.rows, args.env_path)


if __name__ == "__main__":

    main()
