#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE,SIG_DFL)

import sys

from logging import getLogger, StreamHandler, Formatter

def setM(obj, l):
    for m in l:
        getattr(obj, m[0])(*m[1])
    return obj

def loginit(logname, format="%(message)s", stream=sys.stderr, level=15, datefmt="%Y/%m/%dT%H:%M:%S" ):
    return setM(getLogger(logname), [
        ("setLevel", [level]),
        ("addHandler", [setM(StreamHandler(stream),[("setFormatter", [Formatter(fmt=format,datefmt=datefmt)])])])
      ])

loggercfg = {
  "format": "%(asctime)s.%(msecs).03d %(process)d %(thread)x %(levelname).4s;%(module)s(%(lineno)d/%(funcName)s) %(message)s",
  "level": 10,
}
logstdcfg = {
  "level": 10,
  "stream": sys.stdout,
}

logger = loginit(__name__,**loggercfg)
logstd = loginit("std",**logstdcfg)

import functools
def traclog( f ):
    @functools.wraps(f)
    def _f(*args, **kwargs):
        logger.debug("ENTER:{0} {1}".format( f.__name__, kwargs if kwargs else args))
        result = f(*args, **kwargs)
        logger.debug("RETRN:{0} {1}".format( f.__name__, result))
        return result
    return _f

# Classes generate lines, parse and merge, output to collector| file
# TODO:
#   vmstat,iostat,ps,top,pidstat,mpstat,netstat,netstat -s,lsof,df,du,  application log,stat,trace,  java gc
import multiprocessing
import subprocess
import time
import itertools
import re
import select

from regex_dict import regex_dict

def concat( lsts ):
    return list(itertools.chain(*lsts))

CMDS = {
        "v": "vmstat {interval} | awk '{{ print strftime(\"%Y-%m-%dT%H:%M:%S\"), $0; fflush() }}'",
        "i": "iostat -tNxkyz -p ALL {interval}",
        "s": "netstat -s {interval}",
        }

@traclog
def parse_vmstat(backlog, pipe):
    names="time r  b   swpd   free   buff  cache   si   so    bi    bo   in   cs us sy id wa st".split()
    for line in backlog:
        if not line or re.search('p',line): continue
        logstd.info( dict(zip(names,line.strip().split())))
    while True:
        line = pipe.stdout.readline()
        logger.info("getline=[{0}]".format(line.replace("\n","\\n").replace("\t","\\t")))
        if not line or re.search('p',line): continue
        logstd.info( dict(zip(names,line.strip().split())))

vmdict=regex_dict({
    #procs -----------memory---------- ---swap-- -----io---- --system-- -----cpu-----
    # r  b   swpd   free   buff  cache   si   so    bi    bo   in   cs us sy id wa st
    #  2  0 2142864 178948 141932 3479568    0    0     1    17    2    3  8  4 88  0  0
    "^.* +\d+ +\d+ +\d+ +\d+ +\d+ +\d+ +\d+ +\d+ +\d+ +\d+ +\d+ +\d+ +\d+ +\d+ +\d+ +\d+ +\d+\s*$": (parse_vmstat,1),
    "^.* +r +b +swpd +free +buff +cache +si +so +bi +bo +in +cs +us +sy +id +wa +st\s*$": (parse_vmstat,2),
    "^.*procs -+memory-+ -+swap-+ -+io-+ -+system-+ -+cpu-+\s*$": (parse_vmstat,None),
})


@traclog
def guess(rdict,line):
    return rdict[line]

@traclog
def worker(cmd, rdict, logfile):
    parser=None
    backlog=[]
    c=subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE)
    while not parser:
        line = c.stdout.readline()
        if not line:
            if c.poll(): return
            time.sleep(1)
        logger.info("getline=[{0}]".format(line.replace("\n","\\n").replace("\t","\\t")))
        backlog.append(line)
        parser=guess(rdict,line)
    parser[0](backlog, c)

@traclog
def care_procs(procs):
    for p in procs: p.start()
    while True:
        deadps = [i for (i,p) in enumerate(procs) if not p.is_alive()]
        for i in deadps:
            procs[i].join()
        procs=[p for (i,p) in enumerate(procs) if not i in deadps]
        if not procs: break
    return 

def main(opts):
    if opts['verbose']: logger.info("{0}".format(opts))
    procs = []
    rdict = regex_dict(dict(concat([ d.items() for d in [vmdict,] ])))
    if not opts["args"]:
        sys.exit()
    for arg in opts["args"]:
        procs.append(multiprocessing.Process(target=worker,args=(CMDS[arg].format(interval=opts["interval"]) if arg in CMDS else arg, rdict, "_".join(arg))))
    return care_procs(procs)

def parsed_opts():
    import optparse
    import os

    opt = optparse.OptionParser()
    opt.add_option("-p", "--prof", default=False, action="store_true", help="get profile [default: %default]" )
    opt.add_option("-v", "--verbose", default=False, action="store_true", help="show detail info [default: %default]" )
    opt.add_option("-l", "--logfile", default=False, action="store_true", help="logging subprocs stdout to file [default: %default]" )
    opt.add_option("-i", "--interval", default=10, type="int", help="interval in sec [default: %default]" )
    (opts, args)= opt.parse_args()
    return dict(vars(opts).items() + [("args", args)])

if __name__ == '__main__':

    opts = parsed_opts()
    if opts['prof']:
      import cProfile
      cProfile.run('main(opts)')
      sys.exit(0)
    main(opts)
