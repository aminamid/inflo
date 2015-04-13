#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

from logging import getLogger, basicConfig
logger = getLogger(__name__)
logcfg = {
  "format": "%(asctime)s.%(msecs).03d %(process)d %(thread)x %(levelname).4s;%(module)s(%(lineno)d/%(funcName)s) %(message)s",
  #"format": "%(message)s",
  "datefmt": "%Y/%m/%dT%H:%M:%S",
  "level": 10,
  "stream": sys.stdout,
}
basicConfig(**logcfg)

#from logging import getLogger, StreamHandler, Formatter
#def setM(obj, l):
#    for m in l:
#        getattr(obj, m[0])(*m[1])
#    return obj
#
#def loginit(logname, format,level,datefmt,stream ):
#    return setM(getLogger(logname), [
#        ("setLevel", [level]),
#        ("addHandler", [setM(StreamHandler(stream),[("setFormatter", [Formatter(fmt=format,datefmt=datefmt)])])])
#      ])
#logger   = loginit(__name__,"%(asctime)s.%(msecs).03d %(process)d %(thread)x %(levelname).4s;%(module)s(%(lineno)d/%(funcName)s) %(message)s",10,"%Y/%m/%dT%H:%M:%S",sys.stderr)
#logstd = loginit("std","%(message)s",10,"%Y/%m/%dT%H:%M:%S",sys.stdout)


# Classes generate lines, parse and merge, output to collector| file
# TODO:
#   vmstat,iostat,ps,top,pidstat,mpstat,netstat,netstat -s,lsof,df,du,  application log,stat,trace,  java gc
import multiprocessing
import subprocess
import select

CMDS = {
        "v": "vmstat {interval} | awk '{{ print strftime(\"%Y-%m-%dT%H:%M:%S\"), $0; fflush() }}'",
        "i": "iostat -tNxkyz -p ALL {interval}",
        "s": "netstat -s {interval}",
        }

def parse_vmstat(backlog, pipe):
    names=" r  b   swpd   free   buff  cache   si   so    bi    bo   in   cs us sy id wa st"
    while True:
        line = c.stdout.readline()
        if "procs" in line: pass
    return dict(zip(names,line.strip().split()))

from regex_dict import regex_dict

rdict=regex_dict({
    #procs -----------memory---------- ---swap-- -----io---- --system-- -----cpu-----
    # r  b   swpd   free   buff  cache   si   so    bi    bo   in   cs us sy id wa st
    #  2  0 2142864 178948 141932 3479568    0    0     1    17    2    3  8  4 88  0  0
    r"^.* +\d+ +\d+ +\d+ +\d+ +\d+ +\d+ +\d+ +\d+ +\d+ +\d+ +\d+ +\d+ +\d+ +\d+ +\d+ +\d+ +\d+\s*$": parse_vmstat,
    r"^.* +r +b +swpd +free +buff +cache +si +so +bi +bo +in +cs +us +sy +id +wa +st\s*$": parse_vmstat,
    r"^.*procs -+memory-+ -+swap-+ -+io-+ -+system-+ -+cpu-+\s*$": parse_vmstat
    r"^.*procs -+memory-+ -+swap-+ -+io-+ -+system-+ -+cpu-+\s*$": parse_vmstat
})

def guess(line):
    return rdict.get(line)

def sub(cmd, guess, logfile):
    thisis=None
    backlog=[]
    logger.debug(cmd)
    c=subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE)
    while not thisis:
        line = c.stdout.readline()
        logger.info( line )
        backlog.append(line)
        parser=guess(line)
    parser(backlog, c)

def main(opts):
    if opts['verbose']: logger.info("{0}".format(opts))
    procs = []
    if not opts["args"]:
        sys.exit()
    for arg in opts["args"]:
        procs.append(multiprocessing.Process(target=sub,args=(CMDS[arg].format(interval=opts["interval"]) if arg in CMDS else arg, guess)))
    for p in procs: p.start()

    while True:
        deadps = [i for (i,p) in enumerate(procs) if not p.is_alive()]
        for i in deadps:
            procs[i].join()
        procs=[p for (i,p) in enumerate(procs) if not i in deadps]
        if not procs: break

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
