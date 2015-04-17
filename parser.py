#!/usr/bin/env python
# -*- coding: utf-8 -*-

from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE,SIG_DFL)

import sys
import common

loggercfg = {
  #  "format": "%(asctime)s.%(msecs).03d %(process)d %(thread)x %(levelname).4s;%(module)s(%(lineno)d/%(funcName)s) %(message)s",
  "level": 10,
}
logstdcfg = {
  "stream": sys.stdout,
  "level": 10,
}

logger = common.loginit(__name__,**loggercfg)
logstd = common.loginit("std",**logstdcfg)

# Classes generate lines, parse and merge, output to collector| file
# TODO:
#   vmstat,iostat,ps,top,pidstat,mpstat,netstat,netstat -s,lsof,df,du,  application log,stat,trace,  java gc
import multiprocessing
import subprocess
import datetime
import time
import itertools
import re
import select
import json

import pars_vmstat
import pars_sar
#common.loginit("pars_vmstat",**loggercfg)
#common.loginit("pars_sar",**loggercfg)


CMDS = {
        "v": "LANG=C vmstat {interval} | awk '{{ print strftime(\"%Y-%m-%dT%H:%M:%S\"), $0; fflush() }}'",
        "i": "LANG=C iostat -tNxkyz -p ALL {interval}",
        "n": "LANG=C netstat -s {interval}",
        "s": "LANG=C sar -A {interval}",
        }

def guess(rdict,line):
    rslt=rdict[line]
    return rslt if rslt else None

@common.traclog(logger)
def worker_loop(cmd, rdict, logfile):
    parser=None
    backlog=[]
    c=subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE)
    while not parser:
        line = c.stdout.readline()
        if not line:
            if not c.poll(): return
            time.sleep(1)
        logger.info("getline=[{0}]".format(line.replace("\n","\\n").replace("\t","\\t")))
        backlog.append(line)
        parser=guess(rdict,line)
    logger.debug("{0}".format(parser))
    gendat = parser[0][0](common.regex_dict(sys.modules[parser[0][1]].rdict),backlog, c)
    for dat in gendat:
        logstd.info("{0}".format(json.dumps(dat)))

def run(procs):
    for p in procs: p.start()
    while True:
        time.sleep(1)
        deadps = [i for (i,p) in enumerate(procs) if not p.is_alive()]
        for i in deadps:
            procs[i].join()
        procs=[p for (i,p) in enumerate(procs) if not i in deadps]
        if not procs: break
    return 


def main(opts):
    if opts['verbose']: logger.info("{0}".format(opts))
    procs = []
    rdict = common.regex_dict(dict(common.concat([ d.items() for d in [pars_vmstat.rdict,pars_sar.rdict,] ])))
    if not opts["args"]:
        sys.exit()
    ts=common.genlogtime()
    for arg in opts["args"]:
        procs.append(multiprocessing.Process(target=worker_loop,args=(CMDS[arg].format(interval=opts["interval"]) if arg in CMDS else arg, rdict, "{0}.{1}".format(arg,ts).replace(" ","_"))))
    run(procs)

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
