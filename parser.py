#!/usr/bin/env python
# -*- coding: utf-8 -*-

from signal import signal, SIGPIPE, SIG_DFL, SIGINT, SIG_IGN
signal(SIGPIPE,SIG_DFL)

import sys
import os
sys.path.append("{0}/libs".format(os.path.dirname(os.path.abspath(__file__))))
import common

loggercfg = {
  "format": "%(asctime)s.%(msecs).03d %(process)d %(thread)x %(levelname).4s;%(module)s(%(lineno)d/%(funcName)s) %(message)s",
  "level": 15,
}
logstdcfg = {
  "stream": sys.stdout,
  "level": 15,
}

logger = common.loginit(__name__,**loggercfg)
logstd = common.loginit("std",**logstdcfg)

# Classes generate lines, parse and merge, output to collector| file
# TODO:
#   vmstat,iostat,ps,top,pidstat,mpstat,netstat,netstat -s,lsof,df,du,  application log,stat,trace,  java gc
import Queue
import multiprocessing
import subprocess
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

@common.traclog(logger)
def guess(rdict,line):
    rslt=rdict[line]
    return rslt if rslt else None

@common.traclog(logger)
def collector_loop(que,cmd,rdict,logfile):
    S={"parser":None, "backlog":[], "pipe":None}
    S["pipe"]=subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE)
    while not S["parser"]:
        line = S["pipe"].stdout.readline()
        if not line:
            if not S["pipe"].poll(): return
            time.sleep(1)
        logger.debug("getline=[{0}]".format(line.replace("\n","\\n").replace("\t","\\t")))
        S["backlog"].append(line)
        S["parser"]=guess(rdict,line)
    logger.debug("{0}".format(S["parser"]))
    gendat =S["parser"][0][0](common.regex_dict(sys.modules[S["parser"][0][1]].rdict),S["backlog"], S["pipe"])
    for dat in gendat:
        logstd.info("{0}".format(json.dumps(dat)))
        que.put(dat)
    return 0

@common.traclog(logger)
def pool_run(target, argss):
    procs=[]
    ques=[]
    for args in argss:
        ques.append(multiprocessing.Queue()) 
        procs.append(multiprocessing.Process(target=target,args=common.concat([[ques[-1]],args])))
    for p in procs:
        p.daemon=True
        p.start()
    while True:
        logger.error("######### pool_run looop begin")
        for que in ques:
            logger.error("######### Checking QUEUE")
            try:
                while True:
                    logger.error("######### Checking QUEUE while loop")
                    logstd.info("gotque={0}".format(que.get(block=False)))
            except Queue.Empty as e:
                pass
        logger.error("######### sleep BEGIN")
        time.sleep(1)
        logger.error("######### sleep END")
        deadps=[i for (i,p) in enumerate(procs) if not p.is_alive()]
        logger.error("######### deadps = {0}".format(deadps))
        for i in deadps:
            logger.error("######### joining deadps[{0}]".format(i))
            procs[i].join()
        procs=[p for (i,p) in enumerate(procs) if not i in deadps]
        if not procs: break
        logger.error("######### pool_run loop end")
    return 


@common.traclog(logger)
def main(opts):
    if not opts["args"]:
        sys.exit()

    pool_run(collector_loop, [(
             CMDS[arg].format(interval=opts["interval"]) if arg in CMDS else arg, #cmd
             common.regex_dict(dict(common.concat([ d.items() for d in [pars_vmstat.rdict,pars_sar.rdict,] ]))),
             "{0}.{1}".format(arg,common.nowstr("%Y%m%dT%H%M%S")).replace(" ","_") #logname
        ) for arg in opts["args"]])

def parsed_opts():
    import optparse
    import os

    opt = optparse.OptionParser(usage="usage: %prog [options] [cmd1] [[cmd2] ...] \n cmd ={0}".format(CMDS))
    opt.add_option("-p", "--prof", default=False, action="store_true", help="get profile [default: %default]" )
    opt.add_option("-v", "--verbose", default=15, type="int", help="15: info, 10: debug, 5: trace [default: %default]" )
    opt.add_option("-l", "--logfile", default=False, action="store_true", help="logging subprocs stdout to file [default: %default]" )
    opt.add_option("-i", "--interval", default=10, type="int", help="interval in sec [default: %default]" )
    (opts, args)= opt.parse_args()
    return dict(vars(opts).items() + [("args", args)])

if __name__ == '__main__':

    opts = parsed_opts()
    logger.setLevel(opts['verbose'])
    if opts['prof']:
      import cProfile
      cProfile.run('main(opts)')
      sys.exit(0)
    main(opts)
