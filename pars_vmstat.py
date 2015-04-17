#!/usr/bin/env python
# -*- coding: utf-8 -*-
from logging import getLogger
logger=getLogger(__name__)
import datetime
import common

def parse(rdict, backlog, pipe):
    cols=""
    for rawline in backlog:
        line=rawline.strip()
        #
        if not line: cotinue
        eval=rdict[line]
        if not eval or "junk" in eval[1].groupdict(): continue
        if "cols" in eval[1].groupdict():
            cols=eval[1].groupdict()["cols"]
            continue
        if "vals" in eval[1].groupdict():
            yield dict(common.concat([
                zip(
                    cols.strip().split(),
                    eval[1].groupdict()["vals"].strip().split()
                ),
                [("time", common.gentime() if not eval[1].groupdict()["time"] else eval[1].groupdict()["time"])]
            ]))
        #
    #for line in pipe.stdout:
    while True:
        rawline = pipe.stdout.readline()
        if not rawline:
            if not pipe.poll():
                return
            else:
                continue
        line=rawline.strip()
        logger.debug("getline=[{0}]".format(line.replace("\n","\\n").replace("\t","\\t")))
        #
        if not line: continue
        eval=rdict[line]
        if not eval or "junk" in eval[1].groupdict(): continue
        if "cols" in eval[1].groupdict():
            cols=eval[1].groupdict()["cols"]
            continue
        if "vals" in eval[1].groupdict():
            yield dict(common.concat([
                zip(
                    cols.strip().split(),
                    eval[1].groupdict()["vals"].strip().split()
                ),
                [("time", common.gentime() if not eval[1].groupdict()["time"] else eval[1].groupdict()["time"])]
            ]))
        #

rdict={
    #procs -----------memory---------- ---swap-- -----io---- --system-- -----cpu-----
    # r  b   swpd   free   buff  cache   si   so    bi    bo   in   cs us sy id wa st
    #  2  0 2142864 178948 141932 3479568    0    0     1    17    2    3  8  4 88  0  0
    "^(?P<time>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}){0,1} *(?P<vals>(\d+ +){16}\d+)\s*$": (parse,__name__),
    "^(?P<time>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}){0,1} *(?P<cols>r .* st)\s*$": (parse,__name__),
    "^(?P<time>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}){0,1} *(?P<junk>procs .*)\s*$": (parse,__name__),
}

if __name__=="__main__":
    import subprocess
    from logging import basicConfig
    basicConfig(level=10)
    rdict = common.regex_dict(dict(common.concat([ d.items() for d in [rdict,] ])))
    c=subprocess.Popen("vmstat 2 2", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE)
    gen=parse(rdict, [], c)
    for dat in gen:
        print dat
