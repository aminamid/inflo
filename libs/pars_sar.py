#!/usr/bin/env python
# -*- coding: utf-8 -*-
from logging import getLogger
logger=getLogger(__name__)
import datetime
import common

@common.traclog(logger)
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
    #    Linux 2.6.32-431.17.1.el6.x86_64 (jpn-zaq50)    04/17/15        _x86_64_        (4 CPU)
    #15:27:24        CPU      %usr     %nice      %sys   %iowait    %steal      %irq     %soft    %guest     %idle
    #15:27:26        all     36.56      0.00     24.25      0.00      0.00      0.00      0.00      1.13     38.07
    "^\s*(?P<uname>Linux\s+\S+)\s+(?P<host>\S+)\s+(?P<date>\S+)\s+\S+\s+\((?P<numcpu>\d+)\s+CPU\)\s*$": (parse,__name__),
    "^(?P<time>\d{2}:\d{2}:\d{2}) (?P<noon>AM|PM){0,1}\s+(?P<cols>(\S*[^\s0-9\.\-\+]\S*\s+){0,}\S*[^\s0-9\.\-\+]\S*)\s*$": (parse,__name__),
    "^(?P<time>\d{2}:\d{2}:\d{2}) (?P<noon>AM|PM){0,1}\s+(?P<vals>(\S+\s+){0,}[0-9\.\-\+]+)\s*$": (parse,__name__),
    "^(?P<junk>Average:.*)\s*$": (parse,__name__),
}
if __name__=="__main__":
    import json
    import subprocess
    from logging import basicConfig
    basicConfig(level=10)
    rdict = common.regex_dict(dict(common.concat([ d.items() for d in [rdict,] ])))
    c=subprocess.Popen("sar -A 2 2", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE)
    gen=parse(rdict, [], c)
    for dat in gen:
        print json.dumps(dat)
