#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
from logging import getLogger, StreamHandler, Formatter

def setM(obj, l):
    for m in l:
        getattr(obj, m[0])(*m[1])
    return obj

def loginit(logname, format,level,datefmt,stream ):
    return setM(getLogger(logname), [
        ("setLevel", [level]),
        ("addHandler", [setM(StreamHandler(stream),[("setFormatter", [Formatter(fmt=format,datefmt=datefmt)])])])
      ])

def loginit(logname, format,level,datefmt,stream ):
    f=Formatter(fmt=format,datefmt=datefmt)
    h=StreamHandler(stream)
    
    return setM(getLogger(logname), [
        ("setLevel", [level]),
        ("addHandler", [setM(StreamHandler(stream),[("setFormatter", [Formatter(fmt=format,datefmt=datefmt)])])])
      ])

logger = loginit(__name__,"%(asctime)s.%(msecs).03d %(process)d %(thread)x %(levelname).4s;%(module)s(%(lineno)d/%(funcName)s) %(message)s",10,"%Y/%m/%dT%H:%M:%S",sys.stderr)
stdout = loginit("std","%(message)s",10,"%Y/%m/%dT%H:%M:%S",sys.stdout)

"""
POST http://host.com:8086/write
{
    "database": "mydb",
    "points": [
        {
            "name": "cpu_load_short",
            "tags": {
                "host": "server01"
            },
            "timestamp": "2009-11-10T23:00:00Z",
            "fields": {
                "value": 0.64
            }
        }
    ]
}
{
    "database": "mydb",
    "retentionPolicy": "default",
    "tags": {
        "host": "server01",
        "region": "us-west"
    },
    "timestamp": "2009-11-10T23:00:00Z",
    "points": [
        {
            "name": "cpu_load_short",
            "fields": {
                "value": 0.64
            }
        },
        {
            "name": "cpu_load_short",
            "fields": {
                "value": 0.55
            },
            "timestamp": "1422568543702900257",
            "precision": "n"
        }
    ] 
}
"timestamp": "2015-01-29T21:50:44Z"
"timestamp": "2015-01-29T14:49:23-07:00"
"timestamp": "2015-01-29T21:51:28.968422294Z"
"timestamp": "2015-01-29T14:48:36.127798015-07:00"
"timestamp": 1422568543702900257, "precision": "n"
"""
import itertools
import httplib
import urllib
import json

APIS = {
        "query": ["GET","/query?{params}"],
        "write": ["POST","/write?{params}"],
        }

def meth(obj, tt):
    return [getattr(obj, t[0])(*t[1]) for t in tt]

def concat( lsts ):
        return list(itertools.chain(*lsts))

def httpreqM(conn, request,path,body="",headers={}):
    r1 = conn.request(method=request,url=path,body=body,headers=headers)
    r2 = conn.getresponse()
    try:
        logger.debug('{3}:status=[{0}]:reason=[{1}]:url=[{2}]:body="{4}"'.format(r2.status, r2.reason,path,request,body.replace("\n","\\n")))
        contents = r2.read()
    except Exception, e:
        conn.close()
        logger.error('HTTPReqFailed:error=[{0}]:status=[{1}]:reason=[{2}]:url=[{3}]'.format(e, r2.status, r2.reason, request))
        sys.exit(-1)
    conn.close()
    return contents

def buildurl(tuplelists):
    return urllib.urlencode(dict(concat(tuplelists)))

def parse_dst(dst):
    l = dst.split(":")
    return [l[0], int(l[1]) if len(l)>1 else 80]

def jsonpretty(xs):
    try:
        if isinstance(xs, list):
            return json.dumps([ json.loads(x) for x in xs ], indent=2, ensure_ascii=False)
        else:
            return json.dumps(json.loads(xs), indent=2, ensure_ascii=False)
    except Exception as e:
        logger.warn("Faile to parse as json: {0}".format(xs))
        return xs

class InfluxClient(object):
    """
      c = InfluxClient(
             http={ "host": "127.0.0.1", "port": 80, "timeout": 3000 },
             auth= { "u": "username", "p": "password" },
             base= { "time_precision": "s" }
           )
    """
    def __init__(self, httpcon, auth,base):
        self._httpcon=httplib.HTTPConnection(**httpcon)
        self._auth=auth
        self._base=base
        #self._headers={ "Content-type": "application/json", "Accept": "text/plain"} 
        self._headers={ "Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain", "Connection": "keep-alive"} 

    def query(self, s, opt={}):
        return httpreqM(
                conn=self._httpcon,
                request=APIS["query"][0],
                path=APIS["query"][1].format(params=buildurl([self._auth,self._base,[("q",s)],opt.items()]))
                )

    def write(self, body, opt={}):
        return httpreqM(
                conn=self._httpcon,
                request=APIS["write"][0],
                path=APIS["write"][1].format(params=buildurl([self._auth,self._base,opt.items()])),
                body=body,
                headers=self._headers
                )

cmds={
        "default": (("query", ["SHOW DATABASES"]),("query", ["SHOW MEASUREMENTS"])),
        "db": (("query", ["SHOW DATABASES"   ]),),
        "ms": (("query", ["SHOW MEASUREMENTS"]),),
}

def main(opts):
    logger.info("{0}".format(opts))
    c = InfluxClient(
            httpcon = dict([(k,v) for (k,v) in zip(["host","port","timeout"], parse_dst(opts["server"]) + [opts["timeout"]])]),
            auth=[("u", opts["user"]), ("p", opts["pswd"])], 
            base=[("time_precision", opts["precision"]) ]
        )
    if opts["write"]:
        stdout.info(jsonpretty( c.write( body="\n".join(opts["args"]), opt={} if not opts["db"] else {"db":opts["db"]}) ))
        return
    if not opts["args"] or opts["args"][0] in cmds:
        stdout.info(jsonpretty(meth(c, cmds[opts["args"][0] if opts["args"] else "default"])))
    else:
        for query in opts["args"]:
            stdout.info(jsonpretty( c.query(s=query,opt={} if not opts["db"] else {"db":opts["db"]}) ) )


def parsed_opts():
    import optparse
    import os

    opt = optparse.OptionParser("{0}".format(cmds))
    opt.add_option("-P", "--prof", default=False, action="store_true", help="get profile [default: %default]" )
    opt.add_option("-L", "--loglevel", default=25, type="int", help="15:info, 10:debug, 5:trace [default: %default]" )
    opt.add_option("-s", "--server", default="localhost:10086", help="destination server [default: %default]" )
    opt.add_option("-u", "--user", default="root", help="user [default: %default]" )
    opt.add_option("-w", "--pswd", default="root", help="pswd [default: %default]" )
    opt.add_option("-d", "--db", default=None, help="dbname [default: %default]" )
    opt.add_option("-p", "--rp", default=None, help="retention policy [default: %default]" )
    opt.add_option("-T", "--precision", default="s", help="time_precision [default: %default]" )
    opt.add_option("-t", "--timeout", default=3000, help="timeout for http request [default: %default]" )
    opt.add_option("-W", "--write", default=False, action="store_true", help="write points [default: %default]" )
    (opts, args)= opt.parse_args()
    return dict(vars(opts).items() + [("args", args)])

if __name__ == '__main__':

    opts = parsed_opts()
    logger.setLevel(opts['loglevel'])
    if opts['prof']:
      import cProfile
      cProfile.run('main(opts)')
      sys.exit(0)
    main(opts)
