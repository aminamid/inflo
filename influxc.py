#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import itertools
import httplib
import urllib
import json

from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE,SIG_DFL)

sys.path.append("{0}/libs".format(os.path.dirname(os.path.abspath(__file__))))
import common
logger = common.loginit(__name__,**common.loggercfg)
stdout = common.loginit("std",**common.stdoutcfg)

def concat( lsts ):
        return list(itertools.chain(*lsts))

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

def http_conn(url,retry=3):
    """
    yield receive:
       arg of httplib.HTTPSConnection(host[, port[, key_file[, cert_file[, strict[, timeout[, source_address]]]]]])
    """
    logger.debug("url={0}".format(url))
    conn=httplib.HTTPConnection(**url)
    while True:
        for i in range(retry):
            try:
                conn.close()
                conn.connect()
                yield conn
            except Exception as e:
                logger.warning("{0}".format(e))
                continue

def http_req(conn,retry=3):
    """
    yield receive:
        [ httpconnection, arg of(HTTPConnection.request(method, url[, body[, headers]])) ]
    """
    logger.debug("conn={0}".format(conn))
    contents=None
    _conn=next(conn)
    while True:
        recv = (yield contents)
        for i in range(retry): 
            try:
                _conn.request(**recv)
                r2 = _conn.getresponse()
                logger.debug('HTTPReqSuccess:{0}:{1}:{2}'.format(r2.status, r2.reason,recv))
                contents=r2.read()
                break
            except Exception as e:
                logger.warn('HTTPReqFail:{0}:{1}:{2}:retry={3}:{4}'.format(r2.status, r2.reason,recv,i,e))
                contents=None
                _conn=next(conn)
            logger.error('HTTPReqFail:{0}:{1}:{2}:retry={3}:{4}'.format(r2.status, r2.reason,recv,i,e))

def influx_getter(req,influx_args):
    """
    yield receive:
        [ influx_params, influx_query ]
    """
    contents=None 
    while True:
        receive= (yield contents)
        contents = req.send({
                       "method": "GET",
                       "url": "/query?{0}".format(urllib.urlencode(dict(concat([influx_args,[("q",receive)]])))),
                       "headers":{ "Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain", "Connection": "keep-alive" },
                           })

def influx_poster(req,influx_args):
    """
    yield receive:
        [ influx_params, influx_body ]
    """
    contents=None
    while True:
        receive= (yield contents)
        contents = req.send({
                       "method": "POST",
                       "url": "/write?{0}".format(urllib.urlencode(dict(concat([influx_args])))),
                       "body": receive,
                       "headers":{ "Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain", "Connection": "keep-alive" },
                           })


def _create_gens(gen_func, http_parms, influx_parms):
    req=http_req(http_conn(dict(http_parms)))
    next(req)
    gen=gen_func(req,influx_parms)
    next(gen)
    return gen

def init_getter(opts):
    http_parms=[(k,v) for (k,v) in opts.items() if k in ["host", "port", "timeout"]]
    influx_parms=[(k,v) for (k,v) in opts.items() if k in ["u","p","precision","db"] and v ]
    return _create_gens(influx_getter, http_parms, influx_parms)

def init_poster(opts):
    http_parms=[(k,v) for (k,v) in opts.items() if k in ["host", "port", "timeout"]]
    influx_parms=[(k,v) for (k,v) in opts.items() if k in ["u","p","precision","db"] and v ]
    return _create_gens(influx_poster, http_parms, influx_parms)

def influxc_params(opt):

    opt.add_option("-s", "--host", default="localhost", help="destination server [default: %default]" )
    opt.add_option("-l", "--port", default=10086, type="int", help="destination port [default: %default]" )
    opt.add_option("-t", "--timeout", default=3000, help="timeout for http request [default: %default]" )

    opt.add_option("-u", "--user", default="root", help="user [default: %default]" )
    opt.add_option("-p", "--pswd", default="root", help="pswd [default: %default]" )
    opt.add_option("-d", "--db", default=None, help="dbname [default: %default]" )
    opt.add_option("-g", "--precision", default="s", help="precision [default: %default]" )


CMDS={
        "default": ( "SHOW DATABASES", "SHOW MEASUREMENTS",),
        "db": ("SHOW DATABASES",),
        "ms": ("SHOW MEASUREMENTS",),
        "mkdb": ("CREATE DATABASE {0}", "SHOW DATABASES",),
}

def parsed_opts():
    import optparse
    import os
    opt = optparse.OptionParser("{0}".format(CMDS))
    common.common_params(opt)
    influxc_params(opt)

    opt.add_option("-W", "--write", default=False, action="store_true", help="write points [default: %default]" )


    (opts, args)= opt.parse_args()
    return dict(vars(opts).items() + [("args", args)])


def main(opts):
    logger.debug("{0}".format(opts))

    w=init_poster(opts)
    r=init_getter(opts)

    if opts["write"]:
        logger.debug(jsonpretty( w.send("\n".join(opts["args"])) ))
        return
    if not opts["args"] or opts["args"][0] in CMDS:
        for query in CMDS[opts["args"][0] if opts["args"] else "default"]:
            stdout.info(jsonpretty( r.send(query.format(*opts["args"][1:]))))
    else:
        for query in opts["args"]:
            stdout.info(jsonpretty( r.send(query)))


if __name__ == '__main__':

    opts = parsed_opts()
    logger.setLevel(opts['loglevel'])
    stdout.setLevel(opts['stdoutlevel'])
    if opts['prof']:
      import cProfile
      cProfile.run('main(opts)')
      sys.exit(0)
    main(opts)

