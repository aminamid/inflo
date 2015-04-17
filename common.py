#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import datetime
import itertools
import re

def concat( lsts ):
        return list(itertools.chain(*lsts))

def gentime():
        return datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

def genlogtime():
        return datetime.datetime.now().strftime("%Y%m%d%H%M%S")

def setM(obj, l):
    for m in l:
        getattr(obj, m[0])(*m[1])
    return obj

# http://taichino.com/programming/1538

class regex_dict(dict):
  def __init__(self, items=None):
    for key, val in items.items():
      self.__setitem__(key, val)

  def __getitem__(self, item):
    try:
      return super(self.__class__, self).__getitem__(item)
    except:
      for key, val in self.items():
          rslt=key.search(item) if isinstance(key, re._pattern_type) else None
          if rslt: return (val,rslt)
      raise KeyError('key not found for>%s<' % item)

  def __setitem__(self, item_key, item_val):
    try:
      if isinstance(item_key, str):
        item_key = re.compile(item_key)
    except:
      pass
    super(self.__class__, self).__setitem__(item_key, item_val)


from logging import getLogger, StreamHandler, Formatter
def loginit(logname, format="%(message)s", stream=sys.stderr, level=15, datefmt="%Y/%m/%dT%H:%M:%S" ):
    return setM(getLogger(logname), [
        ("setLevel", [level]),
        ("addHandler", [setM(StreamHandler(stream),[("setFormatter", [Formatter(fmt=format,datefmt=datefmt)])])])
      ])

import functools
def traclog(logger):
    def recvfunc(f):
        @functools.wraps(f)
        def _f(*args, **kwargs):
            logger.debug("ENTER:{0} {1}".format( f.__name__, kwargs if kwargs else args))
            result = f(*args, **kwargs)
            logger.debug("RETRN:{0} {1}".format( f.__name__, result))
            return result
        return _f
    return recvfunc

