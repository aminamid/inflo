#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import datetime
import itertools
import re

def concat( ll ):
        return list(itertools.chain(*ll))

def nowstr(fmt="%Y-%m-%dT%H:%M:%S"):
        return datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

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


##  logging
def change_state(obj, method_arglist_tpls):
    for m_as in method_arglist_tpls:
        getattr(obj, m_as[0])(*m_as[1])
    return obj

from logging import getLogger, StreamHandler, Formatter
def loginit(logname, format="%(message)s", stream=sys.stderr, level=15, datefmt="%Y/%m/%dT%H:%M:%S" ):
    return change_state(getLogger(logname), [
        ("setLevel", [level]),
        ("addHandler", [change_state(
            StreamHandler(stream),[("setFormatter", [Formatter(fmt=format,datefmt=datefmt)])]
        )])
      ])

## logging functions

import functools
def traclog(logger):
    def recvfunc(f):
        @functools.wraps(f)
        def trac(*args, **kwargs):
            logger.log(5,"ENTER:{0} {1}".format( f.__name__, kwargs if kwargs else args))
            result = f(*args, **kwargs)
            logger.log(5,"RETRN:{0} {1}".format( f.__name__, result))
            return result
        return trac
    return recvfunc

