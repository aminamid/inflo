#!/usr/bin/python
# -*- coding: utf-8 -*-
# http://taichino.com/programming/1538
 
import re
 
class regex_dict(dict):
  def __init__(self, items=None):
    for key, val in items.items():
      self.__setitem__(key, val)
 
  def __getitem__(self, item):
    try:
      return super(self.__class__, self).__getitem__(item)
    except:
      for key, val in self.items():
        if isinstance(key, re._pattern_type) and key.match(item):
          return val
      raise KeyError('key not found for %s' % item)
 
  def __setitem__(self, item_key, item_val):
    try:
      if isinstance(item_key, str):
        item_key = re.compile(item_key)
    except:
      pass
    super(self.__class__, self).__setitem__(item_key, item_val)
