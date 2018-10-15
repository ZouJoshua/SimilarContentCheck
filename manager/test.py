#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author  : Joshua
@Time    : 2018/10/15 19:34
@File    : test.py
@Desc    : 
"""

import re
from simhash import Simhash, SimhashIndex
import time

def get_features(s):
    width = 3
    s = s.lower()
    s = re.sub(r'[^\w]+', '', s)
    return [s[i:i + width] for i in range(max(len(s) - width + 1, 1))]

data = {}
for i in range(10000000):
    data[i] = 'abc' + str(i)
s1 = time.clock()
objs = [(str(k), Simhash(get_features(v))) for k, v in data.items()]
index = SimhashIndex(objs, k=3)
s2 = time.clock()
print '生成指纹耗时{}'.format(s2-s1)
print index.bucket_size()

s3 = time.clock()
s1 = Simhash(get_features(u'abcd111111'))
s4 = time.clock()
print '查询耗时{}'.format(s4-s3)
print index.get_near_dups(s1)

index.add('4', s1)
s5 = time.clock()
print '查询耗时{}'.format(s5-s4)
print index.get_near_dups(s1)