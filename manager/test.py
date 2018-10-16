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

# data = {}
# for i in range(10000000):
#     data[i] = 'abc ' + str(i)

data = []
for i in range(100):
    # _str = {'how': 1, 'are': 1, 'you': 1, 'i': 1, 'am': 1, 'fine': 1, 'blar': int('{}'.format(t))}
    _str = {'how': 1, 'are': 1, 'you': 1, 'fine': 1, 'blar': i}
    data.append(_str)

s1 = time.clock()
objs = [(str(data.index(i)), Simhash(i)) for i in data]
print objs
index = SimhashIndex(objs, k=3)
s2 = time.clock()
print '生成指纹建立索引耗时{}'.format(s2-s1)
print index.bucket_size()

s3 = time.clock()
sim1 = Simhash({'how': 2, 'are': 2, 'you': 3, 'fine': 4, 'thanks': 100})
print index.get_near_dups(sim1)
s4 = time.clock()
print '新内容计算simhash及查询耗时{}'.format(s4-s3)

s5 = time.clock()
index.add('test', sim1)
print index.get_near_dups(sim1)
s6 = time.clock()
print '重新插入内存及查询耗时{}'.format(s6-s5)