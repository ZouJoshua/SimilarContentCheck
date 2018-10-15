#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author  : Joshua
@Time    : 2018/10/12 17:16
@File    : similarity_check.py
@Desc    : 
"""

import logging
import time

from fingerprints_calculation.simhash import Simhash
from fingerprints_storage.simhash_index import SimhashIndex
from similarity_calculation.hamming_distance import HammingDistance


class SimilarityCheck(object):

    def __init__(self, bucket, hashbits=64, k=3):
        self.bucket = bucket
        self.hashbits = hashbits
        self.k = k

    def get_near_dups(self, simhash):
        """
        Args:
            simhash: an instance of Simhash
        Returns:
            return a list of obj_id, which is in type of str
        """
        assert simhash.hashbits == self.hashbits

        ans = set()

        for key in SimhashIndex(self.bucket).get_keys(simhash):
            dups = self.bucket.get(key, set())
            logging.debug('key:%s', key)
            if len(dups) > 200:
                logging.warning('Big bucket found. key:%s, len:%s', key, len(dups))

            for dup in dups:
                sim2, obj_id = dup.split(',', 1)
                sim2 = Simhash(long(sim2, 16), self.hashbits)

                d = HammingDistance(simhash.fingerprint).distance(sim2.fingerprint)
                if d <= self.k:
                    ans.add(obj_id)
        return list(ans)

if __name__ == '__main__':
    s = time.clock()
    t = 0
    objs = []
    for i in range(100):
        t += 1
        _str = {'how': 1, 'are': 1, 'you': 1, 'i': 1, 'am': 1, 'fine': 1, 'blar': int('{}'.format(t))}
        if t == 10:
            t = 0
        simhash = Simhash(_str)
        objs.append((str(i), simhash))
    with open('simhash.txt', 'w') as f:
        for i in objs:
            f.write('{},{}\n'.format(i[0], str(i[1].fingerprint)))
    m1 = time.clock()
    print '计算simhash耗时{}'.format(m1-s)
    index = SimhashIndex(objs, k=3)
    print index.bucket_size
    bucket = index.bucket
    print bucket
    m2 = time.clock()
    str1 = {'how': 1, 'are': 1, 'you': 1, 'i': 1, 'am': 1, 'fine': 1, 'blar': 11}
    sim1 = Simhash(str1)
    m3 = time.clock()
    print '计算一个simhash耗时{}'.format(m3-m2)
    check = SimilarityCheck(bucket, k=3)
    dups = check.get_near_dups(sim1)
    print dups
    e = time.clock()
    print '查找simhash耗时{}'.format(e-m3)