#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author  : Joshua
@Time    : 2018/10/10 11:27
@File    : simhash_index.py
@Desc    : 
"""

import logging
import time
import collections
from fingerprints_calculation.simhash import Simhash
from similarity_calculation.hamming_distance import HammingDistance


class SimhashIndex(object):

    def __init__(self, objs, hashbits=64, k=3):
        """
        Args:
            objs: a list of (obj_id, simhash)
                obj_id is a string, simhash is an instance of Simhash or fingerprint of news
            hashbits: the same with the one for Simhash
            k: the tolerance
        """
        self.k = k
        self.hashbits = hashbits
        count = len(objs)
        logging.info('Initializing %s data.', count)
        # Put the fingerprint in memory
        self.bucket = collections.defaultdict(set)

        for i, q in enumerate(objs):
            if i % 10000 == 0 or i == count - 1:
                logging.info('%s/%s', i + 1, count)

            self.add(*q)

    def add(self, obj_id, simhash):
        """Building an inverted index
        Args:
            obj_id: a string
            simhash: an instance of Simhash
        """
        assert simhash.hashbits == self.hashbits

        for key in self.get_keys(simhash):
            v = '{:x},{}'.format(simhash.fingerprint, obj_id)
            self.bucket.setdefault(key, set())
            self.bucket[key].add(v)

    def delete(self, obj_id, simhash):
        assert simhash.hashbits == self.hashbits

        for key in self.get_keys(simhash):
            v = '{:x},{}'.format(simhash.fingerprint, obj_id)

            if v in self.bucket.get(key, set()):
                self.bucket[key].remove(v)

    @property
    def offsets(self):
        """
        You may optimize this method according to <http://www.wwwconference.org/www2007/papers/paper215.pdf>
        """
        return [self.hashbits // (self.k + 1) * i for i in range(self.k + 1)]

    def get_keys(self, simhash):
        """Block the hash value and build the key for the inverted index
        """
        for i, offset in enumerate(self.offsets):
            if i == len(self.offsets) - 1:
                m = 2 ** (self.hashbits - offset) - 1
            else:
                m = 2 ** (self.offsets[i + 1] - offset) - 1
            c = simhash.fingerprint >> offset & m
            yield '{:x}:{:x}'.format(c, i)

    @property
    def bucket_size(self):
        return len(self.bucket)

    def get_near_dups(self, simhash):
        """
        Args:
            simhash: an instance of Simhash
        Returns:
            return a list of obj_id, which is in type of str
        """
        assert simhash.hashbits == self.hashbits

        ans = set()

        for key in self.get_keys(simhash):
            dups = self.bucket.get(key, set())
            logging.debug('key:{}'.format(key))
            if len(dups) > 200:
                logging.warning('Big bucket found. key:%s, len:%s', key, len(dups))

            for dup in dups:
                sim2, obj_id = dup.split(',', 1)
                sim2 = Simhash(int(sim2, 16), self.hashbits)

                _sim1 = HammingDistance(simhash)
                d = _sim1.distance(sim2)
                rate = _sim1.similarity(sim2)
                if d <= self.k:
                    ans.add((obj_id, rate))
        return list(ans)

if __name__ == '__main__':
    data = []
    for i in range(10000):
        # _str = {'how': 1, 'are': 1, 'you': 1, 'i': 1, 'am': 1, 'fine': 1, 'blar': int('{}'.format(t))}
        _str = {'how': 1, 'are': 2, 'you': 3, 'fine': 4, 'blar': i, 'i': 1, 'am': 7, 'ok': 99,'reboot': 4}
        data.append(_str)

    s1 = time.clock()
    objs = [(str(data.index(i)), Simhash(i)) for i in data]
    index = SimhashIndex(objs, k=3)
    s2 = time.clock()
    print('生成指纹建立索引耗时{}'.format(s2 - s1))
    print(index.bucket_size)

    s3 = time.clock()
    sim1 = Simhash({'thanks': 2, 'are': 2, 'you': 3, 'fine': 4, 'blar': 10, 'ok': 3, 'reboot': 4})
    print(index.get_near_dups(sim1))
    print(len(index.get_near_dups(sim1)))
    s4 = time.clock()
    print('新内容计算simhash及查询耗时{}'.format(s4 - s3))

    s5 = time.clock()
    index.add('test', sim1)
    print(index.get_near_dups(sim1))
    print(len(index.get_near_dups(sim1)))
    s6 = time.clock()
    print('重新插入内存及查询耗时{}'.format(s6 - s5))
