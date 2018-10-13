#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author  : Joshua
@Time    : 2018/10/10 11:27
@File    : simhash_index.py
@Desc    : 
"""

import logging
import collections

class SimhashIndex(object):

    def __init__(self, objs, hashbits=64, k=3):
        """
        Args:
            objs: a list of (obj_id, simhash)
                obj_id is a string, simhash is an instance of Simhash
            hashbits: the same with the one for Simhash
            k: the tolerance
        """
        self.k = k
        self.hashbits = hashbits
        count = len(objs)
        logging.info('Initializing %s data.', count)
        # Put the fingerprints in memory
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
            v = '%x,%s' % (simhash.fingerprints, obj_id)

            self.bucket.setdefault(key, set())
            self.bucket[key].add(v)

    def delete(self, obj_id, simhash):
        assert simhash.hashbits == self.hashbits

        for key in self.get_keys(simhash):
            v = '%x,%s' % (simhash.fingerprints, obj_id)

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
            c = simhash.fingerprints >> offset & m
            yield '%x:%x' % (c, i)

    def bucket_size(self):
        return len(self.bucket)

if __name__ == '__main__':
    from fingerprints_calculation.simhash import Simhash
