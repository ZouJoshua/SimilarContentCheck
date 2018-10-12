#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author  : Joshua
@Time    : 2018/10/12 17:16
@File    : similarity_check.py
@Desc    : 
"""

import logging

from fingerprints_calculation.simhash import Simhash
from fingerprints_storage.simhash_index import SimhashIndex
from similarity_calculation.hamming_distance import HammingDistance

class SimilarityCheck(object):

    def __init__(self):
        pass

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
            logging.debug('key:%s', key)
            if len(dups) > 200:
                logging.warning('Big bucket found. key:%s, len:%s', key, len(dups))

            for dup in dups:
                sim2, obj_id = dup.split(',', 1)
                sim2 = Simhash(long(sim2, 16), self.hashbits)

                d = HammingDistance().distance(sim2)
                if d <= self.k:
                    ans.add(obj_id)
        return list(ans)