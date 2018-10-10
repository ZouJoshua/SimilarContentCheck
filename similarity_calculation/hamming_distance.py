#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author  : Joshua
@Time    : 2018/10/9 10:24
@File    : hamming_distance.py
@Desc    : 
"""

class HammingDistance(object):

    def __init__(self, hash, hashbits):
        self.hash = hash
        self.hashbits = hashbits

    def distance(self, other_hash):
        assert self.hashbits == other_hash.hashbits
        x = (self.hash ^ other_hash.hash) & ((1 << self.hashbits) - 1)
        tot = 0
        while x:
            tot += 1
            x &= x - 1
        return tot

    def similarity(self, other_hash):
        a = float(self.hash)
        b = float(other_hash)
        if a > b:
            return b / a
        return a / b