#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author  : Joshua
@Time    : 2018/10/9 10:24
@File    : hamming_distance.py
@Desc    : 
"""

class HammingDistance(object):

    def __init__(self, simhash, hashbits=64):
        """Calculate Hamming distance
         Args:
             simhash: an instance of Simhash
             hashbits: the dimensions of fingerprint
        """
        self.simhash = simhash
        self.hashbits = hashbits

    def distance(self, another_fingerprint):
        assert self.hashbits == another_fingerprint.hashbits
        x = (self.simhash.fingerprint ^ another_fingerprint.fingerprint) & ((1 << self.hashbits) - 1)
        tot = 0
        while x:
            tot += 1
            x &= x - 1
        return tot

    def similarity(self, another_fingerprint):
        a = float(self.simhash.fingerprint)
        b = float(another_fingerprint.fingerprint)
        if a > b:
            return '{}%'.format(b / a* 100, '0,02f')
        return '{}%'.format(a / b * 100, '0.02f')