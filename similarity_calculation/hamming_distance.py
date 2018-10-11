#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author  : Joshua
@Time    : 2018/10/9 10:24
@File    : hamming_distance.py
@Desc    : 
"""

class HammingDistance(object):

    def __init__(self, fingerprints, hashbits):
        """Calculate Hamming distance
         Args:
             fingerprints: the fingerprints of an article
             hashbits: the dimensions of fingerprints
        """
        self.fingerprints = fingerprints
        self.hashbits = hashbits

    def distance(self, another_fingerprints):
        assert self.hashbits == another_fingerprints.hashbits
        x = (self.fingerprints ^ another_fingerprints.fingerprints) & ((1 << self.hashbits) - 1)
        tot = 0
        while x:
            tot += 1
            x &= x - 1
        return tot

    def similarity(self, another_fingerprints):
        a = float(self.fingerprints)
        b = float(another_fingerprints)
        if a > b:
            return b / a
        return a / b