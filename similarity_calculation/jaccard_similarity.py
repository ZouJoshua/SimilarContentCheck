#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author  : Joshua
@Time    : 2018/10/9 10:25
@File    : jaccard_similarity.py
@Desc    : 
"""

class JaccardSim(object):

    def __init__(self):
        pass

    def similarity(self, str_a, str_b):
        """Calculate Jaccard Similarity of two fingerprints
        Args:

        """

        seta = self.splitWords(str_a)[1]
        setb = self.splitWords(str_b)[1]

        sa_sb = 1.0 * len(seta & setb) / len(seta | setb)

        return sa_sb