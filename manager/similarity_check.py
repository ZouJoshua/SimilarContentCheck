#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author  : Joshua
@Time    : 2018/10/12 17:16
@File    : similarity_check.py
@Desc    : 
"""

import time

from fingerprints_calculation.simhash import Simhash
from fingerprints_storage.simhash_index import SimhashIndex
from extract_features.extract_features_tfidf import get_keywords_tfidf
from extract_features.extract_features_participle import Participle


class SimilarityCheck(object):

    def __init__(self, hashbits=64, k=3):
        self.hashbits = hashbits
        self.k = k

    def test_check_similarity(self, text, text_id):
        s1 = time.clock()
        # keywords = get_keywords_tfidf(text)
        keywords = Participle().get_text_feature(text)
        s2 = time.clock()
        print("分词耗时**********{}s".format(s2-s1))
        simhash = Simhash(keywords)
        s3 = time.clock()
        print("计算指纹耗时**********{}s".format(s3-s2))
        objs = [(text_id, simhash)] * 5000000
        s4 = time.clock()
        index = SimhashIndex(objs, k=3)
        print(index.bucket)
        s5 = time.clock()
        print("加载进内存耗时**********{}s".format(s5-s4))
        s6 = time.clock()
        dups_list = index.get_near_dups(simhash)
        s7 = time.clock()
        print("查找耗时**********{}s".format(s7-s6))
        return dups_list


if __name__ == '__main__':
    text ="Natural language processing (NLP) is a field of computer science, artificial intelligence and computational linguistics concerned with the interactions between computers and human (natural) languages, and, in particular, concerned with programming computers to fruitfully process large natural language corpora. Challenges in natural language processing frequently involve natural language understanding, natural language generation (frequently from formal, machine-readable logical forms), connecting language and machine perception, managing human-computer dialog systems, or some combination thereof." \
    "The Georgetown experiment in 1954 involved fully automatic translation of more than sixty Russian sentences into English. The authors claimed that within three or five years, machine translation would be a solved problem.[2] However, real progress was much slower, and after the ALPAC report in 1966, which found that ten-year-long research had failed to fulfill the expectations, funding for machine translation was dramatically reduced. Little further research in machine translation was conducted until the late 1980s, when the first statistical machine translation systems were developed." \
    "During the 1970s, many programmers began to write conceptual ontologies, which structured real-world information into computer-understandable data. Examples are MARGIE (Schank, 1975), SAM (Cullingford, 1978), PAM (Wilensky, 1978), TaleSpin (Meehan, 1976), QUALM (Lehnert, 1977), Politics (Carbonell, 1979), and Plot Units (Lehnert 1981). During this time, many chatterbots were written including PARRY, Racter, and Jabberwacky。"
    s = time.clock()
    dups = SimilarityCheck().test_check_similarity(text=text, text_id='test1')
    e = time.clock()
    print('全程查找耗时{}'.format(e - s))
    print(dups)