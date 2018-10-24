#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author  : Joshua
@Time    : 2018/10/15 19:34
@File    : test11.py
@Desc    : 
"""

from fingerprints_calculation.simhash import Simhash
from fingerprints_storage.simhash_index import SimhashIndex
from extract_features.extract_features_tfidf import get_keywords_tfidf
from extract_features.extract_features_participle import Participle
from fingerprints_storage.simhash_index_mongo import SimhashIndexWithMongo
from fingerprints_storage.simhash_index_redis import SimhashIndexWithRedis
from db.simhash_mongo import SimHashCache, get_all_simhash
from queue import Queue
import threading
import time


class test_SimilarityCheck(object):

    def __init__(self, hashbits=64, k=3):
        self.hashbits = hashbits
        self.k = k
        self._SimHashCache = SimHashCache
        self.redis = SimhashRedis()
        self.objs = [(obj[0], obj[1]) for obj in self.get_fingerprint_from_mongodb()][:100]
        print(len(self.objs))
        self.db = SimhashIndexWithRedis(self.objs)


        print(self.db.bucket_size())
        print(self.db)
        print(type(self.db))

    def get_fingerprint_from_mongodb(self):
        records = get_all_simhash(self._SimHashCache)
        for record in records:
            # print('{}|{}|{}'.format(record['obj_id'], record['hash_value'], record['last_days']))
            yield list([record['obj_id'], record['hash_value'], record['last_days']])

def _extract_features(text, func='participle'):

    if func == 'participle':
        keywords = Participle().get_text_feature(text)
    elif func == 'tfidf':
        keywords = get_keywords_tfidf(text)
    else:
        raise Exception('Please provide a custom function ')

    return keywords

def check_similarity(text, text_id):

    s1 = time.clock()
    keywords = _extract_features(text)
    s2 = time.clock()
    print("分词耗时**********{}s".format(s2 - s1))
    simhash = Simhash(keywords)
    s3 = time.clock()
    print("计算指纹耗时**********{}s".format(s3 - s2))
    s6 = time.clock()
    objs = [(obj[0], obj[1]) for obj in get_fingerprint_from_mongodb()]
    if not objs:
        add(obj_id=text_id, simhash=simhash)
    dups_list = self.db.get_near_dups(simhash)
    s7 = time.clock()
    print("查找耗时**********{}s".format(s7 - s6))
    self.db.add(obj_id=text_id, simhash=simhash)
    return dups_list

 def get_fingerprint_from_mongodb():
        records = get_all_simhash(self._SimHashCache)
        for record in records:
            # print('{}|{}|{}'.format(record['obj_id'], record['hash_value'], record['last_days']))
            yield list([record['obj_id'], record['hash_value'], record['last_days']])


if __name__ == '__main__':
    all = test_SimilarityCheck(SimHashCache)
