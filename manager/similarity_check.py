#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author  : Joshua
@Time    : 2018/10/29 20:54
@File    : similarity_check.py
@Desc    :
"""

import time
from queue import Queue

from db.simhash_mongo import SimHashCache, SimhashInvertedIndex, get_all_simhash
from db.simhash_redis import SimhashRedis
from extract_features.extract_features_participle import Participle
from extract_features.extract_features_tfidf import get_keywords_tfidf
from fingerprints_calculation.simhash import Simhash
from fingerprints_storage.simhash_index_redis import SimhashIndexWithRedis
from utils.logger import Logger
from setting import PROJECT_LOG_FILE
import json

logger = Logger('simhash', log2console=False, log2file=True, logfile=PROJECT_LOG_FILE).get_logger()

class InitDB(object):

    def __init__(self):

        self.redis = SimhashRedis()
        self.simhashcache = SimHashCache
        self.simhash_inverted_index = SimhashInvertedIndex

        self.objs = [(obj[0], obj[1]) for obj in self.get_simhash_from_mongodb(self.simhashcache)]
        self.invert_index = [(obj[0], obj[1]) for obj in self.get_inverted_index_from_mongodb(self.simhash_inverted_index)]

        self.db = SimhashIndexWithRedis(self.simhashcache, self.simhash_inverted_index, self.redis)

        if not self.objs:
            self.redis = self.redis.flushdb()
            logger.info("初始化 Redis...")

        if self.objs and self.invert_index:
            s4 = time.clock()
            for ii in self.invert_index:
                self.redis.add(ii[0], ii[1])
            s5 = time.clock()
            logger.info("从 MongoDB 加载数据到 Redis...{}s".format(s5-s4))

    @staticmethod
    def get_simhash_from_mongodb(db):

        records = get_all_simhash(db)
        for record in records:
            yield list([record['obj_id'], record['hash_value'], record['last_days']])

    @staticmethod
    def get_inverted_index_from_mongodb(db):
        records = get_all_simhash(db)
        for record in records:
            yield list([record['key'], record['simhash_value_obj_id']])

class Check(object):

    def __init__(self, text_id, text, db):
        self.text_id = text_id
        self.text = text
        self.db = db

    def _extract_features(self, func='participle'):

        if func == 'participle':
            keywords = Participle().get_text_feature(self.text)
        elif func == 'tfidf':
            keywords = get_keywords_tfidf(self.text)
        else:
            raise Exception('Please provide a custom function ')

        return keywords

    def check_similarity(self):

        s1 = time.clock()
        keywords = self._extract_features()
        s2 = time.clock()
        logger.info("Text_id:{} 分词耗时...{}s".format(self.text_id, (s2 - s1)))
        simhash = Simhash(keywords)
        s3 = time.clock()
        logger.info("计算 Text_id:{}指纹耗时...{}s".format(self.text_id, (s3 - s2)))
        s6 = time.clock()
        dups_list = self.db.get_near_dups(simhash)
        s7 = time.clock()
        logger.info("查找 Text_id:{}耗时...{}s".format(self.text_id, (s7-s6)))
        self.db.add(obj_id=self.text_id, simhash=simhash)
        logger.info('Add Text_id:{} to db...'.format(self.text_id))

        return dups_list


def update_db(init_db, keep_days=15):

    return _check_mongodb(init_db, keep_days=keep_days)

def _check_mongodb(init_db, keep_days=30):

    s = time.time()
    logger.info('正在更新数据库')
    f_mongo = init_db.get_simhash_from_mongodb(init_db.simhashcache)
    _e1 = time.time()
    logger.info('读取数据库耗时{}'.format(_e1 - s))
    for fingerprint in f_mongo:
        init_db.db.add(fingerprint[0], fingerprint[1])
        if fingerprint[2] >= keep_days:
            init_db.db.delete(obj_id=fingerprint[0], simhash=fingerprint[1])
    e = time.time()
    logger.info('更新数据库耗时{}'.format(e - _e1))
    return init_db

def work(task_queue, result_queue, init_db):

    UPDATE_FREQUENCY = 30
    start = time.time()
    i = 0
    while True:

        if task_queue.qsize():
            i += 1
            item = task_queue.get()
            text, text_id = item
            dups_list = Check(text, text_id, init_db.db).check_similarity()
            result_queue.put({text_id: dups_list})
            if i > 10000:
                break
            if time.time() - start > UPDATE_FREQUENCY:
                init_db.objs = [(obj[0], obj[1]) for obj in init_db.get_simhash_from_mongodb(init_db.simhashcache)]
                init_db = update_db(init_db)
                print(len(init_db.objs))
                start = time.time()
        else:
            print('队列没任务')
            break

    while not result_queue.empty():
        print(result_queue.get())


def get_task(task_queue, filepath):
    with open(filepath, encoding='utf-8') as f:
        lines = f.readlines()
        for line in lines[10000:]:
            d = json.loads(line)
            text_id = d['resource_id']
            text = d['html']
            task_queue.put((text_id, text))
    return task_queue


if __name__ == '__main__':
    filepath = r'C:\Users\zoushuai\Desktop\new1_json\part-00000'
    init_db = InitDB()
    task_queue = Queue()
    result_queue = Queue()
    queue = get_task(task_queue, filepath)
    print(queue.qsize())
    work(queue, result_queue, init_db)