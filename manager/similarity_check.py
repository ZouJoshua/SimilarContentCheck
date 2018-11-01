#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author  : Joshua
@Time    : 2018/10/29 20:54
@File    : similarity_check.py
@Desc    : main
"""

import sys
import os
from os.path import dirname
sys.path.append(dirname(dirname(os.path.realpath(__file__))))

import time

from db.simhash_mongo import SimhashInvertedIndex, get_all_simhash, get_simhash_count
from db.simhash_redis import SimhashRedis
from extract_features.extract_features_participle import Participle
from extract_features.extract_features_tfidf import get_keywords_tfidf
from fingerprints_calculation.simhash import Simhash
from fingerprints_storage.simhash_index_redis import SimhashIndexWithRedis
from setting import PROJECT_LOG_FILE
from utils.logger import Logger

logger = Logger('simhash', log2console=False, log2file=True, logfile=PROJECT_LOG_FILE).get_logger()

class InitDB(object):
    """Init db and data"""
    def __init__(self):

        self.redis = SimhashRedis()
        self.simhash_inverted_index = SimhashInvertedIndex

        self.invert_index = self.simhash_inverted_index.objects.first()
        if not self.invert_index:
            self.redis.flushdb()
            self.siwr = SimhashIndexWithRedis(self.simhash_inverted_index, self.redis)
            logger.info("Initializing Redis...")
        else:
            s4 = time.clock()
            self.redis.flushdb()
            for i in self.get_inverted_index_from_mongodb(self.simhash_inverted_index):
                self.redis.add(i[2], i[1], i[3])
            self.siwr = SimhashIndexWithRedis(self.simhash_inverted_index, self.redis)
            s5 = time.clock()
            logger.info("Loading data from MongoDB to Redis...{}s".format(s5-s4))

    @staticmethod
    def get_inverted_index_from_mongodb(db):
        records = get_all_simhash(db)
        for record in records:
            yield list([record['obj_id'], record['add_time'], record['key'], record['simhash_value_obj_id']])

class Check(object):
    """main function"""
    def __init__(self, text_id, text, siwr):
        self.text_id = text_id
        self.text = text
        self.siwr = siwr

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
        logger.info('Text_id:{} Word segmentation time...{}s'.format(self.text_id, (s2 - s1)))
        simhash = Simhash(keywords)
        s3 = time.clock()
        logger.info('Text_id:{} Calculate fingerprint time...{}s'.format(self.text_id, (s3 - s2)))
        s6 = time.clock()
        dups_list = self.siwr.get_near_dups(simhash)
        s7 = time.clock()
        logger.info('Text_id:{} Find time...{}s'.format(self.text_id, (s7-s6)))
        self.siwr.add(obj_id=self.text_id, simhash=simhash)
        logger.info('Text_id:{} Add to db...'.format(self.text_id))

        return dups_list, self.siwr


def update_db(init_db, keep_days=15):

    return _check_mongodb(init_db, keep_days=keep_days)

def _check_mongodb(init_db, keep_days=30):
    logger.info('Updating database...')
    redis = SimhashRedis()
    logger.info('Redis has {} keys'.format(redis.status))

    redis.flushdb()
    logger.info('Now redis have been cleaned {} keys'.format(redis.status))

    s = time.time()
    f_mongo = init_db.get_inverted_index_from_mongodb(SimhashInvertedIndex)
    _e1 = time.time()
    logger.info('Reading database time...{}s'.format(_e1 - s))
    for fingerprint in f_mongo:
        init_db.siwr.update(fingerprint[0])
    _e2 = time.time()
    logger.info('Update date time...{}s'.format(_e2 - _e1))
    f_mongo_new = init_db.get_inverted_index_from_mongodb(SimhashInvertedIndex)
    for fingerprint in f_mongo_new:
        if fingerprint[1] >= keep_days:
            init_db.siwr.delete(obj_id=fingerprint[0], simhash=fingerprint[3].split(',')[0])
    _e3 = time.time()
    logger.info('Delete timeout data time...{}s'.format(_e3 - _e2))
    f_mongo_update = init_db.get_inverted_index_from_mongodb(SimhashInvertedIndex)
    for fingerprint in f_mongo_update:
        redis.add(fingerprint[2], fingerprint[3])
    _e = time.time()
    logger.info('Time-consuming data after loading updates...{}s'.format(_e - _e3))
    return init_db

if __name__ == '__main__':
    import json
    from queue import Queue

    def get_task(task_queue, filepath):
        with open(filepath, encoding='utf-8') as f:
            lines = f.readlines()
            for line in lines[:20000]:
                d = json.loads(line)
                text_id = d['resource_id']
                text = d['html']
                task_queue.put((text_id, text))
        return task_queue

    def work_with_mongo_redis(task_queue, result_queue):
        init_db = InitDB()
        UPDATE_FREQUENCY = 360000
        start = time.time()
        i = 0
        while True:

            if task_queue.qsize():
                i += 1
                item = task_queue.get()
                text_id, text = item
                dups_list, _db = Check(text_id, text, init_db.siwr).check_similarity()
                result_queue.put({text_id: dups_list})
                # init_db.siwr = _db
                # if i > 10000:
                #     break
                if time.time() - start > UPDATE_FREQUENCY:
                    init_db = update_db(init_db)
                    start = time.time()
            else:
                print('队列没任务')
                break

        while not result_queue.empty():
            print(result_queue.get())

    def work_with_redis(task_queue, result_queue):

        siwr = SimhashIndexWithRedis(SimhashInvertedIndex, SimhashRedis())
        while True:

            if task_queue.qsize():
                item = task_queue.get()
                text_id, text = item
                dups_list, _db = Check(text_id, text, siwr).check_similarity()
                print(dups_list)
                result_queue.put({text_id: dups_list})
            else:
                print('队列没任务')
                break

    filepath = r'C:\Users\zoushuai\Desktop\new1_json\part-00006'
    task_queue = Queue()
    result_queue = Queue()
    queue = get_task(task_queue, filepath)
    print(queue.qsize())
    work_with_mongo_redis(queue, result_queue)
    # work_with_redis(queue, result_queue)