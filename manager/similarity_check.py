#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author  : Joshua
@Time    : 2018/10/29 20:54
@File    : similarity_check.py
@Desc    : main
"""

import os
import sys
from os.path import dirname

sys.path.append(dirname(dirname(os.path.realpath(__file__))))

import time

from db.simhash_mongo import SimhashInvertedIndex, get_all_simhash
from db.simhash_redis import SimhashRedis
from extract_features.extract_features_participle import Participle
from extract_features.extract_features_tfidf import get_keywords_tfidf
from fingerprints_calculation.simhash import Simhash
from fingerprints_storage.simhash_index_redis import SimhashIndexWithRedis
from setting import PROJECT_LOG_FILE
from utils.logger import Logger
import logging

logger = Logger('simhash', log2console=True, log2file=True, logfile=PROJECT_LOG_FILE).get_logger()

class InitDB(object):
    """Init db and data"""
    def __init__(self, load_data_from_mongo_to_redis=True, logger=None):

        if logger is None:
            self.log = logging.getLogger("simhash")
        else:
            self.log = logger
        self.redis = SimhashRedis()
        self.mongo = SimhashInvertedIndex

        self.invert_index = self.mongo.objects.first()
        if not self.invert_index:
            self.redis.flushdb()
            self.log.info('Initializing Redis...')
        else:
            s4 = time.clock()
            if not load_data_from_mongo_to_redis:
                self.redis.flushdb()
                for i in self.get_inverted_index_from_mongodb(self.mongo):
                    self.redis.add(i[2], i[1], i[3])
                s5 = time.clock()
                self.log.info('Loading data from MongoDB to Redis...{}s'.format(s5-s4))
            self.log.info('Loading data from redis rdb to Redis...')

        self.siwr = SimhashIndexWithRedis(self.mongo, self.redis, logger=self.log)

    @staticmethod
    def get_inverted_index_from_mongodb(db):
        records = get_all_simhash(db)
        for record in records:
            yield list([record['obj_id'], record['add_time'], record['key'], record['simhash_value_obj_id']])

class Check(object):
    """main function"""
    def __init__(self, text_id, text, siwr, logger=None):
        self.text_id = text_id
        self.text = text
        self.siwr = siwr

        if logger is None:
            self.log = logging.getLogger("simhash")
        else:
            self.log = logger

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
        self.log.info('Text_id:{} Word segmentation time...{}s'.format(self.text_id, (s2 - s1)))
        simhash = Simhash(keywords)
        s3 = time.clock()
        self.log.info('Text_id:{} Calculate fingerprint time...{}s'.format(self.text_id, (s3 - s2)))
        s6 = time.clock()
        dups_list = self.siwr.get_near_dups(simhash)
        s7 = time.clock()
        self.log.info('Text_id:{} Find time...{}s'.format(self.text_id, (s7-s6)))
        self.siwr.add(obj_id=self.text_id, simhash=simhash)
        self.log.info('Text_id:{} Add to db...'.format(self.text_id))

        return dups_list, self.siwr

class UpdateDB(object):

    def __init__(self, db, logger=None):
        self.now = int(time.time())
        if isinstance(db, InitDB):
            self.db = db
            self.redis = db.redis
            self.mongo = db.mongo
        else:
            raise Exception('Wrong type of db...')

        if logger is None:
            self.log = logging.getLogger("simhash")
        else:
            self.log = logger

    def update_db(self, keep_days=15):

        return self._check_mongodb(keep_days=keep_days)

    def _check_mongodb(self, keep_days=30):
        self.log.info('Updating database...')

        self.log.info('Redis has {} keys'.format(self.redis.status))

        self.redis.flushdb()
        self.log.info('Now redis have been cleaned {} keys'.format(self.redis.status))
        timeline = self.now - keep_days * 3600 * 24
        # timeline = self.now - 400
        _e2 = time.time()
        for i in self.mongo.objects(add_time__lte=timeline):
            i.delete()
            i.save()
        _e3 = time.time()
        self.log.info('Delete timeout data takes...{}s'.format(_e3 - _e2))
        f_mongo_update = self.db.get_inverted_index_from_mongodb(self.mongo)
        for fingerprint in f_mongo_update:
            self.redis.add(fingerprint[2], fingerprint[1], fingerprint[3])
        _e = time.time()
        self.log.info('Time-consuming data after loading updates...{}s'.format(_e - _e3))
        return self.db

if __name__ == '__main__':
    import json
    from queue import Queue

    def get_task(task_queue, filepath):
        with open(filepath, encoding='utf-8') as f:
            lines = f.readlines()
            for line in lines[:2000]:
                d = json.loads(line)
                text_id = d['resource_id']
                text = d['html']
                task_queue.put((text_id, text))
        return task_queue

    def work_with_mongo_redis(task_queue, result_queue):
        _db = InitDB(load_data_from_mongo_to_redis=False, logger=logger)
        init_db = UpdateDB(db=_db, logger=logger).update_db()
        print(init_db)
        i = 0
        while True:

            if task_queue.qsize():
                i += 1
                item = task_queue.get()
                text_id, text = item
                dups_list, _db = Check(text_id, text, init_db.siwr).check_similarity()
                result_queue.put({text_id: dups_list})
                print({text_id: dups_list})
                # init_db.siwr = _db
                # if i > 10000:
                #     break
            else:
                print('队列没任务')
                break

        while not result_queue.empty():
            print(result_queue.get())

    def work_with_redis(task_queue, result_queue):

        init_db = InitDB(logger=logger)
        while True:

            if task_queue.qsize():
                item = task_queue.get()
                text_id, text = item
                dups_list, _db = Check(text_id, text, init_db.siwr, logger=logger).check_similarity()
                print({text_id: dups_list})
                result_queue.put({text_id: dups_list})
            else:
                print('队列没任务')
                break

    filepath = r'C:\Users\zoushuai\Desktop\new1_json\part-00007'
    task_queue = Queue()
    result_queue = Queue()
    queue = get_task(task_queue, filepath)
    print(queue.qsize())
    # work_with_mongo_redis(queue, result_queue)
    work_with_redis(queue, result_queue)
    # db = InitDB(logger=logger)
    # update = UpdateDB(db=db, logger=logger).update_db()
    # print(update)