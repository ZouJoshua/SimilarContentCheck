#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author  : Joshua
@Time    : 2018/10/29 15:02
@File    : test_similarity_check.py
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

def get_task(task_queue, filepath):
    with open(filepath, encoding='utf-8') as f:
        line = f.readline()
        d = json.loads(line)
        text_id = d['resource_id']
        text = d['html']
        task_queue.put((text_id, text))
    return task_queue

if __name__ == '__main__':
    task_queue = Queue()
    filepath = r'C:\Users\zoushuai\Desktop\new1_json\part-00000'
    task_queue = get_task(task_queue, filepath)
    print(task_queue.get())


