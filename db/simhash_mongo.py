#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author  : Joshua
@Time    : 2018/10/19 16:30
@File    : simhash_mongo.py
@Desc    : simhash storage of fingerprint and simhash inverted index with mongodb
"""

import sys
import os
from os.path import dirname
_dirname = dirname(os.path.realpath(__file__))
sys.path.append(dirname(_dirname))

import time
from mongoengine import register_connection
from setting import simhash_mongodb_config
from mongoengine import Document, IntField
from mongoengine import StringField

register_connection(**simhash_mongodb_config)

# _retry = 0
# _status = False
# while not _status and _retry <= 3:
#     try:
#         connect('simhash', host='mongodb://localhost:27017/simhash_invert_index')
#         _status = True
#     except:
#         print("连接失败，正在重试")
#         _status = False
#         _retry += 1
#         time.sleep(2)
#         if _retry == 4:
#             raise Exception("Mongodb连接失败，请检查")

class SimhashInvertedIndex(Document):
    """
    simhash inverted index
    """
    obj_id = StringField()
    key = StringField()
    simhash_value_obj_id = StringField()  # hash_value,obj_id
    add_time = IntField(default=int(time.time()))
    # hash_value = StringField()  # OverflowError: MongoDB can only handle up to 8-byte ints
    # simhash_caches_index = ListField(StringField())  # hash_value,obj_id
    # hash_type = StringField()
    # update_time = DateTimeField(default=int(time.time())
    # last_days = IntField(default=0)
    meta = {
        'db_alias': 'news',
        'strict': False,
        # 'index_background': True ,
        "collection": "simhash_invert_index",
        "indexes": [
            "key",
            "simhash_value_obj_id",
            "-add_time",
            # "-update_time",
            # "last_days",
            "obj_id",
            # "hash_value",
            # "hash_type",
            {
                    "fields": ["key", 'simhash_value_obj_id'],
                    "unique":True,
            },
        ]
    }

    def __str__(self):

        return 'obj_id:{}'.format(self.obj_id)


def get_all_simhash(SimhashInvertedIndex):
    # print('db:{}\ncount: {} records'.format(SimHashCache._meta['db_alias'], len(records)))
    return list(SimhashInvertedIndex.objects.all())

def get_simhash_count(SimhashInvertedIndex):

    return len(list(SimhashInvertedIndex.objects.all()))

if __name__ == '__main__':
    all = get_all_simhash(SimhashInvertedIndex)
    # print(all)
    # objs = list()
    # for i in all:
    #     objs.append((i['obj_id']))
    # print(objs)
    # SimhashInvertedIndex.objects(obj_id='test1').delete()
