#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author  : Joshua
@Time    : 2018/10/19 16:30
@File    : simhash_mongo.py
@Desc    : 
"""

import datetime
import json


from mongoengine import Document, IntField
from mongoengine import StringField, ListField, DateTimeField
from mongoengine import register_connection
from setting import simhash_mongodb_config
register_connection(**simhash_mongodb_config)

# print('simhash db\n', json.dumps(simhash_mongodb_config, indent=4))


class SimHashCache(Document):
    obj_id = StringField()
    hash_value = StringField()  # OverflowError: MongoDB can only handle up to 8-byte ints
    text = StringField()
    hash_type = StringField()
    add_time = DateTimeField(default=datetime.datetime.now())
    update_time = DateTimeField(default=datetime.datetime.now())
    last_days = IntField(default=0)

    meta = {
        'db_alias': 'simhash',
        'strict': False,
        "collection": "simhash_fingerprint",
        "indexes": [
            "obj_id",
            "-add_time",
            "-update_time",
            "hash_type",
            "last_days",
            {
                "fields": ["obj_id", "hash_type"],
                "unique":True,
            },
        ],

    }

    def __str__(self):

        return 'obj_id:{}'.format(self.obj_id)

class SimhashInvertedIndex(Document):
    """
    simhash inverted index
    """
    key = StringField()
    # simhash_caches_index = ListField(StringField())  # hash_value,obj_id
    simhash_value_obj_id = StringField()  # hash_value,obj_id
    hash_type = StringField()
    meta = {
        'db_alias': 'simhash',
        'strict': False,
        # 'index_background': True ,
        "collection": "simhash_invert_index",
        "indexes": [
            "key",
            "simhash_value_obj_id",
            "hash_type",
            {
                    "fields": ["key", 'simhash_value_obj_id'],
                    "unique":True,
            },
        ]
    }


def get_all_simhash(SimHashCache):
    # print('db:{}\ncount: {} records'.format(SimHashCache._meta['db_alias'], len(records)))
    return list(SimHashCache.objects.all())

def get_simhash_count(SimHashCache):
    return len(list(SimHashCache.objects.all()))

if __name__ == '__main__':
    all = get_all_simhash(SimHashCache)
    print(all)
    objs = list()
    for i in all:
        objs.append((i['obj_id']))
    print(objs)
    SimHashCache.objects(obj_id='test1').delete()
