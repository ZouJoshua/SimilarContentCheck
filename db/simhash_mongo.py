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
    dup_obj_ids = ListField(StringField())
    dup_count = IntField(default=0)
    add_time = DateTimeField(default=datetime.datetime.now())
    update_time = DateTimeField(default=datetime.datetime.now())

    meta = {
        'db_alias': 'simhash',
        'strict': False,
        "collection": "simhash_text",
        "indexes": [
            "obj_id",
            "-add_time",
            "-update_time",
            "hash_type",
            "-dup_count",
            {
                "fields": ["obj_id", "hash_type"],
                "unique":True,
            },
        ],

    }

    def __str__(self):

        return 'obj_id:{}'.format(self.obj_id)

    def get_all_dup_ids(self):

        return [dup for dup in self.dup_obj_ids]


class SimhashInvertedIndex(Document):
    """
    simhash inverted index
    """
    key = StringField()
    # simhash_caches_index = ListField(StringField())  # 倒排相似hash索引 hash_value,obj_id
    simhash_value_obj_id = StringField()  # hash索引 hash_value,obj_id
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


if __name__ == '__main__':
    simhashcache = SimHashCache().get_all_dup_ids()
    print(simhashcache)