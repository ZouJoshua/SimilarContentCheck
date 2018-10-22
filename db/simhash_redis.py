#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author  : Joshua
@Time    : 2018/10/22 17:25
@File    : simhash_redis.py
@Desc    : 
"""

from redis import StrictRedis, ConnectionPool
from setting import REDIS_HOST,REDIS_PORT

class SimhashRedis(object):

    def __init__(self, redis_host='localhost', redis_port=6379, redis_db=0, redis_pw=''):
        self._host = redis_host
        self._port = redis_port
        self._db = redis_db
        self._password = redis_pw
        self.redis = self._redis_conn()

    def _redis_conn(self):
        try:
            pool = ConnectionPool(host=self._host, port=self._port, db=self._db, password=self._password)
            r = StrictRedis(connection_pool=pool)
        except Exception as e:
            raise e
        return r

    def add(self, name, value):
        return self.redis.sadd(name, value)

    def delete(self,name, value):
        return self.redis.srem(name, value)

    def status(self):
        return self.redis.dbsize()

    def get(self, key):
        pass

if __name__ == '__main__':
    redis = SimhashRedis()
    id = {"simhash_obj_id": "b67ed5bc9bde424e,test4"}
    test = {"keys": ["424e:0", "9bde:1", "d5bc:2", "a74c:3"]}
    for key in test.get('keys'):
        redis.add(key,id.get('simhash_obj_id'))
        # print(redis.get(key))