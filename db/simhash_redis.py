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

    def get(self, name):
        return self.redis.smembers(name)

    def delete(self, name, value):
        return self.redis.srem(name, value)

    def flushall(self):
        return self.redis.flushall()

    @property
    def status(self):
        return self.redis.dbsize()

if __name__ == '__main__':
    redis = SimhashRedis()
    id = {"simhash_obj_id": "b67ed5bc9bde424e_test4"}
    test = {"keys": ["424e:0", "9bde:1", "d5bc:2", "a74c:3"]}
    for key in test.get('keys'):
        redis.add(key, id.get('simhash_obj_id'))

    for i in test.get('keys'):
        lst = redis.get(i)

        for i in lst:
            if isinstance(i, bytes):
                i = i.decode()
            sim2, obj_id = i.split('_', 1)
            print(sim2)