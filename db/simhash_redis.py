#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author  : Joshua
@Time    : 2018/10/22 17:25
@File    : simhash_redis.py
@Desc    : 
"""

import time

from redis import StrictRedis, ConnectionPool
from setting import SAVE_DAYS

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
            raise Exception('Database connection failed...')
        else:
            return r

    def add(self, name, timenode, value):

        return self.redis.zadd(name, timenode, value)

    def get_values(self, name):

        now = int(time.time())
        timeline = now - 3600 * 24 * SAVE_DAYS
        self.redis.zremrangebyscore(name, 0, timeline)
        # print(self.redis.zrangebyscore(name, 0, timeline))
        return self.redis.zrange(name, timeline, now)


    def get_num(self,name):
        return self.redis.zcard(name)

    def delete(self, name, value):
        return self.redis.zrem(name, value)

    def flushdb(self):
        return self.redis.flushdb()

    def count(self, pattern='*'):

        names = self.redis.keys(pattern=pattern)
        all_count = {}
        for name in names:
            all_count[name] = self.get_num(name)

        return all_count

    @property
    def status(self):
        return self.redis.dbsize()

if __name__ == '__main__':
    redis = SimhashRedis()
    redis.add('x1', 1540980776, 'test1')
    redis.add('x1', 1540980746, 'test2')
    redis.add('x1', 1540980726, 'test3')

    for key in redis.redis.keys(pattern='*'):
        print(key)
        s = redis.get_values(key)
        print(s)

