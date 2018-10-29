#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author  : Joshua
@Time    : 2018/10/12 15:38
@File    : simhash_index_redis.py
@Desc    : simhash storage with mongodb and redis
"""

import datetime
import logging

from db.simhash_mongo2 import SimhashInvertedIndex
from db.simhash_redis import SimhashRedis
from fingerprints_calculation.simhash import Simhash
from similarity_calculation.hamming_distance import HammingDistance


class SimhashIndexWithRedis(object):

    def __init__(self, simhashinvertedindex, redis, objs=(), hashbits=64, k=3, hash_type='news'):
        """
        Args:
            redis: an instance of redis
            simhashinvertedindex : an instance of simhashinvertedindex(mongodb)
            simhashcache: an instance of simhashcache(mongodb)
            objs: a list of (obj_id, origin_text)
                obj_id is a string, simhash is an instance of Simhash
            hashbits: the same with the one for Simhash
            k: the tolerance
            hash_type: the hash type  of the text
        """
        self.k = k
        self.hashbits = hashbits
        self.hash_type = hash_type
        self.redis = redis
        self.simhash_inverted_index = simhashinvertedindex

        count = len(objs)
        logging.info('Initializing {} data.'.format(count))

        for i, q in enumerate(objs):
            if i % 10000 == 0 or i == count - 1:
                logging.info('{}/{}'.format(i + 1, count))
            self.add(*q)

    def _insert(self, obj_id=None, value=None):
        """Insert hash value into mongodb and redis
            data can  be text,{obj_id,text},  {obj_id,simhash}
        #TODO: The most time-consuming place to store and write databases
        """
        assert value != None
        if isinstance(value, str):
            simhash = Simhash(value=value, hashbits=self.hashbits)
        elif isinstance(value, Simhash):
            simhash = value
        else:
            raise Exception('Value not text or simhash')
        assert simhash.hashbits == self.hashbits

        # Cache raw text information
        if obj_id and simhash:

            # Store or update the cache to mongodb,cache invert index into mongodb and redis
            v = '{:x},{}'.format(simhash.fingerprint, obj_id)  # Convert to hexadecimal for compressed storage, which saves space and converts back when querying
            for key in self.get_keys(simhash):
                try:
                    invert_index = self.simhash_inverted_index(key=key, hash_type=self.hash_type,
                                                        simhash_value_obj_id=v)
                    add_time = invert_index.add_time
                    update_time = datetime.datetime.now()
                    invert_index.update_time = update_time
                    invert_index.last_days = (update_time - add_time).days
                    invert_index.obj_id = obj_id
                    invert_index.hash_value = '{:x}'.format(simhash.fingerprint)
                    invert_index.save()

                    self.redis.add(name=key, value=v)
                except Exception as e:
                    # print('%s,%s,%s' % (e, key, v))
                    pass

            return invert_index

    def _find(self, value, k=3):
        assert value != None

        if isinstance(value, str):
            simhash = Simhash(value=value, hashbits=self.hashbits)
        elif isinstance(value, Simhash):
            simhash = value
        else:
            raise Exception('value not text or simhash')

        assert simhash.hashbits == self.hashbits
        ans = set()
        for key in self.get_keys(simhash):
            simhash_list = self.redis.get_values(name=key)

            if len(simhash_list) > 200:
                logging.warning('Big bucket found. key:{}, len:{}'.format(key, len(simhash_list)))

            for simhash_cache in simhash_list:
                if isinstance(simhash_cache, bytes):
                    simhash_cache = simhash_cache.decode()
                # print(simhash_cache)
                try:
                    sim2, obj_id = simhash_cache.split(',', 1)
                    _sim2 = Simhash(int(sim2, 16), self.hashbits)

                    _sim1 = HammingDistance(simhash)
                    d = _sim1.distance(_sim2)

                    if d < k:
                        ans.add(obj_id)
                except Exception as e:
                    logging.warning('not exists {}'.format(e))
        return list(ans)

    def query_simhash_cache(self, obj_id):
        """Find similar objects by obj_id"""
        simhash_caches = self.simhash_inverted_index.objects.filter(obj_id__contains=obj_id)
        return simhash_caches

    def find_similiar(self, obj_id):
        simhash_caches = self.simhash_inverted_index.objects.filter(obj_id__contains=obj_id)
        return simhash_caches

    def delete(self, obj_id, simhash):
        """
        Args:
            obj_id: a string
            simhash: an instance of Simhash or str
        """
        if isinstance(simhash, str):
            simhash = Simhash(value=simhash, hashbits=self.hashbits)
        elif isinstance(simhash, Simhash):
            simhash = simhash
        else:
            logging.warning('simhash not str or an instance of Simhash')
            pass

        # delete simhash in mongodb
        try:
            self.simhash_inverted_index.objects(obj_id=obj_id).delete()
        except:
            pass
        # delete simhash in redis
        for key in self.get_keys(simhash):
            v = '{:x},{}'.format(simhash.fingerprint, obj_id)
            self.redis.delete(name=key, value=v)

    def add(self, obj_id, simhash):
        return self._insert(obj_id=obj_id, value=simhash)

    def get_near_dups(self, simhash):
        """
        Args:
            simhash: an instance of Simhash
        Returns:
            return a list of obj_id, which is in type of str
        """
        return self._find(simhash, self.k)

    def get_keys(self, simhash):
        for i, offset in enumerate(self.offsets):
            # m = (i == len(self.offsets) - 1 and 2 ** (self.hashbits - offset) - 1 or 2 ** (self.offsets[i + 1] - offset) - 1)
            if i == len(self.offsets) - 1:
                m = 2 ** (self.hashbits - offset) - 1
            else:
                m = 2 ** (self.offsets[i + 1] - offset) - 1
            c = simhash.fingerprint >> offset & m
            yield '{:x}:{:x}'.format(c, i)

    @property
    def offsets(self):
        return [self.hashbits // (self.k + 1) * i for i in range(self.k + 1)]

    @property
    def bucket_size(self):
        return self.redis.status

if __name__ == '__main__':
    s = SimhashIndexWithRedis(SimHashCache, SimhashInvertedIndex, SimhashRedis())
    import random
    import string
    for i in range(100):
        obj_id = 'test'+ str(i)
        salt = ''.join(random.sample(string.ascii_letters + string.digits, 60))
        value = 'weoigjnalksdmgl;kansd;kgnqw;smdfkasndg;olqwokmdfl,ndg;qw' + salt
        simhash = Simhash(value)
        test = s.add(obj_id, value)
        print(test)
        # s.delete('test3', simhash)
        print(s.bucket_size)
    a = s.redis.get('a903:2')
    print(a)