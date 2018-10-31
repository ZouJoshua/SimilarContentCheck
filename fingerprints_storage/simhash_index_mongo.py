#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author  : Joshua
@Time    : 2018/10/12 15:38
@File    : simhash_index_mongo.py
@Desc    : 
"""

import logging
import collections
import datetime
from utils.timer import Timer

from fingerprints_calculation.simhash import Simhash
from similarity_calculation.hamming_distance import HammingDistance
from db.simhash_mongo import SimhashInvertedIndex

class SimhashIndexWithMongo(object):

    def __init__(self, objs=(), hashbits=64, k=3, hash_type='news'):
        """
        Args:
            objs: a list of (obj_id, origin_text)
                obj_id is a string, simhash is an instance of Simhash
            hashbits: the same with the one for Simhash
            k: the tolerance
            hash_type: the hash type  of the text
        """
        self.k = k
        self.hashbits = hashbits
        self.hash_type = hash_type
        count = len(objs)
        logging.info('Initializing %s data.', count)

        for i, q in enumerate(objs):
            if i % 10000 == 0 or i == count - 1:
                logging.info('%s/%s', i + 1, count)
            self.add(*q)

    def _insert(self, obj_id=None, value=None):
        """Insert hash value
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
            with Timer(msg='add_simhash_cache'):
                # Store or update the cache
                simhashcaches = SimHashCache.objects.filter(obj_id=obj_id,
                         hash_type=self.hash_type).exclude('text').order_by('-update_time')
                if simhashcaches:
                    simhashcache = simhashcaches[0]
                else:
                    simhashcache = SimHashCache(obj_id=obj_id,
                                hash_type=self.hash_type)
                if isinstance(value, str):
                    simhashcache.text = value
                simhashcache.update_time = datetime.datetime.now()
                simhashcache.hash_value = "%x" % simhash.fingerprint
                simhashcache.save()
            with Timer(msg='add_invert_index'):
                # cache invert index
                v = '%x,%s' % (simhash.fingerprint, obj_id)  # Convert to hexadecimal for compressed storage, which saves space and converts back when querying
                for key in self.get_keys(simhash):
                    with Timer(msg='add_invert_index-update_index-insert'):
                        try:
                            invert_index = SimhashInvertedIndex(key=key, hash_type=self.hash_type,
                                                            simhash_value_obj_id=v
                                                                )
                            invert_index.save()
                        except Exception as e:
                            print('%s,%s,%s' % (e, key, v))
                            pass

            return simhashcache

    def _find(self, value, k=3, exclude_obj_ids=set(), exclude_obj_id_contain=None):
        assert value != None

        if isinstance(value, str):
            simhash = Simhash(value=value, hashbits=self.hashbits)
        elif isinstance(value, Simhash):
            simhash = value
        else:
            raise Exception('value not text or simhash')
        assert simhash.hashbits == self.hashbits
        sim_hash_dict = collections.defaultdict(list)
        ans = set()
        for key in self.get_keys(simhash):
            with Timer(msg='==query: {}'.format(key)):
                simhash_invertindex = SimhashInvertedIndex.objects.filter(key=key)
                if simhash_invertindex:
                    simhash_caches_index = [sim_index.simhash_value_obj_id
                                        for sim_index in simhash_invertindex]
                else:
                    # logging.warning('SimhashInvertedIndex not exists key %s: %s' % (key, e))
                    continue
            with Timer(msg='find d < k {:d}'.format(k)):
                if len(simhash_caches_index) > 200:
                    logging.warning('Big bucket found. key:%s, len:%s', key, len(simhash_caches_index))
                for simhash_cache in simhash_caches_index:
                    try:
                        sim2, obj_id = simhash_cache.split(',', 1)
                        if obj_id in exclude_obj_ids or \
                        (exclude_obj_id_contain and exclude_obj_id_contain in simhash_cache):
                            continue

                        sim2 = Simhash(int(sim2, 16), self.hashbits)
                        _sim1 = HammingDistance(simhash)
                        d = _sim1.distance(sim2)
                        # print('**' * 50)
                        # print("d:%d obj_id:%s key:%s " % (d, obj_id, key))
                        sim_hash_dict[obj_id].append(d)
                        if d < k:
                            ans.add(obj_id)
                    except Exception as e:
                        logging.warning('not exists {}'.format(e))
        return list(ans)

    @staticmethod
    def query_simhash_cache(obj_id):
        """Find similar objects by obj_id"""
        simhash_caches = SimHashCache.objects.filter(obj_id__contains=obj_id)
        return simhash_caches

    @staticmethod
    def find_similiar(obj_id):
        simhash_caches = SimHashCache.objects.filter(obj_id__contains=obj_id)
        return simhash_caches

    def delete(self, obj_id, simhash):
        """
        Args:
            obj_id: a string
            simhash: an instance of Simhash
        """
        assert simhash.hashbits == self.hashbits
        try:
            simhashcache = SimHashCache.objects.get(obj_id=obj_id, hash_type=self.hash_type)
        except Exception as e:
            logging.warning('Not exists {}'.format(e))
            return

        for key in self.get_keys(simhash):
            try:
                simhash_invertindex = SimhashInvertedIndex.objects.get(key=key)
                if simhashcache in simhash_invertindex.simhash_caches_index:
                    simhash_invertindex.simhash_caches_index.remove(simhashcache)
                    simhash_invertindex.save()
            except Exception as e:
                logging.warning('Not exists {}'.format(e))

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

    @property
    def offsets(self):
        return [self.hashbits // (self.k + 1) * i for i in range(self.k + 1)]

    def get_keys(self, simhash):
        for i, offset in enumerate(self.offsets):
            # m = (i == len(self.offsets) - 1 and 2 ** (self.hashbits - offset) - 1 or 2 ** (self.offsets[i + 1] - offset) - 1)
            if i == len(self.offsets) - 1:
                m = 2 ** (self.hashbits - offset) - 1
            else:
                m = 2 ** (self.offsets[i + 1] - offset) - 1
            c = simhash.fingerprint >> offset & m
            yield '{:x}:{:x}'.format(c, i)

    def bucket_size(self):
        return SimhashInvertedIndex.objects.count()