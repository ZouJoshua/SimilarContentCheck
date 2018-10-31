#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author  : Joshua
@Time    : 2018/9/28 19:27
@File    : test_simhash.py
@Desc    : simhash test
"""

from unittest import main, TestCase

from fingerprints_calculation.simhash import Simhash
from similarity_calculation.hamming_distance import HammingDistance
from fingerprints_storage.simhash_index_redis import SimhashIndexWithRedis
from sklearn.feature_extraction.text import TfidfVectorizer


class TestSimhash(TestCase):

    def test_int_value(self):
        self.assertEqual(Simhash(0).fingerprint, 0)
        self.assertEqual(Simhash(4390059585430954713).fingerprint, 4390059585430954713)
        self.assertEqual(Simhash(9223372036854775808).fingerprint, 9223372036854775808)

    def test_value(self):
        self.assertEqual(Simhash(['aaa', 'bbb']).fingerprint, 57087923692560392)

    def test_distance(self):
        sh = Simhash('How are you? I AM fine. Thanks. And you?')
        sh2 = Simhash('How old are you ? :-) i am fine. Thanks. And you?')
        self.assertTrue(sh.fingerprint(sh2) > 0)

        sh3 = Simhash(sh2)
        self.assertEqual(sh2.fingerprint(sh3), 0)

        self.assertNotEqual(Simhash('1').fingerprint(Simhash('2')), 0)

    def test_chinese(self):
        self.maxDiff = None

        sh1 = Simhash(u'你好　世界！　　呼噜。')
        sh2 = Simhash(u'你好，世界　呼噜')

        sh4 = Simhash(u'How are you? I Am fine. ablar ablar xyz blar blar blar blar blar blar blar Thanks.')
        sh5 = Simhash(u'How are you i am fine.ablar ablar xyz blar blar blar blar blar blar blar than')
        sh6 = Simhash(u'How are you i am fine.ablar ablar xyz blar blar blar blar blar blar blar thank')

        self.assertEqual(HammingDistance(sh1).distance(sh2), 0)

        self.assertTrue(HammingDistance(sh4).distance(sh6) < 3)
        self.assertTrue(HammingDistance(sh5).distance(sh6) < 3)

    def test_short(self):
        shs = [Simhash(s).fingerprint for s in ('aa', 'aaa', 'aaaa', 'aaaab', 'aaaaabb', 'aaaaabbb')]

        for i, sh1 in enumerate(shs):
            for j, sh2 in enumerate(shs):
                if i != j:
                    self.assertNotEqual(sh1, sh2)

    def test_sparse_features(self):
        data = [
            'How are you? I Am fine. blar blar blar blar blar Thanks.',
            'How are you i am fine. blar blar blar blar blar than',
            'This is simhash test.',
            'How are you i am fine. blar blar blar blar blar thank1'
        ]
        vec = TfidfVectorizer()
        D = vec.fit_transform(data)
        voc = dict((i, w) for w, i in vec.vocabulary_.items())

        # Verify that distance between data[0] and data[1] is < than
        # data[2] and data[3]
        shs = []
        for i in range(D.shape[0]):
            Di = D.getrow(i)
            # features as list of (token, weight) tuples)
            features = zip([voc[j] for j in Di.indices], Di.data)
            shs.append(Simhash(features))
        self.assertNotEqual(shs[0].distance(shs[1]), 0)
        self.assertNotEqual(shs[2].distance(shs[3]), 0)
        self.assertLess(shs[0].distance(shs[1]), shs[2].distance(shs[3]))

        # features as token -> weight dicts
        D0 = D.getrow(0)
        dict_features = dict(zip([voc[j] for j in D0.indices], D0.data))
        self.assertEqual(Simhash(dict_features).fingerprint, 17583409636488780916)

        # the sparse and non-sparse features should obviously yield
        # different results
        self.assertNotEqual(Simhash(dict_features).fingerprint,
                            Simhash(data[0]).fingerprint)


class TestSimhashIndex(TestCase):
    data = {
        1: 'How are you? I Am fine. blar blar blar blar blar Thanks.',
        2: 'How are you i am fine. blar blar blar blar blar than',
        3: 'This is simhash test.',
        4: 'How are you i am fine. blar blar blar blar blar thank1',
    }

    def setUp(self):
        objs = [(str(k), Simhash(v)) for k, v in self.data.items()]
        self.index = SimhashIndexWithRedis(objs, k=10)

    def test_get_near_dup(self):
        s1 = Simhash(u'How are you i am fine.ablar ablar xyz blar blar blar blar blar blar blar thank')
        dups = self.index.get_near_dups(s1)
        self.assertEqual(len(dups), 3)

        self.index.delete('1', Simhash(self.data[1]))
        dups = self.index.get_near_dups(s1)
        self.assertEqual(len(dups), 2)

        self.index.delete('1', Simhash(self.data[1]))
        dups = self.index.get_near_dups(s1)
        self.assertEqual(len(dups), 2)

        self.index.add('1', Simhash(self.data[1]))
        dups = self.index.get_near_dups(s1)
        self.assertEqual(len(dups), 3)

        self.index.add('1', Simhash(self.data[1]))
        dups = self.index.get_near_dups(s1)
        self.assertEqual(len(dups), 3)


if __name__ == '__main__':
    main()