#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author  : Joshua
@Time    : 2018/9/28 19:20
@File    : simhash.py
@Desc    : 
"""

from __future__ import division, unicode_literals

import re
import sys
import hashlib
import logging
import numbers
import collections
from itertools import groupby

if sys.version_info[0] >= 3:
    basestring = str
    unicode = str
    long = int
else:
    range = xrange


class Simhash(object):

    def __init__(
            self, value, hashbits=64, reg=r'[\w]+', hashfunc=None, log=None):
        """Generate fingerprints of the content
        Args:
            value: content
            hashbits: the dimensions of fingerprints
            reg: is meaningful only when `value` is basestring and describes
                what is considered to be a letter inside parsed string. Regexp
                object can also be specified (some attempt to handle any letters
                is to specify reg=re.compile(r'\w', re.UNICODE))
            hashfunc: accepts a utf-8 encoded string and returns a unsigned
                integer in at least `hashbits` bits.
        Returns:
            the fingerprints of value
        """

        self.hashbits = hashbits
        self.reg = reg
        self.fingerprints = None

        if hashfunc is None:
            self.hashfunc = self._hashfunc
        else:
            self.hashfunc = hashfunc

        if log is None:
            self.log = logging.getLogger("simhash")
        else:
            self.log = log

        if isinstance(value, Simhash):
            self.fingerprints = value.fingerprints
        elif isinstance(value, basestring):
            self.build_by_text(unicode(value))
        elif isinstance(value, collections.Iterable):
            self.build_by_features(value)
        elif isinstance(value, numbers.Integral):
            self.fingerprints = value
        else:
            raise Exception('Bad parameter with type {}'.format(type(value)))

    def _hashfunc(self, x):
        # Generate hash value with hashlib.md5
        return int(hashlib.md5(x).hexdigest(), 16)

    def _hashfunc_builtin(self, x):
        # Generate hash value with hash
        hashcode = str(bin(hash(x)).replace('0b', '').replace('-', '').zfill(self.hashbits)[-self.hashbits:])
        return hashcode

    def _slide(self, content, width=4):
        return [content[i:i + width] for i in range(max(len(content) - width + 1, 1))]

    def _tokenize(self, content):
        content = content.lower()
        content = ''.join(re.findall(self.reg, content))
        ans = self._slide(content)
        return ans

    def build_by_text(self, content):
        features = self._tokenize(content)
        features = {k: sum(1 for _ in g) for k, g in groupby(sorted(features))}
        return self.build_by_features(features)

    def build_by_features(self, features):
        """
        Args:
            features: might be a list of unweighted tokens (a weight of 1
                       will be assumed), a list of (token, weight) tuples or
                       a token -> weight dict.
        """
        v = [0] * self.hashbits
        masks = [1 << i for i in range(self.hashbits)]
        if isinstance(features, dict):
            features = features.items()
        for f in features:
            if isinstance(f, basestring):
                h = self.hashfunc(f.encode('utf-8'))
                w = 1
            else:
                assert isinstance(f, collections.Iterable)
                h = self.hashfunc(f[0].encode('utf-8'))
                w = f[1]
            for i in range(self.hashbits):
                v[i] += w if h & masks[i] else -w
        _fingerprints = 0
        for i in range(self.hashbits):
            if v[i] > 0:
                _fingerprints |= masks[i]
        self.fingerprints = _fingerprints

if __name__ == '__main__':
    str = {'hello': 3, 'world': 4, 'fine': 5, 'new': 2, 'text': 3}
    print bin(Simhash(str).fingerprints).replace('0b', '').zfill(64)[-64:]
    str1 = {'hello': 5, 'world': 4, 'fine': 5, 'new': 2, 'text': 3}
    print bin(Simhash(str1).fingerprints).replace('0b', '').zfill(64)[-64:]
    str2 = {'hello': 3, 'world': 4, 'fine': 5, 'news': 2, 'text': 2, 'test': 1}
    print bin(Simhash(str2).fingerprints).replace('0b', '').zfill(64)[-64:]
    str3 = 'hello,world! i\'m  working in China!'
    print bin(Simhash(str3).fingerprints).replace('0b','').zfill(64)[-64:]