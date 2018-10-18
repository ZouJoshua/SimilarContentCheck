#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author  : Joshua
@Time    : 2018/10/16 16:18
@File    : extract_features_tfidf.py
@Desc    : 
"""

import nltk
import math
import string
import os

from nltk.corpus import stopwords
from collections import Counter
from nltk.stem.porter import*

# nltk.download('punkt')
# nltk.download('stopwords')


class PreProcessing(object):

    def __init__(self, text, stopwords):
        """Preprocessing of text
        Args:
            text: string of text
            stopwords: list of stopwords
        Returns：

        """
        self._text = text
        self.stopwords = stopwords

    def _get_tokens(self):
        """Preprocessing of word segmentation
        Remove the punctuation using the character deletion step of translate
        """
        # lower = text.lower().decode('utf-8') # python2
        tokens = list()
        lower = self._text.lower()
        sens = nltk.sent_tokenize(lower)
        remove_punctuation_map = dict((ord(char), None) for char in string.punctuation)
        for sen in sens:
            no_punctuation = sen.translate(remove_punctuation_map)
            tokens += nltk.word_tokenize(no_punctuation)
        return tokens

    def _stem_tokens(self, tokens, stemmer):
        """Preprocessing of stem extraction
        Args:
            tokens:
            stemmer:
        Returns:

        """
        stemmed = []
        for item in tokens:
            stemmed.append(stemmer.stem(item))
        return stemmed

    def count_term(self):
        """Calculate the word frequency after cleaning"""
        filtered = [w for w in self._get_tokens() if not w in self.stopwords]
        stemmer = PorterStemmer()
        stemmed = self._stem_tokens(filtered, stemmer)
        count = Counter(stemmed)
        return count


class CalculateTFIDF(object):

    def __init__(self, word, count, count_list=None):
        """Calculate word frequency
        Args:
            word:
            count:
            count_list
        Returns:
            TF-IDF of some keywords
        """
        self.word = word
        self.count = count

        if count_list:
            self.count_list = count_list
        else:
            self.count_list = count

    def _tokens_frequency(self):
        """Calculate tokens frequency"""
        return self.count[self.word] / sum(self.count.values())

    def _n_containing(self):
        return sum(1 for count in self.count_list if self.word in count)

    def _inverse_document_frequency(self):
        """Calculate the inverse document frequency"""
        return math.log(len(self.count_list)) / (1 + self._n_containing())

    def tfidf(self):
        """Calculate TF-IDF"""
        return self._tokens_frequency() * self._inverse_document_frequency()

def get_stopwords(stopword_file=None):
    """Get stopwords from nltk and stopword_file
    Args:
        stopword_file: path of stopword_file
    Returns:
        a set of stopwords
    """
    if stopword_file:
        try:
            my_stopword = open(stopword_file).read().split("\n")
        except Exception as e:
            my_stopword = ''
    else:
        my_stopword = ''
    return set(stopwords.words('english')).union(my_stopword)


def get_keywords_tfidf(text, stopword_file=None, corpus=None, topk=10):
    """Get tfidf of text's top k keywords
    Args:
        text: string of text
        stopword_file: path of stopword_file
        corpus: a list of
        topk: k words after sorting
    Returns:
        a dict (keyword, TFIDF) of text's topk keywords
    """
    if not stopword_file:
        stopword_file = os.path.join(os.path.abspath(__file__), 'stopwords_en.txt')
    stopwords = get_stopwords(stopword_file=stopword_file)
    count = PreProcessing(text, stopwords).count_term()
    if corpus:
        count_list = [count, PreProcessing(corpus, stopwords).count_term()]
    else:
        count_list = count
    scores = {word: CalculateTFIDF(word, count, count_list=count_list).tfidf() for word in count}
    sorted_words = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    # for word, score in sorted_words[:topk]:
    #     print("\tWord: {}, TF-IDF: {}".format(word, round(score, 5)))
    return dict((word, score) for word, score in sorted_words[:topk])

if __name__ == "__main__":
    text1 = "Natural language processing (NLP) is a field of computer science, artificial intelligence and computational linguistics concerned with the interactions between computers and human (natural) languages, and, in particular, concerned with programming computers to fruitfully process large natural language corpora. Challenges in natural language processing frequently involve natural language understanding, natural language generation (frequently from formal, machine-readable logical forms), connecting language and machine perception, managing human-computer dialog systems, or some combination thereof." \
            "The Georgetown experiment in 1954 involved fully automatic translation of more than sixty Russian sentences into English. The authors claimed that within three or five years, machine translation would be a solved problem.[2] However, real progress was much slower, and after the ALPAC report in 1966, which found that ten-year-long research had failed to fulfill the expectations, funding for machine translation was dramatically reduced. Little further research in machine translation was conducted until the late 1980s, when the first statistical machine translation systems were developed." \
            "During the 1970s, many programmers began to write conceptual ontologies, which structured real-world information into computer-understandable data. Examples are MARGIE (Schank, 1975), SAM (Cullingford, 1978), PAM (Wilensky, 1978), TaleSpin (Meehan, 1976), QUALM (Lehnert, 1977), Politics (Carbonell, 1979), and Plot Units (Lehnert 1981). During this time, many chatterbots were written including PARRY, Racter, and Jabberwacky。"
    import time
    stopword_file = os.path.join(os.path.abspath(__file__), 'stopwords_en.txt')
    s = time.clock()
    keywords = get_keywords_tfidf(text1, stopword_file)
    # keywords = get_keywords_tfidf(text1, stopword_file, corpus=text1)
    e = time.clock()
    print('抽取关键词耗时{}'.format(e - s))
    print(keywords)