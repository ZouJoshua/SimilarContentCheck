#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author  : Joshua
@Time    : 2018/10/16 16:18
@File    : ef_tfidf.py
@Desc    : 
"""

import nltk
import math
import string

from nltk.corpus import stopwords
from collections import Counter
from nltk.stem.porter import*

# nltk.download('punkt')
# nltk.download('stopwords')


def get_stopwords(stopword_file):
    if stopword_file:
        try:
            my_stopword = open(stopword_file).read().split("\n")
        except Exception as e:
            my_stopword = ''
    else:
        my_stopword = ''
    return set(stopwords.words('english')).union(my_stopword)

class PreProcessing(object):

    def __init__(self, text, stopwords):
        """Preprocessing of text
        """
        self.tokens = list()
        self._text = text
        self.stopwords = stopwords

    def _get_tokens(self):
        """Preprocessing of word segmentation
        Remove the punctuation using the character deletion step of translate
        """
        # lower = text.lower().decode('utf-8') # python2
        lower = self._text.lower()
        sens = nltk.sent_tokenize(lower)
        remove_punctuation_map = dict((ord(char), None) for char in string.punctuation)
        for sen in sens:
            no_punctuation = sen.translate(remove_punctuation_map)
            self.tokens += nltk.word_tokenize(no_punctuation)

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
        filtered = [w for w in self.tokens if not w in self.stopwords]
        stemmer = PorterStemmer()
        stemmed = self._stem_tokens(filtered, stemmer)
        count = Counter(stemmed)
        return count


class CalculateTFIDF(object):

    def __init__(self, word, count, count_list):
        """Calculate word frequency
        Args:
            word:
            count:
            count_list
        Returns:

        """
        self.word = word
        self.count = count
        self.count_list = count_list

    def tokens_frequency(self):
        """Calculate tokens frequency"""
        return self.count[self.word] / sum(self.count.values())

    def n_containing(self):
        return sum(1 for count in self.count_list if self.word in count)

    def inverse_document_frequency(self, word, count_list):
        """Calculate the inverse document frequency
        """
        return math.log(len(count_list)) / (1 + self.n_containing())

    def tfidf(self, word, count, count_list):
        """Calculate TF-IDF
       """
        return self.tokens_frequency(word, count) * self.inverse_document_frequency(word, count_list)

def main():
    texts = [text1, text2, text3]
    countlist = []
    for text in texts:
        countlist.append(count_term(text))
    for i, count in enumerate(countlist):
        print("Top words in document {}".format(i + 1))
        scores = {word: tfidf(word, count, countlist) for word in count}
        sorted_words = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        for word, score in sorted_words[:10]:
            print("\tWord: {}, TF-IDF: {}".format(word, round(score, 5)))

if __name__ == "__main__":
    text1 = "Natural language processing (NLP) is a field of computer science, artificial intelligence and computational linguistics concerned with the interactions between computers and human (natural) languages, and, in particular, concerned with programming computers to fruitfully process large natural language corpora. Challenges in natural language processing frequently involve natural language understanding, natural language generation (frequently from formal, machine-readable logical forms), connecting language and machine perception, managing human-computer dialog systems, or some combination thereof."
    text2 = "The Georgetown experiment in 1954 involved fully automatic translation of more than sixty Russian sentences into English. The authors claimed that within three or five years, machine translation would be a solved problem.[2] However, real progress was much slower, and after the ALPAC report in 1966, which found that ten-year-long research had failed to fulfill the expectations, funding for machine translation was dramatically reduced. Little further research in machine translation was conducted until the late 1980s, when the first statistical machine translation systems were developed."
    text3 = "During the 1970s, many programmers began to write conceptual ontologies, which structured real-world information into computer-understandable data. Examples are MARGIE (Schank, 1975), SAM (Cullingford, 1978), PAM (Wilensky, 1978), TaleSpin (Meehan, 1976), QUALM (Lehnert, 1977), Politics (Carbonell, 1979), and Plot Units (Lehnert 1981). During this time, many chatterbots were written including PARRY, Racter, and Jabberwacky。"
    main()