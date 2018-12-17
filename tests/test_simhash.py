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

    @classmethod
    def setUpClass(cls):
        print(">>>>>>>>>>测试环境已准备好！")
        print(">>>>>>>>>>即将测试 Case ...")

    @classmethod
    def tearDownClass(cls):
        print(">>>>>>>>>>Case 用例已测试完成 ...")
        print(">>>>>>>>>>测试环境已清理完成！")

    def test_int_value(self):
        self.assertEqual(Simhash(0).fingerprint, 0)
        self.assertEqual(Simhash(4390059585430954713).fingerprint, 4390059585430954713)
        self.assertEqual(Simhash(9223372036854775808).fingerprint, 9223372036854775808)

    def test_value(self):
        self.assertEqual(Simhash(['aaa', 'bbb']).fingerprint, 57087923692560392)

    def test_distance(self):
        sh = Simhash('How are you? I AM fine. Thanks. And you?')
        sh2 = Simhash('How old are you ? :-) i am fine. Thanks. And you?')
        self.assertTrue(HammingDistance(sh).distance(sh2) > 0)

        sh3 = Simhash(sh2)
        self.assertEqual(HammingDistance(sh2).distance(sh3), 0)

        self.assertNotEqual(HammingDistance("1").distance(Simhash("2")), 0)

    def test_long_article(self):
        self.maxDiff = None

        sh1 = Simhash("\nBengaluru: Software major Adobe is the best technology company to work for in India, followed by chip maker NVIDIA and Microsoft, employment-related search engine giant Indeed said on Tuesday.\nThe Indian Space Research Organization (ISRO) was the highest-ranked Indian organisation at No. 10 — the only public sector firm to feature on the top-15 list, titled “Top Rated Workplaces: Best in Tech” on the basis of over 100 million ratings and reviews available on Indeed.\n“In addition to strategic workplace programmes, companies that have focused on people management and contributed to creating a conducive environment for their employees have been highly-rated by job seekers,” Indeed India Managing Director Sashi Kumar said in a statement.\nThe top 10 tech companies included like SAP, Akamai Technologies, VMware, Cisco, Intel and Citrix Systems Inc.\nApple was at 13th place while HP was 11th on the list. Some of the other Indian companies that feature in the top 15 include e-commerce company Myntra and Tata Consultancy Services (TCS).\n“Other than competitive remuneration, companies that work to make their employees feel like they work with and not for the company, create a culture of ownership and instil a sense of loyalty in their employees,” Kumar added.")
        sh2 = Simhash("\tSoftware major Adobe is the best technology company to work for in India, followed by chip maker NVIDIA and Microsoft, employment-related search engine giant Indeed said on Tuesday.\tThe Indian Space Research Organization (ISRO) was the highest-ranked Indian organisation at No. 10 -- the only public sector firm to feature on the top-15 list, titled \"Top Rated Workplaces: Best in Tech\" on the basis of over 100 million ratings and reviews available on Indeed.\t\"In addition to strategic workplace programmes, companies that have focused on people management and contributed to creating a conducive environment for their employees have been highly-rated by job seekers,\" Indeed India Managing Director Sashi Kumar said in a statement.\tThe top 10 tech companies included like SAP, Akamai Technologies, VMware, Cisco, Intel and Citrix Systems Inc.\tApple was at 13th place while HP was 11th on the list. Some of the other Indian companies that feature in the top 15 include e-commerce company Myntra and Tata Consultancy Services (TCS).\t\"Other than competitive remuneration, companies that work to make their employees feel like they work with and not for the company, create a culture of ownership and instil a sense of loyalty in their employees,\" Kumar added.\tRead more news:\tSaraswat Co-operative Bank, First to go Live on Whatsapp Channel from Co-operative Sector\tTrump commits to $750 bn defence budget\tead more news:\tSaraswat Co-operative Bank, First to go Live on Whatsapp Channel from Co-operative Sector\tTrump commits to $750 bn defence budget" )

        sh4 = Simhash('Actor Shreyas Talpade, who has been mired in a legal controversy owing to a publicity gimmick for his forthcoming film ‘Wah Taj’ is apologetic for hurting sentiments. “It is disturbing. But if somebody’s sentiment is hurt because of us, we have said it repeatedly that we are sorry. We didn’t do anything intentionally,” he said in an exclusive interview. As part of the film’s promotion, its makers distributed posters across Agra in Uttar Pradesh, stating the Taj Mahal will be shut for tourists from September 23, the release date of the film. A notice was sent to ‘Wah Taj’ maker Abhinav Verma, director Ajit Sinha and actors Pawan Sharma, Shreyas and Manjari for creating chaos in Agra and spoiling the image of the country. “We actually thought to do something out of the box while releasing the teaser poster. It was a promotional strategy. Because you’ve to do something new as a promotional plan, we thought to build up curiosity among the audience, which should relate to the film as well. But we didn’t want to make controversy,” he said. Shreyas feels he is fortunate enough to shoot around the beautiful monument. “I saw Taj Mahal for the first time when I came here to shoot. We started our shooting on the Yamuna banks. Watching the Taj, when the first ray of sun was falling on it and at the time of full moon, was quite an amazing experience. I think we were so fortunate to shoot around the symbol of love.” The ‘Iqbal’ actor, who will be seen in a farmer’s role in the new film, said playing a Maharashtrian is quite easy for him as it is his mother tongue. Unlike his ‘Welcome to Sajjanpur’ character Mahadev, the 40-year-old actor said Tukaram (Wah Taj) is quite aggressive. “Tukaram is little stronger than how Mahadev was. He has a firm conviction about his issues and he knows he will win. In spite of knowing that what will be the consequences he goes for it.” Directed by Ajit Sinha, ‘Wah Taj’ highlights the story of a farmer’s struggle against a corrupt political system. The film also features Manjari Phadnis in a lead role.')
        sh5 = Simhash("""MUMBAI: Actor Shreyas Talpade, who has been mired in a legal controversy owing to a publicity gimmick for his forthcoming film 'Wah Taj' is apologetic for hurting sentiments. Bollywood film actor Shreyas Talpade. (File Photo | AFP) "It is disturbing. But if somebody's sentiment is hurt because of us, we have said it repeatedly that we are sorry. We didn't do anything intentionally," he said in an exclusive interview. As part of the film's promotion, its makers distributed posters across Agra in Uttar Pradesh, stating the Taj Mahal will be shut for tourists from September 23, the release date of the film. A notice was sent to 'Wah Taj' maker Abhinav Verma, director Ajit Sinha and actors Pawan Sharma, Shreyas and Manjari for creating chaos in Agra and spoiling the image of the country. "We actually thought to do something out of the box while releasing the teaser poster. It was a promotional strategy. Because you've to do something new as a promotional plan, we thought to build up curiosity among the audience, which should relate to the film as well. But we didn't want to make controversy," he said. Shreyas feels he is fortunate enough to shoot around the beautiful monument. 'I saw Taj Mahal for the first time when I came here to shoot. We started our shooting on the Yamuna banks. Watching the Taj, when the first ray of sun was falling on it and at the time of full moon, was quite an amazing experience. I think we were so fortunate to shoot around the symbol of love.' The 'Iqbal' actor, who will be seen in a farmer's role in the new film, said playing a Maharashtrian is quite easy for him as it is his mother tongue. Unlike his 'Welcome to Sajjanpur' character Mahadev, the 40-year-old actor said Tukaram (Wah Taj) is quite aggressive. 'Tukaram is little stronger than how Mahadev was. He has a firm conviction about his issues and he knows he will win. In spite of knowing that what will be the consequences he goes for it.' Directed by Ajit Sinha, 'Wah Taj' highlights the story of a farmer's struggle against a corrupt political system. The film also features Manjari Phadnis in a lead role.""")
        sh6 = Simhash("""Rakul Preet is definitely in a bad shape as her last release 'Aiyaary' failed so bad at the BoxâOffice. In Telugu, Mahesh Babu's 'SPYder' totally spoiled her career and the movie was a huge flop. The actress still couldn't recover from that loss.Â It was touted that she will pair with Nandamuri Kalyan Ram for a Telugu project and she will team up with Siva Karthikeyan for a Space entertainer in Tamil. But now reports confirm that Rakul has just signed the Space entertainer flick in Tamil alone. Now, Rakul Preet has walked the ramp dressed in Saree at Teach for Change fundraiser event in Hyderabad. She was drop-dead gorgeous and was looking like a Million bucks in these photos. You can take a look at those photos below,""")

        print(HammingDistance(sh1).distance(sh2))
        print(HammingDistance(sh4).distance(sh6))
        print(HammingDistance(sh4).distance(sh5))

        self.assertTrue(HammingDistance(sh1).distance(sh2) < 4)
        self.assertTrue(HammingDistance(sh4).distance(sh5) < 4)
        self.assertTrue(HammingDistance(sh5).distance(sh6) < 4)

    def test_short_article(self):
        shs = [Simhash(s).fingerprint for s in ('aa', 'aaa', 'aaaa', 'aaaab', 'aaaaabb', 'aaaaabbb')]

        for i, sh1 in enumerate(shs):
            for j, sh2 in enumerate(shs):
                if i != j:
                    self.assertNotEqual(sh1, sh2)

        sh4 = Simhash('How are you? I Am fine. ablar ablar xyz blar blar blar blar blar blar blar Thanks.')
        sh5 = Simhash('How are you i am fine.ablar ablar xyz blar blar blar blar blar blar blar than')
        sh6 = Simhash('How are you i am fine.ablar ablar xyz blar blar blar blar blar blar blar thank')

        print(HammingDistance(sh4).distance(sh6))
        print(HammingDistance(sh4).distance(sh5))
        self.assertTrue(HammingDistance(sh4).distance(sh6) < 4)
        self.assertTrue(HammingDistance(sh5).distance(sh6) < 4)

    def test_sparse_features(self):
        data = [
            'How are you? I Am fine. blar blar blar blar blar Thanks.',
            'How are you i am fine. blar blar blar blar blar than',
            'This is simhash test.',
            'How are you i am fine. blar blar blar blar blar thank1'
        ]
        vec = TfidfVectorizer()
        D = vec.fit_transform(data)
        print(D)
        voc = dict((i, w) for w, i in vec.vocabulary_.items())
        print(voc)

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