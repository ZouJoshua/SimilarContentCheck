#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author  : Joshua
@Time    : 2018/10/23 20:17
@File    : test.py
@Desc    : 
"""

import threading
import time

def test():
    s = time.time()
    while True:
        if time.time() - s > 3:
            s = time.time()
            print(s)

def test1(x,y):
    print('{}**{}'.format(x,y))

def main():
    t = threading.Thread(target=test)
    t.setDaemon(True)
    t.start()
    x = 'a'
    y = 'b'
    m = 0
    while True:
        m +=1
        y += str(m)
        t2 = threading.Thread(target=test1, args=(x, y))
        t2.setDaemon(True)
        t2.start()
        print(t2)
        if m > 500:
            break
    t2.join()
    t.join()

if __name__ == '__main__':
    main()
