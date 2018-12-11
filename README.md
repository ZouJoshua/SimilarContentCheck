# 文档说明

## 项目描述

完成线上实时抓取的文章的去重工作，实现主要以SimHash算法为去重算法，并提供SimHash指纹的倒排索引的存储，更新及线上项目恢复的方案。

## 代码逻辑

![代码逻辑图](https://raw.githubusercontent.com/ZouJoshua/image/png/20181105154132.png)

## 各模块说明
入口：\manager\similarity_check.py

分词：\extract_features\extract_features_participle.py

计算 SimHash：\fingerprints_calculation\simhash.py

计算海明距离：\similarity_calculation\hamming_distance.py

倒排索引计算及存储：\fingerprints_storage\simhash_index_redis.py

MongoDB 数据库：\db\simhash_mongo.py

Redis 数据库：\db\simhash_redis.py

配置：\setting\__init__.py

日志：\utils\logger.py

测试：\tests

计时器：\ utils\timer.py
