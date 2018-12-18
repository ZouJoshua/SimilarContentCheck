# 文档说明

## 项目描述

完成线上实时抓取的英文相似文章的去重工作，实现主要以SimHash算法为去重算法，并提供SimHash指纹的倒排索引的存储，更新及线上项目恢复的方案。


## 代码逻辑

![代码逻辑图](https://raw.githubusercontent.com/ZouJoshua/image/png/20181105154132.png)

线上实时抓取文章，计算simhash指纹值，根据存在于redis数据库的已爬取文章所计算的simhash指纹值建立的倒排索引，进行simhash指纹的比对，
如果判断两个指纹值的距离小于一定阈值，返回该文章的重复文章列表，并将刚抓取的文章计算的指纹值建立的倒排索引存入mongodb数据库，同时
加入redis数据库。


## 各模块说明

### 入口
- \manager

入口模块主要为：

```similarity_check.py```

该模块为程序的入口，主要实现了三个类，初始化数据库类 **InitDB**，检查重复类 **Check**，
更新数据库的类 **UpdateDB**（该类待验证），线上主要以初始化数据库类和检查重复类为主。

### 分词模块
- \extract_features

计算分词模块主要为：

```extract_features_participle.py```

该模块为分词模块，是将文章切分为多个特征，并计算特征的权重，用于计算simhash。主要实现类为
**Participle**

其次，实现以tfidf为分词方法，也可用于相似文章的去重。

```extract_features_tfidf.py```

主要实现了预处理类 **PreProcessing**，计算tfidf的类 **CalculateTFIDF**

### SimHash 计算模块
- \fingerprints_calculation

```simhash.py```

### 海明距离计算模块
- \similarity_calculation

```hamming_distance.py```

### 倒排索引计算及存储模块
- \fingerprints_storage

```simhash_index_redis.py```

### 数据库模块
- \db

```simhash_mongo.py```

```simhash_redis.py```

### 配置模块
- \setting

```__init__.py```

### 工具模块
- \utils

```logger.py```

```timer.py```

### 单元测试模块
- \tests