## 文档说明

### 项目描述（相似文章去重）

完成线上实时抓取的英文相似文章的去重工作，实现主要以 SimHash 算法为去重算法，并提供 SimHash 指纹的倒排索引的存储，更新及线上项目恢复的方案。


### 实现功能
1. 可实时计算文章的去重工作
2. MongoDB 指纹存储
3. 服务重启时，自动加载已计算的指纹库到 Redis
4. 定期更新 Redis 内存（新闻推荐业务需要保持一定的时效性，并且内存在不断扩大）

### 代码逻辑

<div align=center>

![代码逻辑图](https://raw.githubusercontent.com/ZouJoshua/image/master/png/20181105154132.png)

</div>

线上实时抓取文章，计算 SimHash 指纹值，根据存在于 Redis 数据库的已爬取文章所计算的 SimHash 指纹值建立的倒排索引，进行 SimHash 指纹的比对，
如果判断两个指纹值的距离小于一定阈值，返回该文章的重复文章列表，并将刚抓取的文章计算的指纹值建立的倒排索引存入 MongoDB 数据库，同时
加入 Redis 数据库。

### 实际测试

在笔记本本地拿 158W+ 线上真实文章数据进行实测，去重耗时 _19小时20分钟_ 效果如下

> 电脑配置情况

![工作电脑配置](https://raw.githubusercontent.com/ZouJoshua/image/master/png/20181221180315.png)

> 数据处理到140W时，电脑内存占用情况

![电脑内存占用情况1](https://raw.githubusercontent.com/ZouJoshua/image/master/png/20181221145649.png)

> 数据处理完，电脑内存占用情况

![电脑内存占用情况2](https://raw.githubusercontent.com/ZouJoshua/image/master/png/20181221175626.png)

> 158W 文章的 SimHash 指纹的倒排索引占 Redis 内存情况

![Redis占用情况](https://raw.githubusercontent.com/ZouJoshua/image/master/png/20181221164551.png)


### 各模块说明

#### 入口
- \manager

入口模块主要为：

```similarity_check.py```

> 该模块为程序的入口，主要实现了三个类，初始化数据库类 _InitDB_，检查重复类 _Check_，
> 更新数据库的类 _UpdateDB_（该类待验证），线上主要以初始化数据库类和检查重复类为主。


#### 分词模块
- \extract_features

计算分词模块主要为：

```extract_features_participle.py```

> 该模块为分词模块，是将文章切分为多个特征，并计算特征的权重，用于计算 SimHash， 主要实现类为
> _Participle_

其次，实现以 tfidf 为分词方法，也可用于相似文章的去重。

```extract_features_tfidf.py```

主要实现了预处理类 _PreProcessing_，计算 tfidf 的类 _CalculateTFIDF_

#### SimHash 计算模块
- \fingerprints_calculation

```simhash.py```

#### 海明距离计算模块
- \similarity_calculation

```hamming_distance.py```

#### 倒排索引计算及存储模块
- \fingerprints_storage

```simhash_index_redis.py```

#### 数据库模块
- \db

```simhash_mongo.py```

```simhash_redis.py```

#### 配置模块
- \setting

```__init__.py```

#### 工具模块
- \utils

```logger.py```

```timer.py```

#### 单元测试模块
- \tests