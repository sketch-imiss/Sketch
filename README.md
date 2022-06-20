## A Python Library for Monitoring High-speed Network Traffic

![version](https://img.shields.io/badge/version-v1-green)
![python](https://img.shields.io/badge/python-3.9-blue)
![ray](https://img.shields.io/badge/ray-1.12.1-orange)

This repository contains our Python implementation of sketch methods for monitoring high-speed network traffic. 

Monitoring high-speed network traffic is a fundamental but important task in the fields of network security and management, which 
helps network administrators to better understand the runtime status by measuring a variety of metrics such as 
cardinality, frequency and persistency. Due to limited computational and memory resources, it becomes impractical to 
exactly compute metrics. Sketch is an effective and efficient data structure for network monitoring, which utilizes a 
family of hash functions to embed the raw dataset into a compact structure. Sketch keeps a trade-off among accuracy, 
computational costs, and memory usage. However, existing sketch methods still suffer from the problems of low accuracy, 
high time and memory complexities.

In this repository, we select three representative metrics, namely cardinality, frequency, and persistency, and develop
fast and accurate sketch structures for estimations.

### Cardinality Estimation
Given a sequence of user-item pairs, cardinality estimation aims to approximate the number of distinct items that each
user connects to. 

### Frequency Estimation

The frequency of each user is defined as its number of occurrences.

### Persistency Estimation

When dividing a long period into a set of timeslots without overlapping, the persistency of each user is defined as the 
number of timeslots that it occurs at least once.

### Sampling Strategy

This repository supports three sampling strategies according to different estimation tasks:
* Fixed-probability sampling
* Reservoir sampling
* Sample and hold

### Partitioning Strategy

This repository provides two partitioning strategies:
* Random partitioning
* Hashing partitioning

### Requirement
* Python >= 3.9
* Ray >= 1.12.1

### Citation

If you use our code, please cite:

#### Cardinality Estimation
```
Peng Jia, Pinghui Wang, Yuchao Zhang, Xiangliang Zhang, Jing Tao, Jianwei Ding, Xiaohong Guan, Don Towsley. Accurately Estimating User Cardinalities and Detecting Super Spreaders over Time. Transactions on Knowledge and Data Engineering (TKDE), 2020, 34 (1): 92-106.
```
```
Pinghui Wang, Peng Jia, Xiangliang Zhang, Jing Tao, Xiaohong Guan, Don Towsley. Utilizing Dynamic Properties of Sharing Bits and Registers to Estimate User Cardinalities over Time. International Conference on Data Engineering (ICDE), 2019: 1094-1105.
```

#### Frequency Estimation
```
Peng Jia, Pinghui Wang, Junzhou Zhao, Ye Yuan, Jing Tao, Xiaohong Guan. LogLog Filter: Filtering Cold Items within a Large Range over High Speed Data Streams. International Conference on Data Engineering (ICDE), 2021: 804-815.
```

#### Persistency Estimation
```
Pinghui Wang, Peng Jia, Jing Tao, Xiaohong Guan. Detecting a Variety of Long-Term Stealthy User Behaviors on High Speed Links. Transactions on Knowledge and Data Engineering (TKDE), 2018, 31 (10): 1912-1925.
```
```
Pinghui Wang, Peng Jia, Jing Tao, and Xiaohong Guan. Mining Long-Term Stealthy User Behaviors on High Speed Links. International Conference on Computer Communications (INFOCOM), 2018: 2051-2059.
```

### Contact

For more details, you can drop us an [email](pengjiapp@gmail.com).