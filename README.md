# 字节跳动青训营-大数据方向【作业三】简易的分布式存储

## 项目介绍

**队名：提前批能不能别挂简历了队  队号：3306**

本项目基于Go语言以及ProtoBuf&gRPC通信协议，利用git进行了项目管理，参考了GFS的框架完成了一个简易分布式存储系统，利用Raft实现了元数据服务的一致性共识模块，并利用LevelDB持久化存储的元数据内容。

## 项目架构

本团队所设计的分布式存储系统参考了传统GFS设计，基础结构共分为三大块，分别是clinent、namenode、和datanode。

- **clinent**主要负责和用户直接进行交互，是用户无感知的使用项目的相关通用接口来获取，管理，和存储数据。
- **namenode**主要负责元数据的管理，是整个系统管理的核心，任何数据的读取存储都需要通过namenode来处理，为了降低整体项目的复杂度，整个项目仅有一个处于工作状态的namenode，详细的流程设计将会在开发文档中阐述。
- **datanode**主要负责数据的存储，本项目的datanode设计为类似于GFS的chunk设计，在代码中体现为block。每一个datanode拥有若干个block，而每个bolck将存储文件的部分内容，实现多副本存储以及，将文件分布式存储的效果。

**（详见的运行流程和设计请参考下方链接的答辩文档及其中的开发手册）**

[答辩文档 - 飞书文档 (feishu.cn)](https://bqn8fhqijw.feishu.cn/docx/doxcnAajUj9AdPhzgYUsRQd1Wnd)
> 注: 项目架构与具体细节以主分支最新源代码为准
---
该项目最终获得了日月同辉奖
![QQ截图20220906120452.png](http://tva1.sinaimg.cn/large/005Uj3w8ly1h5wr9wu2hyj30v7011q3n.jpg)

## TODO
1. 基于raft搭建nn集群 (自搭raft框架)
2. cmd目录设置,方法入口优化
3. 编写Makefile文件
4. ~~文件目录树数采用B-树优化(经调研后发现没有此项优化的必要)~~
5. ~~dn接受后的数据转发优化 (目前仍是client多次连接发送数据)~~
6. ~~新增节点直接追加，缺少判断 (可能出现节点重复添加的问题)~~
7. ~~namenode结构体优化,leveldb三库合并~~
8. ~~datanodeList改用集合的数据结构,快速定位避免重复~~
9. ~~json配置文件优化 (采用.yaml或者.toml)~~
10. ~~副本转移后元数据即时更新~~
11. 垃圾回收机制
12. namenode部分方法两个阶段propose合并,增加简单的undo回滚机制
13. client的put操作优化长连接
14. client缓存namenode的leader地址信息
15. datanode数据的校验和
16. datanode采用EC纠删码节省冗余空间
17. datanode节点挂掉时,在副本转移之后在datanodeList中删除 (必要性有待讨论)
