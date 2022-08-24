# 字节青训营-大数据方向-基础班-【作业三】简易的分布式存储

## 项目介绍

**队名：能不能别挂提前批队  队号：3306**

本项目基于GO语言以及ProtoBuf&gRPC通信协议，利用git进行了项目管理，参考了GFS的框架完成了一个简易分布式存储系统，利用Raft实现了元数据服务的一致性共识模块，并利用LevelDB持久化存储的元数据内容。

## 项目架构

本团队所设计的分布式存储系统参考了传统设计，基础结构共分为三大块，分别是Clinent、namenode、和datanode。

- **Clinent**主要负责和用户直接进行交互，是用户无感知的使用项目的相关通用接口来获取，管理，和存储数据。
- **namenode**主要负责元数据的管理，是整个系统管理的核心，任何数据的读取存储都需要通过namenode来处理，为了降低整体项目的复杂度，整个项目仅有一个处于工作状态的namenode，详细的流程设计将会在开发文档中阐述。
- **datanode**主要负责数据的存储，本项目的datanode设计为类似于GFS的chunk设计，在代码中体现为block。每一个datanode拥有若干个block，而每个bolck将存储文件的部分内容，实现多副本存储以及，将文件分布式存储的效果。

**（详见目录下的答辩文档及其中的开发手册）**

## 项目配置

> 仅需安装1.18+golang环境即可，表格中的资源包均由go mod管理。

|  开发环境  |                       版本                       |
| :--------: | :----------------------------------------------: |
|   Golang   |                      1.18+                       |
| **资源包** |                     **版本**                     |
|  protubuf  |                     v1.28.0                      |
|    grpc    |                     v1.48.0                      |
|  go.uuid   |                      v1.2.0                      |
|  levelDB   | **（这里没有mod管理版本未知,以及raft是否添加）** |

## 运行步骤

- 代码获取

```c++
git clone https://github.com/Kirov7/FayDFS.git
```

- 使用步骤

1. 配置资源包

在FayDFS路径下执行：

```go
go mod tidy
```

2. 启动namenode以及datanode服务

```go
cd ./FayDFS/namenode
go run main.go
cd ./FayDFS/datanode
go run main.go
```

3. 启动或修改test.go以进行接口调用或测试

```go
cd ./FayDFS
go run test.go
```