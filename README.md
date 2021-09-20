# Tiny-Godis
## Introduction

The project is a learning project, the main framework and implementation principles are referenced https://github.com/HDT3213/godis. This is an awesome project to study redis and golaing. I suggested if you are interested in redis or want to improve golang code style, go git clone https://github.com/HDT3213/godis and study.

As I am busy with my studies and do not have the energy and time to complete all parts of redis/godis, the goals of this project are as follows.

Step1: Complete the tcp server and redis protocol parser, which can correctly parse redis information.

Step2: Complete the basic data structure required by redis, such as list, set, hash table, etc.

Step3: Complete redis command abstract excuse, which can register, find, and execute commands correctly.

Step4: Complete AOF and MULTI Commands Transaction for redis.

Step5: Complete pub/sub function and a simple redis client based pipeline.

## Quick Start

Linux:

```bash
./godis-linux
```

MacOS:

```
./godis-darwin
```

You could use telnet  to connect Tiny-Godis server, which listens on 0.0.0.0:6399 on default configuration.
