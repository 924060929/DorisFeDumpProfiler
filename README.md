# doris fe的读写表锁分析工具

## 支持功能
1. 分析当前被用到的表的表锁排队情况
2. 分析线程与线程间由于表锁互相阻塞的链路
3. 打印出线程相关的sql、queryId和loadId



## 编译
```shell
mvn clean package
```

## 使用方式
1. 先在卡住的fe上执行dump命令
```shell
jmap -dump:live,format=b,file=fe.hprof <fe_pid>
```

2. 分析
```shell
java -jar DorisFeDumpProfiler-*.jar fe.hprof
```