# doris fe的读写表锁分析工具

## 支持功能
1. 分析当前被用到的表的表锁排队情况
2. 分析线程与线程间由于表锁互相阻塞的链路
3. 打印出线程相关的sql、queryId和loadId



## 编译
```shell
mvn clean package
```

## 直接下载预编译包
[release](https://github.com/924060929/DorisFeDumpProfiler/releases)

## 使用方式
### 1. 先在卡住的fe上执行dump命令
```shell
jmap -dump:live,format=b,file=fe.hprof <fe_pid>
```

### 2. 分析
```shell
java -jar DorisFeDumpProfiler-*.jar fe.hprof
```

### 3. 分析输出结果

分析结果主要打印出
- Locks: 每个表锁的持有、排队线程
- Threads: 每个线程持有的锁、阻塞的锁、锁依赖链路、是否产生死锁、栈帧、queryId、loadId、事件拓扑顺序
- Queries: 每个查询线程对应的SQL

```angular2html
===== Locks =====
trade.klines_1m lock:
    hold read lock:  "mtmv-task-execute-1-thread-2"
    waiting queue:
       write: "thrift-server-pool-4"
       read:  "mysql-nio-pool-2"
       read:  "report-thread"
       read:  "tablet checker"
       read:  "PartitionInfoCollector"
       read:  "tablet stat mgr"
       read:  "Analysis Job Executor-0"

trade.klines_mv_1d lock:
    hold read lock:  "mysql-nio-pool-2"
    waiting queue:
       write: "mtmv-task-execute-1-thread-9"
       read:  "mtmv-task-execute-1-thread-2"

trade.klines_mv_1h lock:
    hold read lock:  "mtmv-task-execute-1-thread-2"


===== Threads =====
"tablet checker" daemon prio=5 tid=43 WAITING
    blocked read lock: "trade.klines_1m"
    block depends tree:
       "tablet checker" blocked at read "trade.klines_1m"
          "thrift-server-pool-4" blocked at write "trade.klines_1m"
             "mtmv-task-execute-1-thread-2" hold read "trade.klines_1m", blocked at read "trade.klines_mv_1d"
                "mtmv-task-execute-1-thread-9" blocked at write "trade.klines_mv_1d"
                   "mysql-nio-pool-2" hold read "trade.klines_mv_1d", blocked at read "trade.klines_1m"
                      "thrift-server-pool-4" blocked at write "trade.klines_1m" (Dead lock)
    events topology order:
       "mysql-nio-pool-2" hold read "trade.klines_mv_1d"
       "mtmv-task-execute-1-thread-2" hold read "trade.klines_1m"
       "mtmv-task-execute-1-thread-9" blocked at write "trade.klines_mv_1d"
       "thrift-server-pool-4" blocked at write "trade.klines_1m"
       "mtmv-task-execute-1-thread-2" blocked at read "trade.klines_mv_1d"
       "tablet checker" blocked at read "trade.klines_1m"
       "mysql-nio-pool-2" blocked at read "trade.klines_1m"


    jdk.internal.misc.Unsafe.park(Native Method)
    java.util.concurrent.locks.LockSupport.park(LockSupport.java:211)
    java.util.concurrent.locks.AbstractQueuedSynchronizer.acquire(AbstractQueuedSynchronizer.java:715)
    java.util.concurrent.locks.AbstractQueuedSynchronizer.acquireShared(AbstractQueuedSynchronizer.java:1027)
    java.util.concurrent.locks.ReentrantReadWriteLock$ReadLock.lock(ReentrantReadWriteLock.java:738)
    org.apache.doris.catalog.Table.readLock(Table.java:177)
    org.apache.doris.clone.TabletChecker.checkTablets(TabletChecker.java:306)
    org.apache.doris.clone.TabletChecker.runAfterCatalogReady(TabletChecker.java:211)
    org.apache.doris.common.util.MasterDaemon.runOneCycle(MasterDaemon.java:58)
    org.apache.doris.common.util.Daemon.run(Daemon.java:116)

"PartitionInfoCollector" daemon prio=5 tid=35 WAITING
    blocked read lock: "trade.klines_1m"
    block depends tree:
       "PartitionInfoCollector" blocked at read "trade.klines_1m"
          "thrift-server-pool-4" blocked at write "trade.klines_1m"
             "mtmv-task-execute-1-thread-2" hold read "trade.klines_1m", blocked at read "trade.klines_mv_1d"
                "mtmv-task-execute-1-thread-9" blocked at write "trade.klines_mv_1d"
                   "mysql-nio-pool-2" hold read "trade.klines_mv_1d", blocked at read "trade.klines_1m"
                      "thrift-server-pool-4" blocked at write "trade.klines_1m" (Dead lock)
    events topology order:
       "mysql-nio-pool-2" hold read "trade.klines_mv_1d"
       "mtmv-task-execute-1-thread-2" hold read "trade.klines_1m"
       "mtmv-task-execute-1-thread-9" blocked at write "trade.klines_mv_1d"
       "thrift-server-pool-4" blocked at write "trade.klines_1m"
       "mtmv-task-execute-1-thread-2" blocked at read "trade.klines_mv_1d"
       "PartitionInfoCollector" blocked at read "trade.klines_1m"
       "mysql-nio-pool-2" blocked at read "trade.klines_1m"


    jdk.internal.misc.Unsafe.park(Native Method)
    java.util.concurrent.locks.LockSupport.park(LockSupport.java:211)
    java.util.concurrent.locks.AbstractQueuedSynchronizer.acquire(AbstractQueuedSynchronizer.java:715)
    java.util.concurrent.locks.AbstractQueuedSynchronizer.acquireShared(AbstractQueuedSynchronizer.java:1027)
    java.util.concurrent.locks.ReentrantReadWriteLock$ReadLock.lock(ReentrantReadWriteLock.java:738)
    org.apache.doris.catalog.Table.readLock(Table.java:177)
    org.apache.doris.master.PartitionInfoCollector.updatePartitionCollectInfo(PartitionInfoCollector.java:89)
    org.apache.doris.master.PartitionInfoCollector.runAfterCatalogReady(PartitionInfoCollector.java:66)
    org.apache.doris.common.util.MasterDaemon.runOneCycle(MasterDaemon.java:58)
    org.apache.doris.common.util.Daemon.run(Daemon.java:116)

"mtmv-task-execute-1-thread-2" prio=5 tid=124 WAITING queryId=f56d9c94376441d9-b27c0dbdddef87a8
    blocked read lock: "trade.klines_mv_1d"
    hold read lock:  "trade.klines_1m"
    hold read lock:  "trade.klines_mv_1h"
    block depends tree:
       "mtmv-task-execute-1-thread-2" blocked at read "trade.klines_mv_1d"
          "mtmv-task-execute-1-thread-9" blocked at write "trade.klines_mv_1d"
             "mysql-nio-pool-2" hold read "trade.klines_mv_1d", blocked at read "trade.klines_1m"
                "thrift-server-pool-4" blocked at write "trade.klines_1m"
                   "mtmv-task-execute-1-thread-2" hold read "trade.klines_1m", blocked at read "trade.klines_mv_1d" (Dead lock)
    events topology order:
       "mysql-nio-pool-2" hold read "trade.klines_mv_1d"
       "mtmv-task-execute-1-thread-2" hold read "trade.klines_1m"
       "mtmv-task-execute-1-thread-9" blocked at write "trade.klines_mv_1d"
       "thrift-server-pool-4" blocked at write "trade.klines_1m"
       "mtmv-task-execute-1-thread-2" blocked at read "trade.klines_mv_1d"
       "mysql-nio-pool-2" blocked at read "trade.klines_1m"


    jdk.internal.misc.Unsafe.park(Native Method)
    java.util.concurrent.locks.LockSupport.park(LockSupport.java:211)
    java.util.concurrent.locks.AbstractQueuedSynchronizer.acquire(AbstractQueuedSynchronizer.java:715)
    java.util.concurrent.locks.AbstractQueuedSynchronizer.acquireShared(AbstractQueuedSynchronizer.java:1027)
    java.util.concurrent.locks.ReentrantReadWriteLock$ReadLock.lock(ReentrantReadWriteLock.java:738)
    org.apache.doris.catalog.Table.readLock(Table.java:177)
    org.apache.doris.catalog.OlapTable.getAndCopyPartitionItems(OlapTable.java:3019)
    org.apache.doris.catalog.MTMV.calculatePartitionMappings(MTMV.java:397)
    org.apache.doris.mtmv.MTMVRefreshContext.buildContext(MTMVRefreshContext.java:49)
    org.apache.doris.mtmv.MTMVRewriteUtil.getMTMVCanRewritePartitions(MTMVRewriteUtil.java:69)
    org.apache.doris.mtmv.MTMVRelationManager.isMVPartitionValid(MTMVRelationManager.java:105)
    org.apache.doris.mtmv.MTMVRelationManager.getAvailableMTMVs(MTMVRelationManager.java:90)
    org.apache.doris.nereids.rules.exploration.mv.InitConsistentMaterializationContextHook.getAvailableMTMVs(InitConsistentMaterializationContextHook.java:55)
    org.apache.doris.nereids.rules.exploration.mv.InitMaterializationContextHook.createAsyncMaterializationContext(InitMaterializationContextHook.java:129)
    org.apache.doris.nereids.rules.exploration.mv.InitMaterializationContextHook.doInitMaterializationContext(InitMaterializationContextHook.java:110)
    org.apache.doris.nereids.rules.exploration.mv.InitConsistentMaterializationContextHook.initMaterializationContext(InitConsistentMaterializationContextHook.java:48)
    org.apache.doris.nereids.rules.exploration.mv.InitMaterializationContextHook.afterAnalyze(InitMaterializationContextHook.java:65)
    org.apache.doris.nereids.NereidsPlanner.lambda$analyze$3(NereidsPlanner.java:302)
    org.apache.doris.nereids.NereidsPlanner$$Lambda$943+0x00007fdeb8b51f68.accept(<unresolved string 0x0>)
    java.util.ArrayList.forEach(ArrayList.java:1511)
    org.apache.doris.nereids.NereidsPlanner.analyze(NereidsPlanner.java:302)
    org.apache.doris.nereids.NereidsPlanner.planWithoutLock(NereidsPlanner.java:221)
    org.apache.doris.nereids.NereidsPlanner.planWithLock(NereidsPlanner.java:207)
    org.apache.doris.nereids.NereidsPlanner.plan(NereidsPlanner.java:133)
    org.apache.doris.nereids.trees.plans.commands.insert.InsertOverwriteTableCommand.run(InsertOverwriteTableCommand.java:133)
    org.apache.doris.job.extensions.mtmv.MTMVTask.exec(MTMVTask.java:231)
    org.apache.doris.job.extensions.mtmv.MTMVTask.run(MTMVTask.java:200)
    org.apache.doris.job.task.AbstractTask.runTask(AbstractTask.java:167)
    org.apache.doris.job.extensions.mtmv.MTMVTask.runTask(MTMVTask.java:310)
    org.apache.doris.job.executor.DefaultTaskExecutorHandler.onEvent(DefaultTaskExecutorHandler.java:50)
    org.apache.doris.job.executor.DefaultTaskExecutorHandler.onEvent(DefaultTaskExecutorHandler.java:33)
    com.lmax.disruptor.WorkProcessor.run(WorkProcessor.java:143)
    java.lang.Thread.run(Thread.java:840)

"mtmv-task-execute-1-thread-9" prio=5 tid=131 WAITING
    blocked write lock: "trade.klines_mv_1d"
    block depends tree:
       "mtmv-task-execute-1-thread-9" blocked at write "trade.klines_mv_1d"
          "mysql-nio-pool-2" hold read "trade.klines_mv_1d", blocked at read "trade.klines_1m"
             "thrift-server-pool-4" blocked at write "trade.klines_1m"
                "mtmv-task-execute-1-thread-2" hold read "trade.klines_1m", blocked at read "trade.klines_mv_1d"
                   "mtmv-task-execute-1-thread-9" blocked at write "trade.klines_mv_1d" (Dead lock)
    events topology order:
       "mysql-nio-pool-2" hold read "trade.klines_mv_1d"
       "mtmv-task-execute-1-thread-2" hold read "trade.klines_1m"
       "mtmv-task-execute-1-thread-9" blocked at write "trade.klines_mv_1d"
       "thrift-server-pool-4" blocked at write "trade.klines_1m"
       "mtmv-task-execute-1-thread-2" blocked at read "trade.klines_mv_1d"
       "mysql-nio-pool-2" blocked at read "trade.klines_1m"


    jdk.internal.misc.Unsafe.park(Native Method)
    java.util.concurrent.locks.LockSupport.park(LockSupport.java:211)
    java.util.concurrent.locks.AbstractQueuedSynchronizer.acquire(AbstractQueuedSynchronizer.java:715)
    java.util.concurrent.locks.AbstractQueuedSynchronizer.acquire(AbstractQueuedSynchronizer.java:938)
    java.util.concurrent.locks.ReentrantReadWriteLock$WriteLock.lock(ReentrantReadWriteLock.java:959)
    org.apache.doris.catalog.Table.writeLockIfExist(Table.java:228)
    org.apache.doris.insertoverwrite.InsertOverwriteUtil.replacePartition(InsertOverwriteUtil.java:74)
    org.apache.doris.nereids.trees.plans.commands.insert.InsertOverwriteTableCommand.run(InsertOverwriteTableCommand.java:208)
    org.apache.doris.job.extensions.mtmv.MTMVTask.exec(MTMVTask.java:231)
    org.apache.doris.job.extensions.mtmv.MTMVTask.run(MTMVTask.java:200)
    org.apache.doris.job.task.AbstractTask.runTask(AbstractTask.java:167)
    org.apache.doris.job.extensions.mtmv.MTMVTask.runTask(MTMVTask.java:310)
    org.apache.doris.job.executor.DefaultTaskExecutorHandler.onEvent(DefaultTaskExecutorHandler.java:50)
    org.apache.doris.job.executor.DefaultTaskExecutorHandler.onEvent(DefaultTaskExecutorHandler.java:33)
    com.lmax.disruptor.WorkProcessor.run(WorkProcessor.java:143)
    java.lang.Thread.run(Thread.java:840)

"tablet stat mgr" daemon prio=5 tid=40 WAITING
    blocked read lock: "trade.klines_1m"
    block depends tree:
       "tablet stat mgr" blocked at read "trade.klines_1m"
          "thrift-server-pool-4" blocked at write "trade.klines_1m"
             "mtmv-task-execute-1-thread-2" hold read "trade.klines_1m", blocked at read "trade.klines_mv_1d"
                "mtmv-task-execute-1-thread-9" blocked at write "trade.klines_mv_1d"
                   "mysql-nio-pool-2" hold read "trade.klines_mv_1d", blocked at read "trade.klines_1m"
                      "thrift-server-pool-4" blocked at write "trade.klines_1m" (Dead lock)
    events topology order:
       "mysql-nio-pool-2" hold read "trade.klines_mv_1d"
       "mtmv-task-execute-1-thread-2" hold read "trade.klines_1m"
       "mtmv-task-execute-1-thread-9" blocked at write "trade.klines_mv_1d"
       "thrift-server-pool-4" blocked at write "trade.klines_1m"
       "mtmv-task-execute-1-thread-2" blocked at read "trade.klines_mv_1d"
       "tablet stat mgr" blocked at read "trade.klines_1m"
       "mysql-nio-pool-2" blocked at read "trade.klines_1m"


    jdk.internal.misc.Unsafe.park(Native Method)
    java.util.concurrent.locks.LockSupport.park(LockSupport.java:211)
    java.util.concurrent.locks.AbstractQueuedSynchronizer.acquire(AbstractQueuedSynchronizer.java:715)
    java.util.concurrent.locks.AbstractQueuedSynchronizer.acquireShared(AbstractQueuedSynchronizer.java:1027)
    java.util.concurrent.locks.ReentrantReadWriteLock$ReadLock.lock(ReentrantReadWriteLock.java:738)
    org.apache.doris.catalog.Table.readLock(Table.java:177)
    org.apache.doris.catalog.Table.readLockIfExist(Table.java:186)
    org.apache.doris.catalog.TabletStatMgr.runAfterCatalogReady(TabletStatMgr.java:123)
    org.apache.doris.common.util.MasterDaemon.runOneCycle(MasterDaemon.java:58)
    org.apache.doris.common.util.Daemon.run(Daemon.java:116)

"report-thread" daemon prio=5 tid=208 WAITING
    blocked read lock: "trade.klines_1m"
    block depends tree:
       "report-thread" blocked at read "trade.klines_1m"
          "thrift-server-pool-4" blocked at write "trade.klines_1m"
             "mtmv-task-execute-1-thread-2" hold read "trade.klines_1m", blocked at read "trade.klines_mv_1d"
                "mtmv-task-execute-1-thread-9" blocked at write "trade.klines_mv_1d"
                   "mysql-nio-pool-2" hold read "trade.klines_mv_1d", blocked at read "trade.klines_1m"
                      "thrift-server-pool-4" blocked at write "trade.klines_1m" (Dead lock)
    events topology order:
       "mysql-nio-pool-2" hold read "trade.klines_mv_1d"
       "mtmv-task-execute-1-thread-2" hold read "trade.klines_1m"
       "mtmv-task-execute-1-thread-9" blocked at write "trade.klines_mv_1d"
       "thrift-server-pool-4" blocked at write "trade.klines_1m"
       "mtmv-task-execute-1-thread-2" blocked at read "trade.klines_mv_1d"
       "report-thread" blocked at read "trade.klines_1m"
       "mysql-nio-pool-2" blocked at read "trade.klines_1m"


    jdk.internal.misc.Unsafe.park(Native Method)
    java.util.concurrent.locks.LockSupport.park(LockSupport.java:211)
    java.util.concurrent.locks.AbstractQueuedSynchronizer.acquire(AbstractQueuedSynchronizer.java:715)
    java.util.concurrent.locks.AbstractQueuedSynchronizer.acquireShared(AbstractQueuedSynchronizer.java:1027)
    java.util.concurrent.locks.ReentrantReadWriteLock$ReadLock.lock(ReentrantReadWriteLock.java:738)
    org.apache.doris.catalog.Table.readLock(Table.java:177)
    org.apache.doris.catalog.Env.getPartitionIdToStorageMediumMap(Env.java:4234)
    org.apache.doris.master.ReportHandler.tabletReport(ReportHandler.java:479)
    org.apache.doris.master.ReportHandler$ReportTask.exec(ReportHandler.java:336)
    org.apache.doris.master.ReportHandler.runOneCycle(ReportHandler.java:1484)
    org.apache.doris.common.util.Daemon.run(Daemon.java:116)

"thrift-server-pool-4" daemon prio=5 tid=514 WAITING
    blocked write lock: "trade.klines_1m"
    block depends tree:
       "thrift-server-pool-4" blocked at write "trade.klines_1m"
          "mtmv-task-execute-1-thread-2" hold read "trade.klines_1m", blocked at read "trade.klines_mv_1d"
             "mtmv-task-execute-1-thread-9" blocked at write "trade.klines_mv_1d"
                "mysql-nio-pool-2" hold read "trade.klines_mv_1d", blocked at read "trade.klines_1m"
                   "thrift-server-pool-4" blocked at write "trade.klines_1m" (Dead lock)
    events topology order:
       "mysql-nio-pool-2" hold read "trade.klines_mv_1d"
       "mtmv-task-execute-1-thread-2" hold read "trade.klines_1m"
       "mtmv-task-execute-1-thread-9" blocked at write "trade.klines_mv_1d"
       "thrift-server-pool-4" blocked at write "trade.klines_1m"
       "mtmv-task-execute-1-thread-2" blocked at read "trade.klines_mv_1d"
       "mysql-nio-pool-2" blocked at read "trade.klines_1m"


    jdk.internal.misc.Unsafe.park(Native Method)
    java.util.concurrent.locks.LockSupport.park(LockSupport.java:211)
    java.util.concurrent.locks.AbstractQueuedSynchronizer.acquire(AbstractQueuedSynchronizer.java:715)
    java.util.concurrent.locks.AbstractQueuedSynchronizer.acquire(AbstractQueuedSynchronizer.java:938)
    java.util.concurrent.locks.ReentrantReadWriteLock$WriteLock.lock(ReentrantReadWriteLock.java:959)
    org.apache.doris.catalog.Table.writeLock(Table.java:224)
    org.apache.doris.catalog.Table.writeLockOrException(Table.java:268)
    org.apache.doris.catalog.Table.writeLockOrDdlException(Table.java:276)
    org.apache.doris.datasource.InternalCatalog.addPartition(InternalCatalog.java:1763)
    org.apache.doris.catalog.Env.addPartition(Env.java:3328)
    org.apache.doris.service.FrontendServiceImpl.createPartition(FrontendServiceImpl.java:3419)
    jdk.internal.reflect.GeneratedMethodAccessor17.invoke(<unresolved string 0x0>)
    jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
    java.lang.reflect.Method.invoke(Method.java:569)
    org.apache.doris.service.FeServer.lambda$start$0(FeServer.java:60)
    org.apache.doris.service.FeServer$$Lambda$307+0x00007fdeb8813cb0.invoke(<unresolved string 0x0>)
    jdk.proxy2.$Proxy45.createPartition(<unresolved string 0x0>)
    org.apache.doris.thrift.FrontendService$Processor$createPartition.getResult(FrontendService.java:4948)
    org.apache.doris.thrift.FrontendService$Processor$createPartition.getResult(FrontendService.java:4928)
    org.apache.thrift.ProcessFunction.process(ProcessFunction.java:38)
    org.apache.thrift.TBaseProcessor.process(TBaseProcessor.java:38)
    org.apache.thrift.server.TThreadPoolServer$WorkerProcess.run(TThreadPoolServer.java:250)
    java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136)
    java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635)
    java.lang.Thread.run(Thread.java:840)

"mysql-nio-pool-2" daemon prio=5 tid=538 WAITING queryId=5cf12cd0be2c4d7f-a52469b84dac5f03
    blocked read lock: "trade.klines_1m"
    hold read lock:  "trade.klines_mv_1d"
    block depends tree:
       "mysql-nio-pool-2" blocked at read "trade.klines_1m"
          "thrift-server-pool-4" blocked at write "trade.klines_1m"
             "mtmv-task-execute-1-thread-2" hold read "trade.klines_1m", blocked at read "trade.klines_mv_1d"
                "mtmv-task-execute-1-thread-9" blocked at write "trade.klines_mv_1d"
                   "mysql-nio-pool-2" hold read "trade.klines_mv_1d", blocked at read "trade.klines_1m" (Dead lock)
    events topology order:
       "mysql-nio-pool-2" hold read "trade.klines_mv_1d"
       "mtmv-task-execute-1-thread-2" hold read "trade.klines_1m"
       "mtmv-task-execute-1-thread-9" blocked at write "trade.klines_mv_1d"
       "thrift-server-pool-4" blocked at write "trade.klines_1m"
       "mtmv-task-execute-1-thread-2" blocked at read "trade.klines_mv_1d"
       "mysql-nio-pool-2" blocked at read "trade.klines_1m"


    jdk.internal.misc.Unsafe.park(Native Method)
    java.util.concurrent.locks.LockSupport.park(LockSupport.java:211)
    java.util.concurrent.locks.AbstractQueuedSynchronizer.acquire(AbstractQueuedSynchronizer.java:715)
    java.util.concurrent.locks.AbstractQueuedSynchronizer.acquireShared(AbstractQueuedSynchronizer.java:1027)
    java.util.concurrent.locks.ReentrantReadWriteLock$ReadLock.lock(ReentrantReadWriteLock.java:738)
    org.apache.doris.catalog.Table.readLock(Table.java:177)
    org.apache.doris.catalog.OlapTable.getAndCopyPartitionItems(OlapTable.java:3019)
    org.apache.doris.mtmv.MTMVRelatedPartitionDescInitGenerator.apply(MTMVRelatedPartitionDescInitGenerator.java:32)
    org.apache.doris.mtmv.MTMVPartitionUtil.generateRelatedPartitionDescs(MTMVPartitionUtil.java:164)
    org.apache.doris.catalog.MTMV.calculatePartitionMappings(MTMV.java:396)
    org.apache.doris.mtmv.MTMVRefreshContext.buildContext(MTMVRefreshContext.java:49)
    org.apache.doris.mtmv.MTMVPartitionUtil.getPartitionsUnSyncTables(MTMVPartitionUtil.java:241)
    org.apache.doris.common.proc.PartitionsProcDir.getPartitionInfosInrernal(PartitionsProcDir.java:266)
    org.apache.doris.common.proc.PartitionsProcDir.getPartitionInfos(PartitionsProcDir.java:228)
    org.apache.doris.common.proc.PartitionsProcDir.fetchResultByFilter(PartitionsProcDir.java:165)
    org.apache.doris.qe.ShowExecutor.handleShowPartitions(ShowExecutor.java:1892)
    org.apache.doris.qe.ShowExecutor.execute(ShowExecutor.java:387)
    org.apache.doris.qe.StmtExecutor.handleShow(StmtExecutor.java:2844)
    org.apache.doris.qe.StmtExecutor.executeByLegacy(StmtExecutor.java:1059)
    org.apache.doris.qe.StmtExecutor.execute(StmtExecutor.java:639)
    org.apache.doris.qe.StmtExecutor.queryRetry(StmtExecutor.java:557)
    org.apache.doris.qe.StmtExecutor.execute(StmtExecutor.java:547)
    org.apache.doris.qe.ConnectProcessor.executeQuery(ConnectProcessor.java:397)
    org.apache.doris.qe.ConnectProcessor.handleQuery(ConnectProcessor.java:238)
    org.apache.doris.qe.MysqlConnectProcessor.handleQuery(MysqlConnectProcessor.java:194)
    org.apache.doris.qe.MysqlConnectProcessor.dispatch(MysqlConnectProcessor.java:222)
    org.apache.doris.qe.MysqlConnectProcessor.processOnce(MysqlConnectProcessor.java:281)
    org.apache.doris.mysql.ReadListener.lambda$handleEvent$0(ReadListener.java:52)
    org.apache.doris.mysql.ReadListener$$Lambda$2081+0x00007fdeb910e588.run(<unresolved string 0x0>)
    java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136)
    java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635)
    java.lang.Thread.run(Thread.java:840)

"Analysis Job Executor-0" daemon prio=5 tid=563 WAITING queryId=5bfa3d75a1ce45c4-89158ab6b72b9b1b
    blocked read lock: "trade.klines_1m"
    block depends tree:
       "Analysis Job Executor-0" blocked at read "trade.klines_1m"
          "thrift-server-pool-4" blocked at write "trade.klines_1m"
             "mtmv-task-execute-1-thread-2" hold read "trade.klines_1m", blocked at read "trade.klines_mv_1d"
                "mtmv-task-execute-1-thread-9" blocked at write "trade.klines_mv_1d"
                   "mysql-nio-pool-2" hold read "trade.klines_mv_1d", blocked at read "trade.klines_1m"
                      "thrift-server-pool-4" blocked at write "trade.klines_1m" (Dead lock)
    events topology order:
       "mysql-nio-pool-2" hold read "trade.klines_mv_1d"
       "mtmv-task-execute-1-thread-2" hold read "trade.klines_1m"
       "mtmv-task-execute-1-thread-9" blocked at write "trade.klines_mv_1d"
       "thrift-server-pool-4" blocked at write "trade.klines_1m"
       "mtmv-task-execute-1-thread-2" blocked at read "trade.klines_mv_1d"
       "Analysis Job Executor-0" blocked at read "trade.klines_1m"
       "mysql-nio-pool-2" blocked at read "trade.klines_1m"


    jdk.internal.misc.Unsafe.park(Native Method)
    java.util.concurrent.locks.LockSupport.park(LockSupport.java:211)
    java.util.concurrent.locks.AbstractQueuedSynchronizer.acquire(AbstractQueuedSynchronizer.java:715)
    java.util.concurrent.locks.AbstractQueuedSynchronizer.acquireShared(AbstractQueuedSynchronizer.java:1027)
    java.util.concurrent.locks.ReentrantReadWriteLock$ReadLock.lock(ReentrantReadWriteLock.java:738)
    org.apache.doris.catalog.Table.readLock(Table.java:177)
    org.apache.doris.common.util.MetaLockUtils.readLockTables(MetaLockUtils.java:51)
    org.apache.doris.qe.StmtExecutor.analyze(StmtExecutor.java:1297)
    org.apache.doris.qe.StmtExecutor.executeInternalQuery(StmtExecutor.java:3330)
    org.apache.doris.statistics.OlapAnalysisTask.collectBasicStat(OlapAnalysisTask.java:188)
    org.apache.doris.statistics.OlapAnalysisTask.doSample(OlapAnalysisTask.java:119)
    org.apache.doris.statistics.OlapAnalysisTask.doExecute(OlapAnalysisTask.java:83)
    org.apache.doris.statistics.BaseAnalysisTask.execute(BaseAnalysisTask.java:248)
    org.apache.doris.statistics.AnalysisTaskWrapper.lambda$new$0(AnalysisTaskWrapper.java:43)
    org.apache.doris.statistics.AnalysisTaskWrapper$$Lambda$2899+0x00007fdeb92278b8.call(<unresolved string 0x0>)
    java.util.concurrent.FutureTask.run(FutureTask.java:264)
    org.apache.doris.statistics.AnalysisTaskWrapper.run(AnalysisTaskWrapper.java:66)
    java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:539)
    java.util.concurrent.FutureTask.run(FutureTask.java:264)
    java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136)
    java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635)
    java.lang.Thread.run(Thread.java:840)


===== Queries =====
"mysql-nio-pool-0" daemon prio=5 tid=284 TIMED_WAITING queryId=44a45d67bdd74cac-b9c9e03da38d9697
    originSqlIdx: 0
    catalog:      internal
    db:           trade
    sql:
insert into klines_1m(symbol, trade_time, low_price, high_price, open_price, close_price) values
('BTCUSDT', '2024-04-23 00:00:00', 65994.38, 66020.43, 65994.39, 66010.0),
('BTCUSDT', '2024-04-23 00:01:00', 65942.7, 66011.99, 66010.0, 65942.7),
('BTCUSDT', '2024-04-23 00:02:00', 65942.7, 65975.98, 65942.7, 65954.53),
('BTCUSDT', '2024-04-23 00:03:00', 65949.21, 65982.68, 65954.53, 65974.16),
('BTCUSDT', '2024-04-23 00:04:00', 65974.15, 66005.84, 65974.15, 65999.56)

"mysql-nio-pool-2" daemon prio=5 tid=538 WAITING queryId=5cf12cd0be2c4d7f-a52469b84dac5f03
    originSqlIdx: 0
    catalog:      internal
    db:           trade
    sql:
show partitions from klines_mv_1d

"Analysis Job Executor-0" daemon prio=5 tid=563 WAITING queryId=5bfa3d75a1ce45c4-89158ab6b72b9b1b
    originSqlIdx: 0
    catalog:      internal
    db:           __internal_schema
    sql:
SELECT SUBSTRING(CAST(MIN(`high_price`) AS STRING), 1, 1024) as min, SUBSTRING(CAST(MAX(`high_price`) AS STRING), 1, 1024) as max FROM `trade`.`klines_1m` 
```