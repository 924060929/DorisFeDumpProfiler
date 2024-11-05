package org.apache.doris.fe.dump;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import static org.apache.doris.fe.dump.AnalyzeDorisFeHprof.State.BLOCKED;
import static org.apache.doris.fe.dump.AnalyzeDorisFeHprof.State.NEW;
import static org.apache.doris.fe.dump.AnalyzeDorisFeHprof.State.RUNNABLE;
import static org.apache.doris.fe.dump.AnalyzeDorisFeHprof.State.TERMINATED;
import static org.apache.doris.fe.dump.AnalyzeDorisFeHprof.State.TIMED_WAITING;
import static org.apache.doris.fe.dump.AnalyzeDorisFeHprof.State.WAITING;
import org.graalvm.visualvm.lib.jfluid.heap.ArrayItemValue;
import org.graalvm.visualvm.lib.jfluid.heap.GCRoot;
import static org.graalvm.visualvm.lib.jfluid.heap.GCRoot.JNI_LOCAL;
import org.graalvm.visualvm.lib.jfluid.heap.Heap;
import org.graalvm.visualvm.lib.jfluid.heap.HeapFactory;
import org.graalvm.visualvm.lib.jfluid.heap.Instance;
import org.graalvm.visualvm.lib.jfluid.heap.JavaClass;
import org.graalvm.visualvm.lib.jfluid.heap.JavaFrameGCRoot;
import org.graalvm.visualvm.lib.jfluid.heap.JniLocalGCRoot;
import org.graalvm.visualvm.lib.jfluid.heap.ObjectArrayInstance;
import org.graalvm.visualvm.lib.jfluid.heap.StringInstanceUtils;
import org.graalvm.visualvm.lib.jfluid.heap.ThreadObjectGCRoot;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

public class AnalyzeDorisFeHprof {
    private static final String LOCAL_VARIABLE = "local variables";

    private final Heap heap;
    private Map<Long, ThreadEntity> threadObjIdToThread;
    private Map<Long, ContextId> threadObjIdToContextId = Maps.newLinkedHashMap();
    private Map<ThreadObjectGCRoot, Map<Integer, List<GCRoot>>> threadStackFrameLocals;

    // tid -> ThreadBlockReason
    private Map<Long, ThreadBlockReason> blockThreads;
    private TableLockDependencies tableLockDependencies;

    public AnalyzeDorisFeHprof(Heap heap) {
        this.heap = heap;
    }

    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            System.out.println("Usage: java -jar DorisFeDumpProfiler-*.jar fe.hprof");
            System.exit(1);
        }
        Heap heap = HeapFactory.createHeap(new File(args[0]));
        new AnalyzeDorisFeHprof(heap).analyze();
    }

    public void analyze() {
        threadStackFrameLocals = computeJavaFrameMap(heap.getGCRoots());
        threadObjIdToThread = getThreads(heap);
        threadObjIdToContextId = analyzeContextId();

        blockThreads = analyzeBlockingThreads();
        tableLockDependencies = bindTableLockToSyncByBlockReason(blockThreads);

        // syncObj -> LockHolder
        Map<Instance, LockHolder> lockHolders = analyzeHoldTableLockThreads(tableLockDependencies);

        Map<Instance, List<ThreadSync>> syncToWaitingThreads = analyzeAndPrintTableLockQueue(lockHolders,
                tableLockDependencies);
        System.out.println();

        printThreadsWithLock(blockThreads, lockHolders, tableLockDependencies, syncToWaitingThreads);
        System.out.println();

        printQueries();
        System.out.println();
    }

    private ThreadEntity toThreadEntity(ThreadObjectGCRoot thread) {
        Instance instance = thread.getInstance();
        String threadName = StringInstanceUtils.getDetailsString((Instance) instance.getValueOfField("name"));
        Boolean daemon = (Boolean) instance.getValueOfField("daemon");
        Integer priority = (Integer) instance.getValueOfField("priority");
        Long tid = (Long) instance.getValueOfField("tid");
        Integer threadStatus = (Integer) instance.getValueOfField("threadStatus");

        List<StackFrameEntity> stackFrames = toStackFrameEntities(thread);
        return new ThreadEntity(threadName, daemon, priority, tid, threadStatus, thread, stackFrames);
    }

    private List<StackFrameEntity> toStackFrameEntities(ThreadObjectGCRoot thread) {
        StackTraceElement[] stackTrace = thread.getStackTrace();
        Map<Integer, List<GCRoot>> frameToLocals = threadStackFrameLocals.get(thread);

        List<StackFrameEntity> stackFrameEntities = Lists.newArrayListWithCapacity(stackTrace.length);
        for (int i = 0; i < stackTrace.length; i++) {
            StackTraceElement stackTraceElement = stackTrace[i];
            List<GCRoot> gcRoots = frameToLocals.get(i);
            if (gcRoots == null) {
                gcRoots = Lists.newArrayList();
            }
            stackFrameEntities.add(new StackFrameEntity(stackTraceElement, gcRoots));
        }
        return stackFrameEntities;
    }

    private Map<Long, ThreadEntity> getThreads(Heap heap) {
        Map<Long, ThreadEntity> threads = Maps.newLinkedHashMap();
        Collection<GCRoot> gcRoots = heap.getGCRoots();
        for (GCRoot root : gcRoots) {
            if (root.getKind().equals(GCRoot.THREAD_OBJECT)) {
                ThreadObjectGCRoot threadRoot = (ThreadObjectGCRoot) root;
                ThreadEntity threadEntity = toThreadEntity(threadRoot);
                threads.put(threadRoot.getInstance().getInstanceId(), threadEntity);
            }
        }
        return threads;
    }

    private static Map<ThreadObjectGCRoot, Map<Integer, List<GCRoot>>> computeJavaFrameMap(Collection<GCRoot> roots) {
        Map<ThreadObjectGCRoot, Map<Integer,List<GCRoot>>> javaFrameMap = new HashMap();
        for (GCRoot root : roots) {
            ThreadObjectGCRoot threadObj;
            Integer frameNo;

            if (GCRoot.JAVA_FRAME.equals(root.getKind())) {
                JavaFrameGCRoot frameGCroot = (JavaFrameGCRoot) root;
                threadObj = frameGCroot.getThreadGCRoot();
                frameNo = frameGCroot.getFrameNumber();
            } else if (JNI_LOCAL.equals(root.getKind())) {
                JniLocalGCRoot jniGCroot = (JniLocalGCRoot) root;
                threadObj = jniGCroot.getThreadGCRoot();
                frameNo = jniGCroot.getFrameNumber();
            } else {
                continue;
            }

            Map<Integer,List<GCRoot>> stackMap = javaFrameMap.get(threadObj);
            List<GCRoot> locals;

            if (stackMap == null) {
                stackMap = new HashMap();
                javaFrameMap.put(threadObj, stackMap);
            }
            locals = stackMap.get(frameNo);
            if (locals == null) {
                locals = new ArrayList(2);
                stackMap.put(frameNo,locals);
            }
            locals.add(root);
        }
        return javaFrameMap;
    }

    private Map<Long, ContextId> analyzeContextId() {
        Map<Long, ContextId> threadObjIdToContextId = Maps.newLinkedHashMap();
        for (ThreadEntity thread : threadObjIdToThread.values()) {
            ContextId contextId = findContextId(thread);
            if (contextId != null) {
                threadObjIdToContextId.put(thread.tid, contextId);
            }
        }
        return threadObjIdToContextId;
    }

    private ContextId findContextId(ThreadEntity thread) {
        for (StackFrameEntity stackFrame : thread.stackFrames) {
            for (GCRoot local : stackFrame.locals) {
                Instance javaLocal = local.getInstance();
                JavaClass classInfo = javaLocal.getJavaClass();
                if (classInfo == null || !hasSuperClass(classInfo, "org.apache.doris.qe.StmtExecutor")) {
                    continue;
                }
                Instance connectContext = (Instance) javaLocal.getValueOfField("context");
                if (connectContext == null) {
                    continue;
                }
                Instance queryId = (Instance) connectContext.getValueOfField("queryId");
                String queryIdStr = null;
                if (queryId != null) {
                    Long lo = (Long) queryId.getValueOfField("lo");
                    Long hi = (Long) queryId.getValueOfField("hi");
                    queryIdStr = printId(hi, lo);
                }

                Instance loadId = (Instance) connectContext.getValueOfField("loadId");
                String loadIdStr = null;
                if (loadId != null) {
                    Long lo = (Long) loadId.getValueOfField("lo");
                    Long hi = (Long) loadId.getValueOfField("hi");
                    loadIdStr = printId(hi, lo);
                }

                String defaultCatalog = StringInstanceUtils.getDetailsString((Instance) connectContext.getValueOfField("defaultCatalog"));
                String currentDb = StringInstanceUtils.getDetailsString((Instance) connectContext.getValueOfField("currentDb"));

                Instance originStmt = (Instance) javaLocal.getValueOfField("originStmt");
                int sqlIdx = 0;
                String sql = null;
                if (originStmt != null) {
                    sqlIdx = (Integer) originStmt.getValueOfField("idx");
                    sql = StringInstanceUtils.getDetailsString((Instance) originStmt.getValueOfField("originStmt"));
                }
                return new ContextId(queryIdStr, loadIdStr, defaultCatalog, currentDb, sql, sqlIdx);
            }
        }
        return null;
    }

    private void printQueries() {
        System.out.println("===== Queries =====");
        for (ThreadEntity threadEntity : threadObjIdToThread.values()) {
            ContextId contextId = threadObjIdToContextId.get(threadEntity.tid);
            if (contextId == null || contextId.sql == null) {
                continue;
            }
            printThreadWithContextId(threadEntity);
            System.out.println("    originSqlIdx: " + contextId.sqlIdx);
            System.out.println("    catalog:      " + contextId.defaultCatalog);
            System.out.println("    db:           " + contextId.currentDb);
            System.out.println("    sql:\n" + contextId.sql);
            System.out.println();
        }
    }

    private void printThreadWithContextId(ThreadEntity threadEntity) {
        ContextId contextId = threadObjIdToContextId.get(threadEntity.tid);
        if (contextId == null || (contextId.queryId == null && contextId.loadId == null)) {
            System.out.println(threadEntity);
        } else {
            String extraInfo = "";
            if (contextId.queryId != null) {
                extraInfo += " queryId=" + contextId.queryId;
            }
            if (contextId.loadId != null) {
                extraInfo += " loadId=" + contextId.loadId;
            }
            System.out.println(threadEntity + extraInfo);
        }
    }

    private Map<Instance, List<ThreadSync>> analyzeAndPrintTableLockQueue(
            Map<Instance, LockHolder> lockHolders, TableLockDependencies tableLockDependencies) {
        System.out.println("===== Locks =====");
        Map<Instance, List<ThreadSync>> analyzeWaitThreads = new LinkedHashMap<>();
        for (Entry<DbTable, Instance> kv : tableLockDependencies.dbTableToSyncObj.entrySet()) {
            DbTable dbTable = kv.getKey();
            Instance sync = kv.getValue();

            System.out.println(dbTable + " lock:");
            LockHolder lockHolder = lockHolders.get(sync);
            if (lockHolder != null) {
                if (lockHolder.holdWriteLockThread != null) {
                    System.out.println("    hold write lock: \"" + lockHolder.holdWriteLockThread.threadName + "\"");
                }
                if (lockHolder.holdReadLockThreads != null) {
                    for (Entry<ThreadEntity, Integer> kv2 : lockHolder.holdReadLockThreads.entrySet()) {
                        ThreadEntity readThread = kv2.getKey();
                        System.out.println("    hold read lock:  \"" + readThread.threadName + "\"");
                    }
                }
            }
            analyzeAndPrintSyncQueue(threadObjIdToThread, sync, analyzeWaitThreads);
            System.out.println();
        }
        return analyzeWaitThreads;
    }

    private void analyzeAndPrintSyncQueue(Map<Long, ThreadEntity> threadObjIdToThread, Instance sync, Map<Instance, List<ThreadSync>> analyzeWaitThreads) {
        List<ThreadSync> threadSyncs = Lists.newArrayList();
        analyzeWaitThreads.put(sync, threadSyncs);

        Instance head = (Instance) sync.getValueOfField("head");
        if (head == null) {
            return;
        }
        Instance next = (Instance) head.getValueOfField("next");
        if (next == null) {
            return;
        }
        System.out.println("    waiting queue:");
        do {
            Instance thread = (Instance) next.getValueOfField("thread");
            // jdk8-jdk13
            if (thread != null) {
                Instance nextWaiter = (Instance) next.getValueOfField("nextWaiter");
                ThreadEntity threadEntity = threadObjIdToThread.get(thread.getInstanceId());
                if (nextWaiter == null) {
                    threadSyncs.add(new ThreadSync(false, threadEntity, sync));
                    System.out.println("       write: \"" + threadEntity.threadName + "\"");
                } else {
                    threadSyncs.add(new ThreadSync(true, threadEntity, sync));
                    System.out.println("       read:  \"" + threadEntity.threadName + "\"");
                }
            } else {
                // jdk14+
                thread = (Instance) next.getValueOfField("waiter");
                if (thread != null) {
                    boolean isRead = hasSuperClass(next.getJavaClass(), "java.util.concurrent.locks.AbstractQueuedSynchronizer$SharedNode");
                    ThreadEntity threadEntity = threadObjIdToThread.get(thread.getInstanceId());
                    if (!isRead) {
                        threadSyncs.add(new ThreadSync(false, threadEntity, sync));
                        System.out.println("       write: \"" + threadEntity.threadName + "\"");
                    } else {
                        threadSyncs.add(new ThreadSync(true, threadEntity, sync));
                        System.out.println("       read:  \"" + threadEntity.threadName + "\"");
                    }
                }
            }
            next = (Instance) next.getValueOfField("next");
        } while (next != null);
    }

    private void printThreadsWithLock(
            Map<Long, ThreadBlockReason> blockThreads, Map<Instance, LockHolder> lockHolders, TableLockDependencies tableLockDependencies,
            Map<Instance, List<ThreadSync>> syncToWaitingThreads) {

        System.out.println("===== Threads =====");
        for (ThreadBlockReason threadBlockReason : blockThreads.values()) {
            printThreadWithLock(threadBlockReason.threadEntity, threadBlockReason, lockHolders, tableLockDependencies);
            ThreadDepends depends = analyzeBlockTree(threadBlockReason, lockHolders, syncToWaitingThreads, tableLockDependencies, blockThreads);

            System.out.println("    block depends tree:");
            printThreadDependsTree(depends, 1);

            printStackFrames(threadBlockReason.threadEntity);
            System.out.println();
        }

        Set<ThreadEntity> nonBlockingThreads = collectNonBlockingThreads(blockThreads, lockHolders, tableLockDependencies);
        for (ThreadEntity nonBlockingThread : nonBlockingThreads) {
            printThreadWithLock(nonBlockingThread, null, lockHolders, tableLockDependencies);
            printStackFrames(nonBlockingThread);
            System.out.println();
        }
    }

    private void printStackFrames(ThreadEntity threadEntity) {
        List<StackFrameEntity> stackFrames = threadEntity.stackFrames;
        if (stackFrames != null && !stackFrames.isEmpty()) {
            System.out.println();
        }
        for (StackFrameEntity stackFrame : stackFrames) {
            System.out.println("    " + stackFrame);
        }
    }

    private void printThreadDependsTree(ThreadDepends depends, int level) {
        String indent = "    " + Strings.repeat("   ", level);
        String ThreadObjectGCRoot;
        if (depends instanceof RunningThreadDepends) {
            ThreadObjectGCRoot = "\"" + depends.threadSync.threadEntity.threadName + "\" running (Live lock)";
        } else if (depends instanceof DeadLockDepends) {
            ThreadObjectGCRoot = "\"" + depends.threadSync.threadEntity.threadName + "\" blocked by " + (depends.threadSync.isRead ? "read " : "write ") + depends.dbTable + " (Dead lock)";
        } else {
            ThreadObjectGCRoot = "\"" + depends.threadSync.threadEntity.threadName + "\" blocked by " + (depends.threadSync.isRead ? "read " : "write ") + depends.dbTable;
        }
        System.out.println(indent + ThreadObjectGCRoot);
        for (ThreadDepends nextDepends : depends.depends.values()) {
            printThreadDependsTree(nextDepends, level + 1);
        }
    }

    private ThreadDepends analyzeBlockTree(ThreadBlockReason threadBlockReason,
            Map<Instance, LockHolder> lockHolders,
            Map<Instance, List<ThreadSync>> syncToWaitingThreads,
            TableLockDependencies tableLockDependencies, Map<Long, ThreadBlockReason> blockThreads) {
        Map<Long, ThreadDepends> visitedThreadIds = Maps.newLinkedHashMap();
        Instance sync = threadBlockReason.blockingReason.syncFrame.sync;
        ThreadDepends depends = new ThreadDepends(
                new ThreadSync(threadBlockReason.blockingReason.lockMethod.isRead, threadBlockReason.threadEntity, sync),
                threadBlockReason.blockingReason.dbTable
        );
        visitedThreadIds.put(threadBlockReason.threadEntity.tid, depends);
        doAnalyzeBlockTree(visitedThreadIds, lockHolders, depends, syncToWaitingThreads, tableLockDependencies, blockThreads);
        return depends;
    }

    private void doAnalyzeBlockTree(Map<Long, ThreadDepends> visitedThreads,
            Map<Instance, LockHolder> lockHolders, ThreadDepends depends,
            Map<Instance, List<ThreadSync>> syncToWaitingThreads,
            TableLockDependencies tableLockDependencies, Map<Long, ThreadBlockReason> blockThreads) {
        Instance sync = depends.threadSync.sync;
        if (depends.threadSync.isRead) {
            LockHolder lockHolder = lockHolders.get(sync);
            if (lockHolder != null && lockHolder.holdWriteLockThread != null) {
                analyzeNextDepends(
                        visitedThreads, lockHolders, depends, syncToWaitingThreads, tableLockDependencies,
                        blockThreads, sync, lockHolder.holdWriteLockThread, false
                );
            } else if (lockHolder != null) {
                List<ThreadSync> threadSyncs = syncToWaitingThreads.get(sync);
                if (threadSyncs != null) {
                    for (ThreadSync threadSync : threadSyncs) {
                        if (!threadSync.isRead) {
                            analyzeNextDepends(visitedThreads, lockHolders, depends,
                                    syncToWaitingThreads, tableLockDependencies,
                                    blockThreads, sync, threadSync.threadEntity, false);
                            break;
                        } else if (threadSync.threadEntity.tid == depends.threadSync.threadEntity.tid) {
                            break;
                        }
                    }
                }
            }
        } else {
            LockHolder lockHolder = lockHolders.get(sync);
            if (lockHolder != null && lockHolder.getHoldReadLockThreads() != null) {
                for (ThreadEntity readThread : lockHolder.getHoldReadLockThreads().keySet()) {
                    analyzeNextDepends(visitedThreads, lockHolders, depends, syncToWaitingThreads, tableLockDependencies,
                            blockThreads, sync, readThread, true);
                }
            } else {
                List<ThreadSync> threadSyncs = syncToWaitingThreads.get(sync);
                if (threadSyncs != null) {
                    for (ThreadSync threadSync : threadSyncs) {
                        if (threadSync.threadEntity.tid == depends.threadSync.threadEntity.tid) {
                            break;
                        } else {
                            analyzeNextDepends(visitedThreads, lockHolders, depends, syncToWaitingThreads, tableLockDependencies,
                                    blockThreads, sync, threadSync.threadEntity, threadSync.isRead);
                            break;
                        }
                    }
                }
            }
        }
    }

    private void analyzeNextDepends(Map<Long, ThreadDepends> visitedThreads,
            Map<Instance, LockHolder> lockHolders, ThreadDepends depends,
            Map<Instance, List<ThreadSync>> syncToWaitingThreads,
            TableLockDependencies tableLockDependencies,
            Map<Long, ThreadBlockReason> blockThreads, Instance sync, ThreadEntity nextThread, boolean isRead) {
        ThreadDepends nextDepends = visitedThreads.get(nextThread.tid);
        if (nextDepends != null) {
            depends.depends.put(nextThread.tid, new DeadLockDepends(nextDepends.threadSync, nextDepends.dbTable));
            return;
        }

        ThreadBlockReason threadBlockReason = blockThreads.get(nextThread.tid);
        if (threadBlockReason == null) {
            depends.depends.put(nextThread.tid, new RunningThreadDepends(new ThreadSync(isRead, nextThread, sync)));
            return;
        }

        ThreadSync holdWriteLockThread = new ThreadSync(isRead, nextThread, sync);
        DbTable dbTable = tableLockDependencies.syncObjToDbTable.get(sync);
        nextDepends = new ThreadDepends(holdWriteLockThread, dbTable);
        depends.depends.put(nextThread.tid, nextDepends);
        visitedThreads.put(nextThread.tid, nextDepends);

        doAnalyzeBlockTree(
                visitedThreads, lockHolders, nextDepends, syncToWaitingThreads, tableLockDependencies, blockThreads);
    }

    private void printThreadWithLock(ThreadEntity threadEntity, ThreadBlockReason threadBlockReason, Map<Instance, LockHolder> lockHolders, TableLockDependencies tableLockDependencies) {
        printThreadWithContextId(threadEntity);
        if (threadBlockReason != null) {
            System.out.println("    blocked " + threadBlockReason.blockingReason);
        }
        HoldTableLocks holdTableLock = findHoldTableLock(threadEntity, lockHolders, tableLockDependencies);
        if (!holdTableLock.writeTables.isEmpty()) {
            for (DbTable writeTable : holdTableLock.writeTables) {
                System.out.println("    hold write lock: \"" + writeTable + "\"");
            }
        }
        if (!holdTableLock.readTables.isEmpty()) {
            for (DbTable readTable : holdTableLock.readTables) {
                System.out.println("    hold read lock:  \"" + readTable + "\"");
            }
        }
    }

    private Set<ThreadEntity> collectNonBlockingThreads(
            Map<Long, ThreadBlockReason> blockThreads, Map<Instance, LockHolder> lockHolders,
            TableLockDependencies tableLockDependencies) {
        Set<ThreadEntity> nonBlockingThreads = Sets.newLinkedHashSet();
        for (Entry<Instance, LockHolder> kv : lockHolders.entrySet()) {
            Instance sync = kv.getKey();
            DbTable dbTable = tableLockDependencies.syncObjToDbTable.get(sync);
            if (dbTable == null) {
                continue;
            }
            LockHolder holder = kv.getValue();
            if (holder.holdWriteLockThread != null && !blockThreads.containsKey(holder.holdWriteLockThread.tid)) {
                ThreadEntity writeThread = holder.holdWriteLockThread;
                nonBlockingThreads.add(writeThread);
            } else if (holder.holdReadLockThreads != null) {
                for (ThreadEntity readThread : holder.holdReadLockThreads.keySet()) {
                    if (!blockThreads.containsKey(readThread.tid)) {
                        nonBlockingThreads.add(readThread);
                    }
                }
            }
        }
        return nonBlockingThreads;
    }

    private HoldTableLocks findHoldTableLock(
            ThreadEntity threadEntity,
            Map<Instance, LockHolder> lockHolders,
            TableLockDependencies tableLockDependencies) {
        HoldTableLocks holdTableLocks = new HoldTableLocks();
        for (Entry<Instance, LockHolder> kv : lockHolders.entrySet()) {
            Instance sync = kv.getKey();
            LockHolder holder = kv.getValue();
            DbTable relateTable = tableLockDependencies.syncObjToDbTable.get(sync);
            if (holder.holdWriteLockThread != null && holder.holdWriteLockThread.tid == threadEntity.tid) {
                holdTableLocks.writeTables.add(relateTable);
            }
            if (holder.holdReadLockThreads != null) {
                for (ThreadEntity holdThread : holder.holdReadLockThreads.keySet()) {
                    if (holdThread.tid == threadEntity.tid) {
                        holdTableLocks.readTables.add(relateTable);
                        break;
                    }
                }
            }
        }
        return holdTableLocks;
    }

    private TableLockDependencies bindTableLockToSyncByBlockReason(Map<Long, ThreadBlockReason> blockThreads) {
        TableLockDependencies tableLockDependencies = new TableLockDependencies();
        for (ThreadBlockReason threadBlockReason : blockThreads.values()) {
            tableLockDependencies.bind(
                    threadBlockReason.blockingReason.dbTable,
                    threadBlockReason.blockingReason.syncFrame.sync
            );
        }
        return tableLockDependencies;
    }

    private Map<Instance, LockHolder> analyzeHoldTableLockThreads(TableLockDependencies tableLockDependencies) {
        Map<Instance, LockHolder> lockHolders = Maps.newLinkedHashMap();
        for (ThreadEntity threadEntity : threadObjIdToThread.values()) {
            for (StackFrameEntity stackFrame : threadEntity.stackFrames) {
                for (GCRoot javaLocal : stackFrame.locals) {
                    JavaClass classEntity = javaLocal.getInstance().getJavaClass();
                    if (classEntity == null) {
                        continue;
                    }
                    if (hasSuperClass(classEntity, "org.apache.doris.catalog.OlapTable")) {
                        analyzeHoldTableLock(javaLocal.getInstance(), lockHolders, tableLockDependencies);
                    } else if (hasSuperClass(classEntity, "org.apache.doris.nereids.CascadesContext$Lock")) {
                        analyzeHoldTableLockInCascadesContext(javaLocal.getInstance(), lockHolders, tableLockDependencies);
                    }
                }
            }
        }
        return lockHolders;
    }

    private LockHolder analyzeReentrantReadWriteLockThreads(Instance sync, Map<Instance, LockHolder> lockHolders) {
        LockHolder lockHolder = lockHolders.get(sync);
        if (lockHolder != null) {
            return lockHolder;
        }
        Integer state = (Integer) sync.getValueOfField("state");

        int SHARED_SHIFT = 16;
        int EXCLUSIVE_MASK = (1 << SHARED_SHIFT) - 1;
        boolean isWriteLocked = (state & EXCLUSIVE_MASK) > 0;
        boolean isReadLocked = (state >>> SHARED_SHIFT) > 0;
        if (isWriteLocked) {
            Instance exclusiveOwnerThread = (Instance) sync.getValueOfField("exclusiveOwnerThread");
            ThreadEntity writeLockThread = threadObjIdToThread.get(exclusiveOwnerThread.getInstanceId());
            lockHolder = new LockHolder(null, writeLockThread);
            lockHolders.put(sync, lockHolder);
            return lockHolder;
        } else if (isReadLocked) {
            Instance readHolds = (Instance) sync.getValueOfField("readHolds");
            Map<ThreadEntity, Integer> holdReadLockThreads = Maps.newLinkedHashMap();
            for (ThreadEntity threadEntity : threadObjIdToThread.values()) {
                LockHolder holdReadLock = isHoldReadLock(threadEntity, sync, readHolds);
                if (holdReadLock.isHoldReadLock()) {
                    holdReadLockThreads.putAll(holdReadLock.holdReadLockThreads);
                }
            }
            lockHolder = new LockHolder(holdReadLockThreads, null);
            lockHolders.put(sync, lockHolder);
            return lockHolder;
        }
        return LockHolder.noLock();
    }

    private LockHolder isHoldReadLock(ThreadEntity threadEntity, Instance sync, Instance readHolds) {
        Instance firstReader = (Instance) sync.getValueOfField("firstReader");
        if (firstReader != null && firstReader.getInstanceId() == threadEntity.threadGcRoot.getInstance().getInstanceId()) {
            Integer firstReaderHoldCount = (Integer) sync.getValueOfField("firstReaderHoldCount");
            if (firstReaderHoldCount != null && firstReaderHoldCount > 0) {
                LinkedHashMap<ThreadEntity, Integer> info = new LinkedHashMap<>();
                info.put(threadEntity, firstReaderHoldCount);
                return LockHolder.holdReadLock(info);
            }
        }

        ThreadEntity threadObjEntity = threadObjIdToThread.get(threadEntity.threadGcRoot.getInstance().getInstanceId());
        Instance threadLocals = (Instance) threadObjEntity.threadGcRoot.getInstance().getValueOfField("threadLocals");
        if (threadLocals == null) {
            return LockHolder.noLock();
        }

        ObjectArrayInstance table = (ObjectArrayInstance) threadLocals.getValueOfField("table");
        for (ArrayItemValue item : table.getItems()) {
            if (item == null || item.getInstance() == null) {
                continue;
            }
            Instance entryObj = item.getInstance();
            Instance reference = (Instance) entryObj.getValueOfField("referent");
            if (reference == null || reference.getInstanceId() != readHolds.getInstanceId()) {
                continue;
            }
            Instance entryValue = (Instance) entryObj.getValueOfField("value");
            if (entryValue == null) {
                continue;
            }
            Integer readCount = (Integer) entryValue.getValueOfField("count");
            Long tid = (Long) entryValue.getValueOfField("tid");
            if (readCount > 0 && tid == threadEntity.tid) {
                LinkedHashMap<ThreadEntity, Integer> info = new LinkedHashMap<>();
                info.put(threadEntity, readCount);
                return LockHolder.holdReadLock(info);
            }
        }
        return LockHolder.noLock();
    }

    private void analyzeHoldTableLockInCascadesContext(
            Instance cascadesTableLock, Map<Instance, LockHolder> lockHolders, TableLockDependencies tableLockDependencies) {
        Instance lockStack = (Instance) cascadesTableLock.getValueOfField("locked");
        ObjectArrayInstance stackArray = (ObjectArrayInstance) lockStack.getValueOfField("elementData");
        Integer elementCount = (Integer) lockStack.getValueOfField("elementCount");
        for (int i = 0; i < elementCount; i++) {
            Instance lockItem = stackArray.getItems().get(i).getInstance();
            JavaClass lockClass = lockItem.getJavaClass();
            if (!hasSuperClass(lockClass, "org.apache.doris.catalog.Table")) {
                continue;
            }
            analyzeHoldTableLock(lockItem, lockHolders, tableLockDependencies);
        }
    }

    private DbTable analyzeTable(Instance table) {
        String tableName = StringInstanceUtils.getDetailsString((Instance) table.getValueOfField("name"));
        String dbName = StringInstanceUtils.getDetailsString((Instance) table.getValueOfField("qualifiedDbName"));
        return new DbTable(dbName, tableName);
    }

    private void analyzeHoldTableLock(
            Instance table, Map<Instance, LockHolder> lockHolders, TableLockDependencies tableLockDependencies) {
        DbTable dbTable = analyzeTable(table);
        Instance rwLock = (Instance) table.getValueOfField("rwLock");
        Instance sync = (Instance) rwLock.getValueOfField("sync");
        tableLockDependencies.bind(dbTable, sync);
        analyzeReentrantReadWriteLockThreads(sync, lockHolders);
    }

    private Map<Long, ThreadBlockReason> analyzeBlockingThreads() {
        Map<Long, ThreadBlockReason> threadLockDependencies = Maps.newLinkedHashMap();
        for (ThreadEntity threadEntity : threadObjIdToThread.values()) {
            analyzeBlockByReadWriteLockThread(threadEntity, threadLockDependencies);
        }
        return threadLockDependencies;
    }

    private void analyzeBlockByReadWriteLockThread(ThreadEntity threadEntity, Map<Long, ThreadBlockReason> threadLockDependencies) {
        List<StackFrameEntity> stackFrames = threadEntity.stackFrames;
        if (stackFrames.isEmpty()) {
            return;
        }
        StackFrameEntity topStackFrame = stackFrames.get(0);
        String topStackFrameString = topStackFrame.toString();
        if (topStackFrameString.contains("jdk.internal.misc.Unsafe.park(") || topStackFrameString.contains("sun.misc.Unsafe.park(")) {
            boolean findOlapTable = false;
            for (int i = 0; i < stackFrames.size(); i++) {
                StackFrameEntity stackFrame = stackFrames.get(i);
                for (GCRoot javaLocal : stackFrame.locals) {
                    JavaClass localVarClass = javaLocal.getInstance().getJavaClass();
                    if (localVarClass == null) {
                        continue;
                    }
                    if (hasSuperClass(localVarClass, "org.apache.doris.catalog.OlapTable")) {
                        SyncFrame syncFrame = findSyncFrame(stackFrames, i - 1);
                        if (syncFrame == null) {
                            return;
                        }

                        LockMethod lockMethod = findLockMethod(stackFrames, i - 1);
                        if (!lockMethod.isRead && !lockMethod.isWrite) {
                            return;
                        }

                        DbTable dbTable = analyzeTable(javaLocal.getInstance());
                        ThreadBlockReason dependency = threadLockDependencies.computeIfAbsent(
                                threadEntity.tid, tid -> new ThreadBlockReason(threadEntity));
                        dependency.blockingReason = new BlockingReason(
                                syncFrame, dbTable, lockMethod
                        );
                        findOlapTable = true;
                    }
                }
                if (findOlapTable) {
                    break;
                }
            }
        }
    }

    private LockMethod findLockMethod(List<StackFrameEntity> stackFrames, int from) {
        for (int i = from; i >= 0; i--) {
            String info = stackFrames.get(i).toString();
            if (info.contains("java.util.concurrent.locks.ReentrantReadWriteLock$ReadLock.lock(")) {
                return new LockMethod(true, false);
            } else if (info.contains("java.util.concurrent.locks.ReentrantReadWriteLock$WriteLock.lock")) {
                return new LockMethod(false, true);
            }
        }
        return new LockMethod(false, false);
    }

    private SyncFrame findSyncFrame(List<StackFrameEntity> stackFrames, int from) {
        for (int i = from; i >= 0; i--) {
            StackFrameEntity stackFrame = stackFrames.get(i);
            for (GCRoot javaLocal : stackFrame.locals) {
                JavaClass classEntity = javaLocal.getInstance().getJavaClass();
                if (classEntity == null) {
                    continue;
                }
                if (hasSuperClass(classEntity, "java.util.concurrent.locks.ReentrantReadWriteLock$FairSync")
                        || hasSuperClass(classEntity, "java.util.concurrent.locks.ReentrantReadWriteLock$NonfairSync")) {
                    return new SyncFrame(stackFrame, javaLocal.getInstance());
                }
            }
        }
        return null;
    }

    private boolean hasSuperClass(JavaClass javaClass, String className) {
        JavaClass clazz = javaClass;
        while (clazz != null) {
            if (clazz.getName().equals(className)) {
                return true;
            }
            clazz = clazz.getSuperClass();
        }
        return false;
    }

    public enum State {
        NEW,
        RUNNABLE,
        BLOCKED,
        WAITING,
        TIMED_WAITING,
        TERMINATED;
    }

    public static class VM {
        private final static int JVMTI_THREAD_STATE_ALIVE = 0x0001;
        private final static int JVMTI_THREAD_STATE_TERMINATED = 0x0002;
        private final static int JVMTI_THREAD_STATE_RUNNABLE = 0x0004;
        private final static int JVMTI_THREAD_STATE_BLOCKED_ON_MONITOR_ENTER = 0x0400;
        private final static int JVMTI_THREAD_STATE_WAITING_INDEFINITELY = 0x0010;
        private final static int JVMTI_THREAD_STATE_WAITING_WITH_TIMEOUT = 0x0020;

        public static State toThreadState(int threadStatus) {
            if ((threadStatus & JVMTI_THREAD_STATE_RUNNABLE) != 0) {
                return RUNNABLE;
            } else if ((threadStatus & JVMTI_THREAD_STATE_BLOCKED_ON_MONITOR_ENTER) != 0) {
                return BLOCKED;
            } else if ((threadStatus & JVMTI_THREAD_STATE_WAITING_INDEFINITELY) != 0) {
                return WAITING;
            } else if ((threadStatus & JVMTI_THREAD_STATE_WAITING_WITH_TIMEOUT) != 0) {
                return TIMED_WAITING;
            } else if ((threadStatus & JVMTI_THREAD_STATE_TERMINATED) != 0) {
                return TERMINATED;
            } else if ((threadStatus & JVMTI_THREAD_STATE_ALIVE) == 0) {
                return NEW;
            } else {
                return RUNNABLE;
            }
        }
    }

    private static class LockHolder {
        Map<ThreadEntity, Integer> holdReadLockThreads;
        ThreadEntity holdWriteLockThread;

        private LockHolder(Map<ThreadEntity, Integer> holdReadLockThreads, ThreadEntity holdWriteLockThread) {
            this.holdReadLockThreads = holdReadLockThreads;
            this.holdWriteLockThread = holdWriteLockThread;
        }

        @Override
        public String toString() {
            if (holdReadLockThreads == null && holdWriteLockThread == null) {
                return "No Lock";
            } else if (holdWriteLockThread != null) {
                return "Hold write lock: \"" + holdWriteLockThread + "\"";
            } else {
                return "Hold read lock: \"" + holdReadLockThreads + "\"";
            }
        }

        public static LockHolder noLock() {
            return new LockHolder(null, null);
        }

        public static LockHolder holdReadLock(Map<ThreadEntity, Integer> holdReadLockThreads) {
            return new LockHolder(holdReadLockThreads, null);
        }

        public static LockHolder holdWriteLock(ThreadEntity holdWriteLockThread) {
            return new LockHolder(null, holdWriteLockThread);
        }

        public Map<ThreadEntity, Integer> getHoldReadLockThreads() {
            return holdReadLockThreads;
        }

        public boolean isHoldReadLock() {
            return holdReadLockThreads != null && !holdReadLockThreads.isEmpty();
        }

        public boolean isHoldWriteLock() {
            return holdWriteLockThread != null;
        }
    }

    private static class BlockingReason {
        SyncFrame syncFrame;

        // blocking at
        DbTable dbTable;
        LockMethod lockMethod;

        public BlockingReason(SyncFrame syncFrame, DbTable dbTable, LockMethod lockMethod) {
            this.syncFrame = syncFrame;
            this.dbTable = dbTable;
            this.lockMethod = lockMethod;
        }

        @Override
        public String toString() {
            return lockMethod + " lock: " + dbTable;
        }
    }

    private static class TableLockInfo {
        String db;
        String table;

        boolean holdWriteLock;
        int holdReadLockNum;

        public TableLockInfo(String db, String table, boolean holdWriteLock, int holdReadLockNum) {
            this.db = db;
            this.table = table;
            this.holdWriteLock = holdWriteLock;
            this.holdReadLockNum = holdReadLockNum;
        }
    }

    private static class ThreadBlockReason {
        ThreadEntity threadEntity;
        BlockingReason blockingReason;
        List<TableLockInfo> tableLockInfos = new ArrayList<>();

        public ThreadBlockReason(ThreadEntity threadEntity) {
            this.threadEntity = threadEntity;
        }
    }

    private static class DbTable {
        String db;
        String table;

        public DbTable(String db, String table) {
            this.db = db;
            this.table = table;
        }
        @Override
        public String toString() {
            return db + "." + table;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DbTable dbTable = (DbTable) o;
            return Objects.equals(db, dbTable.db) && Objects.equals(table, dbTable.table);
        }

        @Override
        public int hashCode() {
            return Objects.hash(db, table);
        }
    }

    private static class SyncFrame {
        StackFrameEntity frame;
        Instance sync;

        public SyncFrame(StackFrameEntity frame, Instance sync) {
            this.frame = frame;
            this.sync = sync;
        }
    }

    private static class LockMethod {
        boolean isRead;
        boolean isWrite;

        public LockMethod(boolean isRead, boolean isWrite) {
            this.isRead = isRead;
            this.isWrite = isWrite;
        }

        @Override
        public String toString() {
            if (isWrite) {
                return "write";
            } else if (isRead) {
                return "read";
            } else {
                return "no lock";
            }
        }
    }

    private static class TableLockDependencies {
        Map<DbTable, Instance> dbTableToSyncObj = Maps.newLinkedHashMap();
        Map<Instance, DbTable> syncObjToDbTable = Maps.newLinkedHashMap();

        public void bind(DbTable dbTable, Instance sync) {
            dbTableToSyncObj.put(dbTable, sync);
            syncObjToDbTable.put(sync, dbTable);
        }
    }

    private class HoldTableLocks {
        Set<DbTable> readTables = Sets.newLinkedHashSet();
        Set<DbTable> writeTables = Sets.newLinkedHashSet();
    }

    private static class ThreadSync {
        boolean isRead;
        ThreadEntity threadEntity;
        Instance sync;

        public ThreadSync(boolean isRead, ThreadEntity threadEntity, Instance sync) {
            this.isRead = isRead;
            this.threadEntity = threadEntity;
            this.sync = sync;
        }
    }

    private static class ThreadDepends {
        ThreadSync threadSync;
        DbTable dbTable;
        Map<Long, ThreadDepends> depends = Maps.newLinkedHashMap();

        public ThreadDepends(ThreadSync threadSync, DbTable dbTable) {
            this.threadSync = threadSync;
            this.dbTable = dbTable;
        }
    }

    public static class DeadLockDepends extends ThreadDepends {
        public DeadLockDepends(ThreadSync threadSync, DbTable dbTable) {
            super(threadSync, dbTable);
        }
    }

    public static class RunningThreadDepends extends ThreadDepends {
        public RunningThreadDepends(ThreadSync threadSync) {
            super(threadSync, null);
        }
    }

    public static String printId(long hi, long lo) {
        StringBuilder builder = new StringBuilder();
        builder.append(Long.toHexString(hi)).append("-").append(Long.toHexString(lo));
        return builder.toString();
    }

    public static class ContextId {
        public String queryId;
        public String loadId;
        public String defaultCatalog;
        public String currentDb;
        public String sql;
        public int sqlIdx;

        public ContextId(String queryId, String loadId, String defaultCatalog, String currentDb, String sql, int sqlIdx) {
            this.queryId = queryId;
            this.loadId = loadId;
            this.defaultCatalog = defaultCatalog;
            this.currentDb = currentDb;
            this.sql = sql;
            this.sqlIdx = sqlIdx;
        }
    }

    private static class ThreadEntity {
        String threadName;
        boolean daemon;
        int priority;
        long tid;
        int threadStatus;
        ThreadObjectGCRoot threadGcRoot;
        List<StackFrameEntity> stackFrames;

        public ThreadEntity(String threadName, boolean daemon, int priority, long tid, int threadStatus,
                ThreadObjectGCRoot threadObj, List<StackFrameEntity> stackFrames) {
            this.threadName = threadName;
            this.daemon = daemon;
            this.priority = priority;
            this.tid = tid;
            this.threadStatus = threadStatus;
            this.threadGcRoot = threadObj;
            this.stackFrames = stackFrames;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ThreadEntity that = (ThreadEntity) o;
            return tid == that.tid;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(tid);
        }

        @Override
        public String toString() {
            String threadStatus = VM.toThreadState(this.threadStatus).toString();
            return "\"" + (threadName == null ? "" : threadName) + "\" " + (daemon ? "daemon " : "") + "prio=" + priority + " tid=" + tid  + " " + threadStatus;
        }
    }

    private static class StackFrameEntity {
        StackTraceElement stackTraceElement;
        List<GCRoot> locals;

        public StackFrameEntity(StackTraceElement stackTraceElement, List<GCRoot> locals) {
            this.stackTraceElement = stackTraceElement;
            this.locals = locals;
        }

        @Override
        public String toString() {
            return stackTraceElement.toString();
        }
    }
}
