package org.apache.doris.fe.dump;

import org.apache.doris.fe.dump.util.DumpUtils;
import org.apache.doris.fe.dump.util.ScriptUtils;
import org.apache.doris.fe.dump.util.TopologySort;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import static org.apache.doris.fe.dump.AnalyzeDorisFeHprof.State.BLOCKED;
import static org.apache.doris.fe.dump.AnalyzeDorisFeHprof.State.NEW;
import static org.apache.doris.fe.dump.AnalyzeDorisFeHprof.State.RUNNABLE;
import static org.apache.doris.fe.dump.AnalyzeDorisFeHprof.State.TERMINATED;
import static org.apache.doris.fe.dump.AnalyzeDorisFeHprof.State.TIMED_WAITING;
import static org.apache.doris.fe.dump.AnalyzeDorisFeHprof.State.WAITING;
import org.graalvm.visualvm.lib.jfluid.heap.ArrayItemValue;
import org.graalvm.visualvm.lib.jfluid.heap.Field;
import org.graalvm.visualvm.lib.jfluid.heap.FieldValue;
import org.graalvm.visualvm.lib.jfluid.heap.GCRoot;
import static org.graalvm.visualvm.lib.jfluid.heap.GCRoot.JNI_LOCAL;
import org.graalvm.visualvm.lib.jfluid.heap.Heap;
import org.graalvm.visualvm.lib.jfluid.heap.HeapFactory;
import org.graalvm.visualvm.lib.jfluid.heap.HeapSummary;
import org.graalvm.visualvm.lib.jfluid.heap.HprofObjectUtils;
import org.graalvm.visualvm.lib.jfluid.heap.Instance;
import org.graalvm.visualvm.lib.jfluid.heap.JavaClass;
import org.graalvm.visualvm.lib.jfluid.heap.JavaFrameGCRoot;
import org.graalvm.visualvm.lib.jfluid.heap.JniLocalGCRoot;
import org.graalvm.visualvm.lib.jfluid.heap.ObjectArrayInstance;
import org.graalvm.visualvm.lib.jfluid.heap.ObjectFieldValue;
import org.graalvm.visualvm.lib.jfluid.heap.StringInstanceUtils;
import org.graalvm.visualvm.lib.jfluid.heap.ThreadObjectGCRoot;
import org.graalvm.visualvm.lib.profiler.oql.engine.api.OQLEngine;
import org.graalvm.visualvm.lib.profiler.oql.engine.api.OQLEngine.ObjectVisitor;
import org.graalvm.visualvm.lib.profiler.oql.engine.api.OQLException;

import java.io.File;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLStreamHandlerFactory;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class AnalyzeDorisFeHprof {
    public static final Option methodOpt = Option.builder("m")
            .required(false)
            .hasArg(true)
            .longOpt("method")
            .desc("select which method to invoke")
            .build();
    public static final Option executeOpt = Option.builder("o")
            .required(false)
            .hasArg(true)
            .longOpt("oql")
            .desc("the oql string")
            .build();
    public static final Option limitOpt = Option.builder("l")
            .required(false)
            .hasArg(true)
            .longOpt("limit")
            .desc("the limit oql query rows")
            .build();

    public static final Options options = new Options()
            .addOption(methodOpt)
            .addOption(executeOpt)
            .addOption(limitOpt);

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

    public static void main(String[] args) throws Throwable {
        if (args.length == 0) {
            System.out.println("Usage: java -jar DorisFeDumpProfiler-*.jar fe.hprof");
            System.exit(1);
        }
        Heap heap = HeapFactory.createHeap(new File(args[0]));
        AnalyzeDorisFeHprof analyzeDorisFeHprof = new AnalyzeDorisFeHprof(heap);
        analyzeDorisFeHprof.basicAnalyze();

        CommandLine cmdLine = new DefaultParser().parse(options, args, false);
        String method = cmdLine.getParsedOptionValue(methodOpt);
        if ("summary".equals(method)) {
            analyzeDorisFeHprof.analyzeSummary();
        } else if ("execute".equals(method)) {
            String oql = cmdLine.getParsedOptionValue(executeOpt);
            if (oql == null) {
                throw new IllegalArgumentException("Missing oql argument");
            }
            Integer limit = Integer.valueOf(cmdLine.getParsedOptionValue(limitOpt, "-1"));

            ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(new CustomClassLoader(new URL[]{}, contextClassLoader));
            analyzeDorisFeHprof.executeOql(oql, limit);
        } else {
            analyzeDorisFeHprof.defaultAnalyze();
        }
    }

    private void executeOql(String query, Integer limit) throws OQLException {
        OQLEngine oqlEngine = new OQLEngine(heap);
        ScriptUtils.registerFunctions(oqlEngine);

        System.out.println("===== OQL Result =====");
        AtomicLong rowCount = new AtomicLong();

        oqlEngine.executeQuery(query, new ObjectVisitor() {
            @Override
            public boolean visit(Object o) {
                System.out.println("=== Row " + rowCount.incrementAndGet() + " ===");
                try {
                    System.out.println(toJsonString(o));
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
                return limit > 0 && rowCount.get() >= limit;
            }
        });
    }

    public void basicAnalyze() {
        threadStackFrameLocals = computeJavaFrameMap(heap.getGCRoots());
        threadObjIdToThread = getThreads(heap);
        threadObjIdToContextId = analyzeContextId();
    }

    public void defaultAnalyze() {
        blockThreads = analyzeBlockingThreads();
        tableLockDependencies = bindTableLockToSyncByBlockReason(blockThreads);

        // syncObj -> LockHolder
        Map<Long, LockHolder> lockHolders = analyzeHoldTableLockThreads(tableLockDependencies);

        Map<Long, List<ThreadSync>> syncToWaitingThreads = analyzeAndPrintTableLockQueue(
                lockHolders, tableLockDependencies);
        System.out.println();

        printThreadsWithLock(blockThreads, lockHolders, tableLockDependencies, syncToWaitingThreads);
        System.out.println();

        printQueries();
        System.out.println();
    }

    public void analyzeSummary() {
        printSummary();

        System.out.println();
        printAllThreads();

        System.out.println();
        printBiggestClassObjects();
    }

    private void printBiggestClassObjects() {
        System.out.println("===== Class =====");

        PriorityQueue<JavaClass> queue = new PriorityQueue<>(
                Comparator.comparing(JavaClass::getAllInstancesSize).reversed()
        );
        queue.addAll(heap.getAllClasses());

        while (!queue.isEmpty()) {
            JavaClass javaClass = queue.poll();
            String name = javaClass.getName();
            int instancesCount = javaClass.getInstancesCount();
            long allInstancesSize = javaClass.getAllInstancesSize();
            // not print < 1M
            if (allInstancesSize < 1024L * 1024) {
                break;
            }
            System.out.println(name + ": " + "instancesCount: " + instancesCount + ", allInstancesSize: "
                    + allInstancesSize + (allInstancesSize > 0 ? "(" + formatBytes(allInstancesSize) + ")": ""));
            // long retainedSizeByClass = javaClass.getRetainedSizeByClass();
            // System.out.println(name + ": " + "instancesCount: " + instancesCount + ", allInstancesSize: "
            //         + allInstancesSize + (allInstancesSize > 0 ? "(" + formatBytes(allInstancesSize) + ")": "")
            //         + ", retainedSizeByClass: " + retainedSizeByClass + (retainedSizeByClass > 0 ? "(" + formatBytes(retainedSizeByClass) + ")" : ""));
        }
    }

    private void printSummary() {
        HeapSummary summary = heap.getSummary();

        System.out.println("===== Summary =====");
        System.out.println("Heap dump time: " + formatDate(summary.getTime()));
        long totalLiveBytes = summary.getTotalLiveBytes();
        System.out.println("totalLiveBytes: " + totalLiveBytes + (totalLiveBytes > 0 ? "(" + formatBytes(totalLiveBytes) + ")" : ""));

        long totalLiveInstances = summary.getTotalLiveInstances();
        System.out.println("totalLiveInstances: " + totalLiveInstances);

        long totalAllocatedBytes = summary.getTotalAllocatedBytes();
        System.out.println("totalAllocatedBytes: " + totalAllocatedBytes + (totalAllocatedBytes > 0 ? "(" + formatBytes(totalAllocatedBytes) + ")" : ""));

        long totalAllocatedInstances = summary.getTotalAllocatedInstances();
        System.out.println("totalAllocatedInstances: " + totalAllocatedInstances);

        ThreadObjectGCRoot oomeThread = DumpUtils.getOOMEThread(heap);
        if (oomeThread != null) {
            try {
                System.out.println("OutOfMemoryErrorThread: " + threadObjIdToThread.get(oomeThread.getInstance().getInstanceId()).threadName);
            } catch (Throwable t) {

            }
        }
    }

    private void printAllThreads() {
        System.out.println("===== Threads =====");
        for (ThreadEntity threadEntity : threadObjIdToThread.values()) {
            printThreadWithContextId(threadEntity);
            printStackFrames(threadEntity);
            System.out.println();
        }
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
        if (frameToLocals == null) {
            return new ArrayList<>();
        }

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
        Map<ThreadObjectGCRoot, Map<Integer,List<GCRoot>>> javaFrameMap = new HashMap<>();
        for (GCRoot root : roots) {
            ThreadObjectGCRoot threadObj;
            int frameNo;

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
            if (stackMap == null) {
                stackMap = new HashMap<>();
                javaFrameMap.put(threadObj, stackMap);
            }
            List<GCRoot> locals = stackMap.get(frameNo);
            if (locals == null) {
                locals = new ArrayList<>(2);
                stackMap.put(frameNo, locals);
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

    private Map<Long, List<ThreadSync>> analyzeAndPrintTableLockQueue(
            Map<Long, LockHolder> lockHolders, TableLockDependencies tableLockDependencies) {
        System.out.println("===== Locks =====");
        Map<Long, List<ThreadSync>> analyzeWaitThreads = new LinkedHashMap<>();
        for (Entry<DbTable, Instance> kv : tableLockDependencies.dbTableToSyncObj.entrySet()) {
            DbTable dbTable = kv.getKey();
            Instance sync = kv.getValue();

            String dbOrTable = (dbTable.table == null ? "db" : "table");
            System.out.println(dbTable + " " + dbOrTable + " lock:");
            LockHolder lockHolder = lockHolders.get(sync.getInstanceId());
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

    private void analyzeAndPrintSyncQueue(
            Map<Long, ThreadEntity> threadObjIdToThread, Instance sync, Map<Long, List<ThreadSync>> analyzeWaitThreads) {
        List<ThreadSync> threadSyncs = Lists.newArrayList();
        analyzeWaitThreads.put(sync.getInstanceId(), threadSyncs);

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
            Map<Long, ThreadBlockReason> blockThreads, Map<Long, LockHolder> lockHolders, TableLockDependencies tableLockDependencies,
            Map<Long, List<ThreadSync>> syncToWaitingThreads) {

        System.out.println("===== Threads =====");
        for (ThreadBlockReason threadBlockReason : blockThreads.values()) {
            printThreadWithLock(threadBlockReason.threadEntity, threadBlockReason, lockHolders, tableLockDependencies);
            ThreadDepends depends = analyzeBlockTree(threadBlockReason, lockHolders, syncToWaitingThreads, tableLockDependencies, blockThreads);

            System.out.println("    block depends tree:");
            printThreadDependsTree(depends, 1);

            System.out.println("    events topology order:");
            printEventTimeline(depends);

            printStackFrames(threadBlockReason.threadEntity, true);
            System.out.println();
        }

        Set<ThreadEntity> nonBlockingThreads = collectNonBlockingThreads(blockThreads, lockHolders, tableLockDependencies);
        for (ThreadEntity nonBlockingThread : nonBlockingThreads) {
            printThreadWithLock(nonBlockingThread, null, lockHolders, tableLockDependencies);
            printStackFrames(nonBlockingThread, true);
            System.out.println();
        }
    }

    private void printEventTimeline(ThreadDepends root) {
        TopologySort<String> topologySort = new TopologySort<>();
        addEvent(topologySort, null, root);

        List<String> sort = topologySort.sort();
        for (String event : sort) {
            System.out.println("       " + event);
        }
        System.out.println();
    }

    private void addEvent(TopologySort<String> topologySort, ThreadDepends preDepends, ThreadDepends currentDepends) {
        if (preDepends != null) {
            if (currentDepends.holdSync != null) {
                topologySort.addPartialOrder(
                        currentDepends.formatThreadName() + " " + currentDepends.formatHoldSync(),
                        preDepends.formatThreadName() + " " + preDepends.formatBlockedSync()
                );
            } else {
                topologySort.addPartialOrder(
                        currentDepends.formatThreadName() + " " + currentDepends.formatBlockedSync(),
                        preDepends.formatThreadName() + " " + preDepends.formatBlockedSync()
                );
            }
        }

        if (currentDepends.holdSync != null && currentDepends.blockedSync != null) {
            String threadName = currentDepends.formatThreadName();
            topologySort.addPartialOrder(
                    threadName + " " + currentDepends.formatHoldSync(),
                    threadName + " " + currentDepends.formatBlockedSync()
            );
        }

        for (ThreadDepends nextDepends : currentDepends.depends.values()) {
            addEvent(topologySort, currentDepends, nextDepends);
        }
    }

    private void printStackFrames(ThreadEntity threadEntity) {
        printStackFrames(threadEntity, false);
    }

    private void printStackFrames(ThreadEntity threadEntity, boolean printEmptyLinePrefix) {
        List<StackFrameEntity> stackFrames = threadEntity.stackFrames;
        if (printEmptyLinePrefix && stackFrames != null && !stackFrames.isEmpty()) {
            System.out.println();
        }
        for (StackFrameEntity stackFrame : stackFrames) {
            System.out.println("    " + stackFrame);
        }
    }

    private void printThreadDependsTree(ThreadDepends depends, int level) {
        String indent = "    " + Strings.repeat("   ", level);
        String lockInfo;
        if (depends instanceof RunningThreadDepends) {
            lockInfo = depends + "\" running (Live lock)";
        } else if (depends instanceof DeadLockDepends) {
            lockInfo = depends + " (Dead lock)";
        } else {
            lockInfo = depends.toString();
        }
        System.out.println(indent + lockInfo);
        for (ThreadDepends nextDepends : depends.depends.values()) {
            printThreadDependsTree(nextDepends, level + 1);
        }
    }

    private ThreadDepends analyzeBlockTree(
            ThreadBlockReason threadBlockReason,
            Map<Long, LockHolder> lockHolders,
            Map<Long, List<ThreadSync>> syncToWaitingThreads,
            TableLockDependencies tableLockDependencies, Map<Long, ThreadBlockReason> blockThreads) {
        Map<Long, ThreadDepends> visitedThreadIds = Maps.newLinkedHashMap();
        Instance blockedSync = threadBlockReason.blockingReason.syncFrame.sync;
        ThreadDepends blockedDepends = new ThreadDepends(
                null, null,
                new ThreadSync(threadBlockReason.blockingReason.lockMethod.isRead, threadBlockReason.threadEntity, blockedSync),
                threadBlockReason.blockingReason.dbTable
        );
        visitedThreadIds.put(threadBlockReason.threadEntity.tid, blockedDepends);
        doAnalyzeBlockTree(visitedThreadIds, lockHolders, blockedDepends, blockedSync, syncToWaitingThreads, tableLockDependencies, blockThreads);
        return blockedDepends;
    }

    private void doAnalyzeBlockTree(Map<Long, ThreadDepends> visitedThreads,
            Map<Long, LockHolder> lockHolders, ThreadDepends depends, Instance blockedSync,
            Map<Long, List<ThreadSync>> syncToWaitingThreads,
            TableLockDependencies tableLockDependencies, Map<Long, ThreadBlockReason> blockThreads) {
        long syncId = blockedSync.getInstanceId();
        if (depends.blockedSync.isRead) {
            LockHolder lockHolder = lockHolders.get(syncId);
            if (lockHolder != null && lockHolder.holdWriteLockThread != null) {
                analyzeNextDepends(
                        visitedThreads, lockHolders, depends, syncToWaitingThreads, tableLockDependencies,
                        blockThreads, blockedSync, lockHolder.holdWriteLockThread, false, true
                );
            } else if (lockHolder != null) {
                List<ThreadSync> threadSyncs = syncToWaitingThreads.get(syncId);
                if (threadSyncs != null) {
                    for (ThreadSync threadSync : threadSyncs) {
                        if (!threadSync.isRead) {
                            analyzeNextDepends(visitedThreads, lockHolders, depends,
                                    syncToWaitingThreads, tableLockDependencies,
                                    blockThreads, blockedSync, threadSync.threadEntity, false, false);
                            break;
                        } else if (threadSync.threadEntity.tid == depends.holdSync.threadEntity.tid) {
                            break;
                        }
                    }
                }
            }
        } else {
            LockHolder lockHolder = lockHolders.get(syncId);
            if (lockHolder != null && lockHolder.getHoldReadLockThreads() != null) {
                for (ThreadEntity readThread : lockHolder.getHoldReadLockThreads().keySet()) {
                    analyzeNextDepends(visitedThreads, lockHolders, depends, syncToWaitingThreads,
                            tableLockDependencies,
                            blockThreads, blockedSync, readThread, true, true);
                }
            } else if (lockHolder != null && lockHolder.holdWriteLockThread != null
                    && lockHolder.holdWriteLockThread.tid != depends.blockedSync.threadEntity.tid) {
                analyzeNextDepends(visitedThreads, lockHolders, depends, syncToWaitingThreads,
                        tableLockDependencies,
                        blockThreads, blockedSync, lockHolder.holdWriteLockThread, false, true);
            } else {
                List<ThreadSync> threadSyncs = syncToWaitingThreads.get(syncId);
                if (threadSyncs != null) {
                    for (ThreadSync threadSync : threadSyncs) {
                        if (depends.holdSync != null && threadSync.threadEntity.tid == depends.holdSync.threadEntity.tid) {
                            break;
                        } else {
                            analyzeNextDepends(visitedThreads, lockHolders, depends, syncToWaitingThreads, tableLockDependencies,
                                    blockThreads, blockedSync, threadSync.threadEntity, threadSync.isRead, false);
                            break;
                        }
                    }
                }
            }
        }
    }

    private void analyzeNextDepends(Map<Long, ThreadDepends> visitedThreads,
            Map<Long, LockHolder> lockHolders, ThreadDepends depends,
            Map<Long, List<ThreadSync>> syncToWaitingThreads,
            TableLockDependencies tableLockDependencies,
            Map<Long, ThreadBlockReason> blockThreads, Instance holdSyncInstance, ThreadEntity nextThread,
            boolean isRead, boolean isHold) {
        ThreadDepends existDepends = visitedThreads.get(nextThread.tid);
        ThreadSync nextSync = new ThreadSync(isRead, nextThread, holdSyncInstance);
        if (existDepends != null) {
            DeadLockDepends deadLockDepends;
            if (isHold) {
                deadLockDepends = new DeadLockDepends(nextSync, depends.blockedSyncDbTable, existDepends.blockedSync, existDepends.blockedSyncDbTable);
            } else {
                deadLockDepends = new DeadLockDepends(null, null, nextSync, depends.blockedSyncDbTable);
            }
            depends.depends.put(nextThread.tid, deadLockDepends);
            return;
        }

        ThreadBlockReason threadBlockReason = blockThreads.get(nextThread.tid);
        if (threadBlockReason == null) {
            depends.depends.put(nextThread.tid, new RunningThreadDepends(nextSync, depends.blockedSyncDbTable));
            return;
        }

        ThreadSync blockedSync = new ThreadSync(
                threadBlockReason.blockingReason.lockMethod.isRead,
                threadBlockReason.threadEntity,
                threadBlockReason.blockingReason.syncFrame.sync
        );
        if (isHold) {
            existDepends = new ThreadDepends(nextSync, depends.blockedSyncDbTable, blockedSync, threadBlockReason.blockingReason.dbTable);
        } else {
            existDepends = new ThreadDepends(null, null, blockedSync, threadBlockReason.blockingReason.dbTable);
        }
        depends.depends.put(nextThread.tid, existDepends);
        visitedThreads.put(nextThread.tid, existDepends);

        doAnalyzeBlockTree(
                visitedThreads, lockHolders, existDepends, threadBlockReason.blockingReason.syncFrame.sync,
                syncToWaitingThreads, tableLockDependencies, blockThreads
        );
    }

    private void printThreadWithLock(
            ThreadEntity threadEntity, ThreadBlockReason threadBlockReason, Map<Long, LockHolder> lockHolders, TableLockDependencies tableLockDependencies) {
        printThreadWithContextId(threadEntity);
        if (threadBlockReason != null) {
            System.out.println("    blocked " + threadBlockReason.blockingReason);
        }
        HoldTableLocks holdTableLock = findHoldTableLock(threadEntity, lockHolders, tableLockDependencies);
        if (!holdTableLock.writeTables.isEmpty()) {
            for (DbTable writeTable : holdTableLock.writeTables) {
                String dbOrTable = writeTable.table == null ? "db" : "table";
                System.out.println("    hold write " + dbOrTable + " lock: \"" + writeTable + "\"");
            }
        }
        if (!holdTableLock.readTables.isEmpty()) {
            for (DbTable readTable : holdTableLock.readTables) {
                String dbOrTable = readTable.table == null ? "db" : "table";
                System.out.println("    hold read " + dbOrTable + " lock:  \"" + readTable + "\"");
            }
        }
    }

    private Set<ThreadEntity> collectNonBlockingThreads(
            Map<Long, ThreadBlockReason> blockThreads, Map<Long, LockHolder> lockHolders,
            TableLockDependencies tableLockDependencies) {
        Set<ThreadEntity> nonBlockingThreads = Sets.newLinkedHashSet();
        for (Entry<Long, LockHolder> kv : lockHolders.entrySet()) {
            Long syncId = kv.getKey();
            DbTable dbTable = tableLockDependencies.syncObjToDbTable.get(syncId);
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
            Map<Long, LockHolder> lockHolders,
            TableLockDependencies tableLockDependencies) {
        HoldTableLocks holdTableLocks = new HoldTableLocks();
        for (Entry<Long, LockHolder> kv : lockHolders.entrySet()) {
            Long syncId = kv.getKey();
            LockHolder holder = kv.getValue();
            DbTable relateTable = tableLockDependencies.syncObjToDbTable.get(syncId);
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

    private Map<Long, LockHolder> analyzeHoldTableLockThreads(TableLockDependencies tableLockDependencies) {
        Map<Long, LockHolder> lockHolders = Maps.newLinkedHashMap();
        for (ThreadEntity threadEntity : threadObjIdToThread.values()) {
            for (StackFrameEntity stackFrame : threadEntity.stackFrames) {
                for (GCRoot javaLocal : stackFrame.locals) {
                    JavaClass classEntity = javaLocal.getInstance().getJavaClass();
                    if (classEntity == null) {
                        continue;
                    }
                    if (hasSuperClass(classEntity, "org.apache.doris.catalog.MTMV")) {
                        analyzeHoldMtmvLock(threadEntity, javaLocal.getInstance(), lockHolders, tableLockDependencies);
                    } else if (hasSuperClass(classEntity, "org.apache.doris.catalog.OlapTable")) {
                        analyzeHoldTableLock(threadEntity, javaLocal.getInstance(), lockHolders, tableLockDependencies);
                    } else if (hasSuperClass(classEntity, "org.apache.doris.nereids.CascadesContext$Lock")) {
                        analyzeHoldTableLockInCascadesContext(threadEntity, javaLocal.getInstance(), lockHolders, tableLockDependencies);
                    } else if (hasSuperClass(classEntity, "org.apache.doris.catalog.Database")) {
                        analyzeHoldDbLock(threadEntity, javaLocal.getInstance(), lockHolders, tableLockDependencies);
                    }
                }
            }
        }
        return lockHolders;
    }

    private void analyzeHoldMtmvLock(ThreadEntity threadEntity, Instance mtmv, Map<Long, LockHolder> lockHolders,
            TableLockDependencies tableLockDependencies) {
        DbTable dbTable = analyzeTable(mtmv);
        Instance mvRwLock = (Instance) mtmv.getValueOfField("mvRwLock");
        Instance mvRewriteSync = (Instance) mvRwLock.getValueOfField("sync");
        tableLockDependencies.bind(new DbTable(dbTable.db, dbTable.table + "@mvRwLock"), mvRewriteSync);
        analyzeReentrantReadWriteLockThreads(threadEntity, mvRewriteSync, lockHolders);

        Instance rwLock = (Instance) mtmv.getValueOfField("rwLock");
        Instance sync = (Instance) rwLock.getValueOfField("sync");
        tableLockDependencies.bind(dbTable, sync);
        analyzeReentrantReadWriteLockThreads(threadEntity, sync, lockHolders);
    }

    private LockHolder analyzeReentrantReadWriteLockThreads(
            ThreadEntity currentThread, Instance sync, Map<Long, LockHolder> lockHolders) {
        long syncId = sync.getInstanceId();
        LockHolder lockHolder = lockHolders.get(syncId);

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
            lockHolders.put(syncId, lockHolder);
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
            lockHolders.put(syncId, lockHolder);
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
            ThreadEntity threadEntity, Instance cascadesTableLock,
            Map<Long, LockHolder> lockHolders, TableLockDependencies tableLockDependencies) {
        Instance lockStack = (Instance) cascadesTableLock.getValueOfField("locked");
        ObjectArrayInstance stackArray = (ObjectArrayInstance) lockStack.getValueOfField("elementData");
        Integer elementCount = (Integer) lockStack.getValueOfField("elementCount");
        for (int i = 0; i < elementCount; i++) {
            Instance lockItem = stackArray.getItems().get(i).getInstance();
            JavaClass lockClass = lockItem.getJavaClass();
            if (!hasSuperClass(lockClass, "org.apache.doris.catalog.Table")) {
                continue;
            }
            analyzeHoldTableLock(threadEntity, lockItem, lockHolders, tableLockDependencies);
        }
    }

    private DbTable analyzeTable(Instance table) {
        String tableName = StringInstanceUtils.getDetailsString(table.getValueOfField("name"));
        String dbName = StringInstanceUtils.getDetailsString(table.getValueOfField("qualifiedDbName"));
        return new DbTable(dbName, tableName);
    }

    private DbTable analyzeMTMV(Instance mtmv, Instance sync) {
        String tableName = StringInstanceUtils.getDetailsString(mtmv.getValueOfField("name"));
        String dbName = StringInstanceUtils.getDetailsString(mtmv.getValueOfField("qualifiedDbName"));
        Instance mvRewriteSync = (Instance) ((Instance) mtmv.getValueOfField("mvRwLock")).getValueOfField("sync");
        if (mvRewriteSync.getInstanceId() == sync.getInstanceId()) {
            return new DbTable(dbName, tableName + "@mvRwLock");
        }
        return new DbTable(dbName, tableName);
    }

    private DbTable analyzeDb(Instance db) {
        String dbName = StringInstanceUtils.getDetailsString(db.getValueOfField("fullQualifiedName"));
        return new DbTable(dbName, null);
    }

    private void analyzeHoldTableLock(
            ThreadEntity threadEntity, Instance table, Map<Long, LockHolder> lockHolders, TableLockDependencies tableLockDependencies) {
        DbTable dbTable = analyzeTable(table);
        Instance rwLock = (Instance) table.getValueOfField("rwLock");
        Instance sync = (Instance) rwLock.getValueOfField("sync");
        tableLockDependencies.bind(dbTable, sync);
        analyzeReentrantReadWriteLockThreads(threadEntity, sync, lockHolders);
    }

    private void analyzeHoldDbLock(
            ThreadEntity threadEntity, Instance db, Map<Long, LockHolder> lockHolders, TableLockDependencies tableLockDependencies) {
        DbTable dbTable = analyzeDb(db);
        Instance rwLock = (Instance) db.getValueOfField("rwLock");
        Instance sync = (Instance) rwLock.getValueOfField("sync");
        tableLockDependencies.bind(dbTable, sync);
        analyzeReentrantReadWriteLockThreads(threadEntity, sync, lockHolders);
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
            boolean findLock = false;
            for (int i = 0; i < stackFrames.size(); i++) {
                StackFrameEntity stackFrame = stackFrames.get(i);
                for (GCRoot javaLocal : stackFrame.locals) {
                    JavaClass localVarClass = javaLocal.getInstance().getJavaClass();
                    if (localVarClass == null) {
                        continue;
                    }

                    boolean isTable = hasSuperClass(localVarClass, "org.apache.doris.catalog.Table");
                    boolean isDb = hasSuperClass(localVarClass, "org.apache.doris.catalog.Database");
                    if (isTable || isDb) {
                        SyncFrame syncFrame = findSyncFrame(stackFrames, i - 1);
                        if (syncFrame == null) {
                            return;
                        }

                        LockMethod lockMethod = findLockMethod(stackFrames, i - 1);
                        if (!lockMethod.isRead && !lockMethod.isWrite) {
                            return;
                        }

                        DbTable dbTable;
                        if (isTable) {
                            if (hasSuperClass(localVarClass, "org.apache.doris.catalog.MTMV")) {
                                dbTable = analyzeMTMV(javaLocal.getInstance(), syncFrame.sync);
                            } else {
                                dbTable = analyzeTable(javaLocal.getInstance());
                            }
                        } else {
                            dbTable = analyzeDb(javaLocal.getInstance());
                        }
                        ThreadBlockReason dependency = threadLockDependencies.computeIfAbsent(
                                threadEntity.tid, tid -> new ThreadBlockReason(threadEntity));
                        dependency.blockingReason = new BlockingReason(
                                syncFrame, dbTable, lockMethod
                        );
                        findLock = true;
                        break;
                    }
                }
                if (findLock) {
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
            } else if (info.contains("java.util.concurrent.locks.ReentrantReadWriteLock$ReadLock.tryLock(")) {
                return new LockMethod(true, false);
            } else if (info.contains("java.util.concurrent.locks.ReentrantReadWriteLock$WriteLock.lock")) {
                return new LockMethod(false, true);
            } else if (info.contains("java.util.concurrent.locks.ReentrantReadWriteLock$WriteLock.tryLock")) {
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

    public static boolean hasSuperClass(JavaClass javaClass, String className) {
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
            String dbOrTable = dbTable.table == null ? "db" : "table";
            return lockMethod + " " + dbOrTable + " lock: \"" + dbTable + "\"";
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

        public ThreadBlockReason(ThreadEntity threadEntity) {
            this.threadEntity = threadEntity;
        }

        @Override
        public String toString() {
            String readOrWrite = blockingReason.lockMethod.isRead ? "read" : "write";
            String dbOrTable = blockingReason.dbTable.table == null ? "db" : "table";
            return threadEntity.threadName + " blocked at " + readOrWrite + " \"" + blockingReason.dbTable + "\" " + dbOrTable + " lock";
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
            if (table == null) {
                return db;
            }
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
        Map<Long, DbTable> syncObjToDbTable = Maps.newLinkedHashMap();

        public void bind(DbTable dbTable, Instance sync) {
            dbTableToSyncObj.put(dbTable, sync);
            syncObjToDbTable.put(sync.getInstanceId(), dbTable);
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

        @Override
        public String toString() {
            return threadEntity.threadName + (isRead ? " read" : " write");
        }
    }

    private static class ThreadDepends {
        ThreadSync holdSync;
        DbTable holdSyncDbTable;
        ThreadSync blockedSync;
        DbTable blockedSyncDbTable;
        Map<Long, ThreadDepends> depends = Maps.newLinkedHashMap();

        public ThreadDepends(ThreadSync holdSync, DbTable holdSyncDbTable, ThreadSync blockedSync, DbTable blockedSyncDbTable) {
            this.holdSync = holdSync;
            this.holdSyncDbTable = holdSyncDbTable;
            this.blockedSync = blockedSync;
            this.blockedSyncDbTable = blockedSyncDbTable;
        }

        public String formatThreadName() {
            String str = holdSync != null
                    ? holdSync.threadEntity.threadName
                    : blockedSync.threadEntity.threadName;
            str = '"' + str + '"';
            return str;
        }

        public String formatHoldSync() {
            String dbOrTable = holdSyncDbTable.table == null ? "db" :"table";
            String readOrWrite = holdSync.isRead ? "read" : "write";
            return "hold " + readOrWrite + " \"" + holdSyncDbTable + "\" " + dbOrTable + " lock";
        }

        public String formatBlockedSync() {
            String dbOrTable = blockedSyncDbTable.table == null ? "db" : "table";
            String readOrWrite = blockedSync.isRead ? "read" : "write";
            return "blocked at " + readOrWrite + " \"" + blockedSyncDbTable + "\" " + dbOrTable + " lock";
        }

        @Override
        public String toString() {
            String str = formatThreadName();
            if (holdSync != null) {
                str += " " + formatHoldSync();
            }
            if (blockedSync != null) {
                if (holdSync != null) {
                    str += ",";
                }
                str += " ";

                str += formatBlockedSync();
            }
            return str;
        }
    }

    public static class DeadLockDepends extends ThreadDepends {
        public DeadLockDepends(ThreadSync holdSync, DbTable holdSyncDbTable, ThreadSync blockedSync, DbTable blockedSyncDbTable) {
            super(holdSync, holdSyncDbTable, blockedSync, blockedSyncDbTable);
        }
    }

    public static class RunningThreadDepends extends ThreadDepends {
        public RunningThreadDepends(ThreadSync holdSync, DbTable holdSyncDbTable) {
            super(holdSync, holdSyncDbTable, null, null);
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

    public static String toJsonString(Object obj) throws JsonProcessingException {
        return toJsonString(obj, 4);
    }

    public static String toJsonString(Object obj, int maxLevel) throws JsonProcessingException {
        Object javaObjectOrMap = toJavaObject(obj, maxLevel, 0);
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.disable(SerializationFeature.FAIL_ON_SELF_REFERENCES);
        objectMapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        objectMapper.enable(SerializationFeature.WRITE_SELF_REFERENCES_AS_NULL);
        return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(javaObjectOrMap);
    }

    public static Object normalize(Object obj, int maxLevel, int currentLevel) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof Map) {
            Map<Object, Object> copy = new LinkedHashMap<>();
            Map<Object, Object> map = (Map) obj;
            if (currentLevel <= maxLevel) {
                for (Entry<Object, Object> entry : map.entrySet()) {
                    copy.put(
                            normalize(entry.getKey(), maxLevel, currentLevel + 1),
                            normalize(entry.getValue(), maxLevel, currentLevel + 1)
                    );
                }
            } else {
                return "IGNORED MAP";
            }
            return copy;
        } else if (obj instanceof List) {
            List<Object> copy = new ArrayList<>();
            List<Object> list = (List) obj;
            if (currentLevel <= maxLevel) {
                for (Object o : list) {
                    copy.add(normalize(o, maxLevel, currentLevel + 1));
                }
            } else {
                return "IGNORED LIST";
            }
            return copy;
        } else if (obj instanceof Set) {
            List<Object> copy = new ArrayList<>();
            Set<Object> set = (Set) obj;
            if (currentLevel <= maxLevel) {
                for (Object o : set) {
                    copy.add(normalize(o, maxLevel, currentLevel + 1));
                }
            } else {
                return "IGNORED SET";
            }
            return copy;
        } else if (obj instanceof ScriptUtils.Entry) {
            Map<Object, Object> map = new LinkedHashMap<>();
            ScriptUtils.Entry entry = (ScriptUtils.Entry) obj;
            if (currentLevel <= maxLevel) {
                map.put(
                        normalize(entry.getKey(), maxLevel, currentLevel + 1),
                        normalize(entry.getValue(), maxLevel, currentLevel + 1)
                );
            } else {
                return "IGNORED ENTRY";
            }
            return map;
        } else if (obj instanceof ArrayItemValue) {
            return normalize(((ArrayItemValue) obj).getInstance(), maxLevel, currentLevel);
        } else if (obj instanceof JavaClass) {
            return "Class<" + ((JavaClass) obj).getName() + ">";
        } else if (obj instanceof ObjectFieldValue) {
            return HprofObjectUtils.getName((ObjectFieldValue) obj) + ": " + normalize(((ObjectFieldValue) obj).getInstance(), maxLevel, currentLevel + 1);
        } else if (obj instanceof FieldValue) {
            return HprofObjectUtils.getName((FieldValue) obj) + ": " + normalize(((FieldValue) obj).getValue(), maxLevel, currentLevel + 1);
        }
        if (!(obj instanceof Instance)) {
            return obj;
        }

        Instance instance = (Instance) obj;
        JavaClass javaClass = instance.getJavaClass();
        if (hasSuperClass(javaClass, "java.lang.String")) {
            return StringInstanceUtils.getDetailsString(instance);
        } else if (hasSuperClass(javaClass, "org.apache.doris.thrift.TUniqueId")) {
            return printId((Long) instance.getValueOfField("hi"), (Long) instance.getValueOfField("lo"));
        } else if (hasSuperClass(javaClass, "org.apache.doris.proto.Types.PUniqueId")) {
            return printId((Long) instance.getValueOfField("hi_"), (Long) instance.getValueOfField("lo_"));
        }

        if (currentLevel > maxLevel) {
            return ((Instance) obj).getJavaClass().getName() + "#" + ((Instance) obj).getInstanceId();
        }
        if (hasSuperClass(javaClass, "java.util.HashMap")) {
            Map<Object, Object> map = new LinkedHashMap<>();
            ObjectArrayInstance table = (ObjectArrayInstance) instance.getValueOfField("table");
            if (table == null) {
                return map;
            }
            for (ArrayItemValue item : table.getItems()) {
                Instance entry = item.getInstance();
                while (entry != null) {
                    Object key = entry.getValueOfField("key");
                    Object value = entry.getValueOfField("value");
                    map.put(normalize(key, maxLevel, currentLevel + 1), normalize(value, maxLevel, currentLevel + 1));

                    entry = (Instance) entry.getValueOfField("next");
                }
            }
            return map;
        } else if (hasSuperClass(javaClass, "java.util.concurrent.ConcurrentHashMap")) {
            Map<Object, Object> map = new LinkedHashMap<>();
            ObjectArrayInstance table = (ObjectArrayInstance) instance.getValueOfField("table");
            if (table == null) {
                return map;
            }
            for (ArrayItemValue item : table.getItems()) {
                Instance entry = item.getInstance();
                while (entry != null) {
                    Object key = entry.getValueOfField("key");
                    Object value = entry.getValueOfField("val");
                    map.put(normalize(key, maxLevel, currentLevel + 1), normalize(value, maxLevel, currentLevel + 1));

                    entry = (Instance) entry.getValueOfField("next");
                }
            }
            return map;

        } else if (hasSuperClass(javaClass, "com.google.common.collect.RegularImmutableMap")) {
            Map<Object, Object> map = new LinkedHashMap<>();
            ObjectArrayInstance listObj = (ObjectArrayInstance) instance.getValueOfField("table");
            if (listObj == null) {
                return map;
            }
            for (int i = 0; i < listObj.getItems().size(); i++) {
                ArrayItemValue item = listObj.getItems().get(i);
                Instance entry = item.getInstance();
                if (entry == null) {
                    continue;
                }
                Object key = entry.getValueOfField("key");
                Object value = entry.getValueOfField("value");
                map.put(normalize(key, maxLevel, currentLevel + 1), normalize(value, maxLevel, currentLevel + 1));
            }
            return map;
        } else if (hasSuperClass(javaClass, "java.util.HashSet")) {
            List<Object> set = new ArrayList<>();
            Instance map = (Instance) javaClass.getValueOfStaticField("map");
            ObjectArrayInstance table = (ObjectArrayInstance) instance.getValueOfField("table");
            if (table == null) {
                return set;
            }
            for (ArrayItemValue item : table.getItems()) {
                Instance entry = item.getInstance();
                while (entry != null) {
                    Object key = entry.getValueOfField("key");
                    set.add(normalize(key, maxLevel, currentLevel + 1));
                    entry = (Instance) entry.getValueOfField("next");
                }
            }
            return map;
        } else if (hasSuperClass(javaClass, "java.util.ArrayList")) {
            List<Object> list = new ArrayList<>();
            Integer size = (Integer) instance.getValueOfField("size");
            ObjectArrayInstance listObj = (ObjectArrayInstance) instance.getValueOfField("elementData");
            if (listObj == null) {
                return list;
            }
            for (int i = 0; i < size; i++) {
                ArrayItemValue item = listObj.getItems().get(i);
                list.add(normalize(item.getInstance(), maxLevel, currentLevel + 1));
            }
            return list;
        } else if (hasSuperClass(javaClass, "com.google.common.collect.RegularImmutableList")) {
            List<Object> list = new ArrayList<>();
            ObjectArrayInstance listObj = (ObjectArrayInstance) instance.getValueOfField("array");
            if (listObj == null) {
                return list;
            }
            for (int i = 0; i < listObj.getItems().size(); i++) {
                ArrayItemValue item = listObj.getItems().get(i);
                list.add(normalize(item.getInstance(), maxLevel, currentLevel + 1));
            }
            return list;
        } else if (hasSuperClass(javaClass, "com.google.common.collect.RegularImmutableSet")) {
            List<Object> list = new ArrayList<>();
            ObjectArrayInstance listObj = (ObjectArrayInstance) instance.getValueOfField("elements");
            if (listObj == null) {
                return list;
            }
            for (int i = 0; i < listObj.getItems().size(); i++) {
                ArrayItemValue item = listObj.getItems().get(i);
                list.add(normalize(item.getInstance(), maxLevel, currentLevel + 1));
            }
            return list;
        } else if (instance instanceof ObjectArrayInstance) {
            ObjectArrayInstance listObj = (ObjectArrayInstance) instance;
            List<Object> list = new ArrayList<>();
            for (int i = 0; i < listObj.getItems().size(); i++) {
                ArrayItemValue item = listObj.getItems().get(i);
                list.add(normalize(item.getInstance(), maxLevel, currentLevel + 1));
            }
            return list;
        } else {
            Map<Object, Object> map = new LinkedHashMap<>();
            for (Field field : javaClass.getFields()) {
                Object value = instance.getValueOfField(field.getName());
                map.put(field.getName(), toJavaObject(value, maxLevel, currentLevel + 1));
            }
            return map;
        }
    }

    public static Object toJavaObject(Object obj, int maxLevel, int currentLevel) {
        return normalize(obj, maxLevel, currentLevel + 1);
    }

    public static String formatDate(long time) {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(time));
    }

    public static String formatBytes(long bytes) {
        if (bytes >= 1024L * 1024 * 1024 * 1024) {
            return new BigDecimal(bytes).divide(BigDecimal.valueOf(1024L * 1024 * 1024 * 1024), new MathContext(3, RoundingMode.HALF_UP)) + "T";
        } else if (bytes >= 1024L * 1024 * 1024) {
            return new BigDecimal(bytes).divide(BigDecimal.valueOf(1024L * 1024 * 1024), new MathContext(3, RoundingMode.HALF_UP)) + "G";
        } else if (bytes >= 1024L * 1024) {
            return new BigDecimal(bytes).divide(BigDecimal.valueOf(1024L * 1024), new MathContext(3, RoundingMode.HALF_UP)) + "M";
        } else if (bytes >= 1024L) {
            return new BigDecimal(bytes).divide(BigDecimal.valueOf(1024L), new MathContext(3, RoundingMode.HALF_UP)) + "K";
        } else {
            return bytes + "B";
        }
    }

    public static class CustomClassLoader extends URLClassLoader {

        public CustomClassLoader(URL[] urls, ClassLoader parent) {
            super(urls, parent);
        }

        public CustomClassLoader(URL[] urls) {
            super(urls);
        }

        public CustomClassLoader(URL[] urls, ClassLoader parent, URLStreamHandlerFactory factory) {
            super(urls, parent, factory);
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            // Avoid loading JDK 14's Nashorn classes
            if (name.startsWith("org.openjdk.nashorn.") && belowJdk14()) {
                if (name.equals("org.openjdk.nashorn.api.scripting.NashornScriptEngineFactory")) {
                    return super.loadClass("jdk.nashorn.api.scripting.NashornScriptEngineFactory");
                }
                return null;
            }
            return super.loadClass(name, resolve);
        }

        private boolean belowJdk14() {
            String version = System.getProperty("java.version");
            try {
                String[] parts = version.split("\\.");
                if (parts[0].equals("1")) { // 1.8.0_402
                    return true;
                } else if (Integer.valueOf(parts[0]) < 14) { // 11.0.25
                    return true;
                } else { // 17.0.13
                    return false;
                }
            } catch (Throwable t) {
                return false;
            }
        }
    }
}
