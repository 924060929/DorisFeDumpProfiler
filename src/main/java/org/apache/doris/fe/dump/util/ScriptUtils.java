package org.apache.doris.fe.dump.util;

import static org.apache.doris.fe.dump.AnalyzeDorisFeHprof.hasSuperClass;
import org.graalvm.visualvm.lib.jfluid.heap.ArrayItemValue;
import org.graalvm.visualvm.lib.jfluid.heap.HprofObjectUtils;
import org.graalvm.visualvm.lib.jfluid.heap.Instance;
import org.graalvm.visualvm.lib.jfluid.heap.JavaClass;
import org.graalvm.visualvm.lib.jfluid.heap.ObjectArrayInstance;
import org.graalvm.visualvm.lib.jfluid.heap.StringInstanceUtils;
import org.graalvm.visualvm.lib.jfluid.heap.Value;
import org.graalvm.visualvm.lib.profiler.oql.engine.api.OQLEngine;
import org.graalvm.visualvm.lib.profiler.oql.engine.api.OQLException;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ScriptUtils {
    public static final ThreadLocal<OQLEngine> engine = new ThreadLocal<>();

    public static void registerFunctions(OQLEngine oqlEngine) throws OQLException {
        engine.set(oqlEngine);
        registerFunction(oqlEngine, "getFieldValue", 2);
        registerFunction(oqlEngine, "getStaticFieldValue", 2);
        registerFunction(oqlEngine, "getString", 1);
        registerFunction(oqlEngine, "printId", 1);
        registerFunction(oqlEngine, "getMapEntries", 1);
        registerFunction(oqlEngine, "fromUnixtime", 1);

        registerFunction(oqlEngine, "getRetainedSize", 1);
        registerFunction(oqlEngine, "getReachableSize", 1);
        registerFunction(oqlEngine, "isGcRoot", 1);
        registerFunction(oqlEngine, "getSize", 1);
        registerFunction(oqlEngine, "getInstanceId", 1);
        registerFunction(oqlEngine, "getInstanceNumber", 1);
        registerFunction(oqlEngine, "getNearestGCRootPointer", 1);
        registerFunction(oqlEngine, "getReferences", 1);
        registerFunction(oqlEngine, "getReferencesString", 1);
        registerFunction(oqlEngine, "getReferencesGraph", 1);
        registerFunction(oqlEngine, "getNearestGCRootPath", 1);
    }

    private static void registerFunction(OQLEngine oqlEngine, String name, int paramNum) throws OQLException {
        String params = "";
        for (int i = 0; i < paramNum; i++) {
            if (i > 0) {
                params += ", ";
            }
            params += "param" + i;
        }
        oqlEngine.executeQuery("function " + name + "(" + params + ") {return org.apache.doris.fe.dump.util.ScriptUtils." + name + "(" + params + ");}", o -> true);
    }

    public static String fromUnixtime(Object object) {
        if (object instanceof Long) {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date((Long) object));
        }
        return null;
    }

    public static List<Entry> getMapEntries(Object object) {
        object = unwrapJavaObject(object);
        if (!(object instanceof Instance)) {
            return null;
        }

        List<Entry> entries = new ArrayList<>();

        Instance instance = (Instance) object;
        JavaClass javaClass = instance.getJavaClass();
        if (hasSuperClass(javaClass, "java.util.HashMap")) {
            ObjectArrayInstance table = (ObjectArrayInstance) instance.getValueOfField("table");
            if (table == null) {
                return null;
            }
            for (ArrayItemValue item : table.getItems()) {
                Instance entry = item.getInstance();
                while (entry != null) {
                    Object key = entry.getValueOfField("key");
                    Object value = entry.getValueOfField("value");
                    entries.add(new Entry(key, value));

                    entry = (Instance) entry.getValueOfField("next");
                }
            }
            return entries;
        } else if (hasSuperClass(javaClass, "java.util.concurrent.ConcurrentHashMap")) {
            ObjectArrayInstance table = (ObjectArrayInstance) instance.getValueOfField("table");
            if (table == null) {
                return null;
            }
            for (ArrayItemValue item : table.getItems()) {
                Instance entry = item.getInstance();
                while (entry != null) {
                    Object key = entry.getValueOfField("key");
                    Object value = entry.getValueOfField("val");
                    entries.add(new Entry(key, value));

                    entry = (Instance) entry.getValueOfField("next");
                }
            }
            return entries;
        } else if (hasSuperClass(javaClass, "com.google.common.collect.RegularImmutableMap")) {
            ObjectArrayInstance listObj = (ObjectArrayInstance) instance.getValueOfField("table");
            for (int i = 0; i < listObj.getItems().size(); i++) {
                ArrayItemValue item = listObj.getItems().get(i);
                Instance entry = item.getInstance();
                if (entry == null) {
                    continue;
                }
                Object key = entry.getValueOfField("key");
                Object value = entry.getValueOfField("value");
                entries.add(new Entry(key, value));
            }
            return entries;
        } else {
            return null;
        }
    }

    public static String getString(Object obj) {
        obj = unwrapJavaObject(obj);
        if (!(obj instanceof Instance)) {
            return null;
        }
        return StringInstanceUtils.getDetailsString(obj);
    }

    public static Object getFieldValue(Object obj, String name) {
        obj = unwrapJavaObject(obj);
        if (!(obj instanceof Instance)) {
            return null;
        }

        return ((Instance) obj).getValueOfField(name);
    }

    public static Object getStaticFieldValue(Object obj, String name) {
        obj = unwrapJavaObject(obj);
        if (!(obj instanceof Instance)) {
            return null;
        }
        return ((Instance) obj).getJavaClass().getValueOfStaticField(name);
    }

    public static Long getRetainedSize(Object obj) {
        obj = unwrapJavaObject(obj);
        if (!(obj instanceof Instance)) {
            return null;
        }
        return ((Instance) obj).getRetainedSize();
    }

    public static Long getReachableSize(Object obj) {
        obj = unwrapJavaObject(obj);
        if (!(obj instanceof Instance)) {
            return null;
        }
        return ((Instance) obj).getReachableSize();
    }

    public static boolean isGcRoot(Object obj) {
        obj = unwrapJavaObject(obj);
        if (!(obj instanceof Instance)) {
            return false;
        }
        return ((Instance) obj).isGCRoot();
    }

    public static Long getSize(Object obj) {
        obj = unwrapJavaObject(obj);
        if (!(obj instanceof Instance)) {
            return null;
        }
        return ((Instance) obj).getSize();
    }

    public static Long getInstanceId(Object obj) {
        obj = unwrapJavaObject(obj);
        if (!(obj instanceof Instance)) {
            return null;
        }
        return ((Instance) obj).getInstanceId();
    }

    public static Integer getInstanceNumber(Object obj) {
        obj = unwrapJavaObject(obj);
        if (!(obj instanceof Instance)) {
            return null;
        }
        return ((Instance) obj).getInstanceNumber();
    }

    public static Instance getNearestGCRootPointer(Object obj) {
        obj = unwrapJavaObject(obj);
        if (!(obj instanceof Instance)) {
            return null;
        }
        return ((Instance) obj).getNearestGCRootPointer();
    }

    public static List<Value> getReferences(Object obj) {
        obj = unwrapJavaObject(obj);
        if (!(obj instanceof Instance)) {
            return null;
        }
        return ((Instance) obj).getReferences();
    }

    public static List<String> getReferencesString(Object obj) {
        obj = unwrapJavaObject(obj);
        if (!(obj instanceof Instance)) {
            return null;
        }

        Instance instance = (Instance) obj;
        List<Value> references = instance.getReferences();
        List<String> refStrings = new ArrayList<>();
        for (Value reference : references) {
            if (reference.getDefiningInstance().getInstanceId() == instance.getInstanceId()) {
                continue;
            }
            refStrings.add(HprofObjectUtils.getName(reference));
        }
        return refStrings;
    }

    public static List<RefNode> getReferencesGraph(Object obj) {
        obj = unwrapJavaObject(obj);
        if (!(obj instanceof Instance)) {
            return null;
        }
        Set<Long> instanceIds = new HashSet<>();
        List<RefNode> graph = new ArrayList<>();
        visitReferences((Instance) obj, instanceIds, graph);
        return graph;
    }

    private static void visitReferences(Instance instance, Set<Long> instanceIds, List<RefNode> graph) {
        instanceIds.add(instance.getInstanceId());

        List<Value> references = instance.getReferences();
        for (Value reference : references) {
            if (instanceIds.contains(reference.getDefiningInstance().getInstanceId())) {
                continue;
            }

            RefNode refNode = new RefNode(HprofObjectUtils.getName(reference), new ArrayList<>());
            graph.add(refNode);
            Instance ref = reference.getDefiningInstance();
            visitReferences(ref, instanceIds, refNode.references);
        }
    }

    public static List<String> getNearestGCRootPath(Object obj) {
        obj = unwrapJavaObject(obj);
        if (!(obj instanceof Instance)) {
            return null;
        }
        Instance instance = (Instance) obj;
        List<String> result = new ArrayList<>();
        result.add(instance.getJavaClass().getName() + "#" + instance.getInstanceId());

        Instance current = instance;
        Instance next = instance.getNearestGCRootPointer();
        while (next != null && next != current) {
            result.add(next.getJavaClass().getName() + "#" + next.getInstanceId());
            current = next;
            next = current.getNearestGCRootPointer();
        }
        return result;
    }

    public static String printId(Object obj) {
        if (obj instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) obj;
            Long lo = (Long) map.get("lo");
            if (lo == null) {
                lo = (Long) map.get("lo_");
            }
            Long hi = (Long) map.get("hi");
            if (hi == null) {
                hi = (Long) map.get("hi_");
            }
            return printId(hi, lo);
        } else if (obj instanceof Instance) {
            Long lo = (Long) ((Instance) obj).getValueOfField("lo");
            if (lo == null) {
                lo = (Long) ((Instance) obj).getValueOfField("lo_");
            }
            Long hi = (Long) ((Instance) obj).getValueOfField("hi");
            if (hi == null) {
                hi = (Long) ((Instance) obj).getValueOfField("hi_");
            }
            return printId(hi, lo);
        }
        return null;
    }

    public static String printId(long hi, long lo) {
        StringBuilder builder = new StringBuilder();
        builder.append(Long.toHexString(hi)).append("-").append(Long.toHexString(lo));
        return builder.toString();
    }

    private static Object unwrapJavaObject(Object instance) {
        if (instance instanceof Map && instance.getClass().getSimpleName().equals("ScriptObjectMirror")) {
            OQLEngine oqlEngine = ScriptUtils.engine.get();
            if (oqlEngine != null) {
                instance = oqlEngine.unwrapJavaObject(instance, true);
            }
        }
        return instance;
    }

    public static class Entry {
        private Object key;
        private Object value;

        public Entry(Object key, Object value) {
            this.key = key;
            this.value = value;
        }

        public Object getKey() {
            return key;
        }

        public void setKey(Object key) {
            this.key = key;
        }

        public Object getValue() {
            return value;
        }

        public void setValue(Object value) {
            this.value = value;
        }
    }

    private static class RefNode {
        private final String name;
        private final List<RefNode> references;

        public RefNode(String name, List<RefNode> references) {
            this.name = name;
            this.references = references;
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
