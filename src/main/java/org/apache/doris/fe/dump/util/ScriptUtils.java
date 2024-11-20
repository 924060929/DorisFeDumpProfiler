package org.apache.doris.fe.dump.util;

import static org.apache.doris.fe.dump.AnalyzeDorisFeHprof.hasSuperClass;
import org.graalvm.visualvm.lib.jfluid.heap.ArrayItemValue;
import org.graalvm.visualvm.lib.jfluid.heap.Instance;
import org.graalvm.visualvm.lib.jfluid.heap.JavaClass;
import org.graalvm.visualvm.lib.jfluid.heap.ObjectArrayInstance;
import org.graalvm.visualvm.lib.jfluid.heap.StringInstanceUtils;
import org.graalvm.visualvm.lib.profiler.oql.engine.api.OQLEngine;
import org.graalvm.visualvm.lib.profiler.oql.engine.api.OQLException;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ScriptUtils {
    public static final ThreadLocal<OQLEngine> engine = new ThreadLocal<>();

    public static void registerFunctions(OQLEngine oqlEngine) throws OQLException {
        engine.set(oqlEngine);
        registerFunction(oqlEngine, "getFieldValue", 2);
        registerFunction(oqlEngine, "getString", 1);
        registerFunction(oqlEngine, "printId", 1);
        registerFunction(oqlEngine, "getMapEntries", 1);
        registerFunction(oqlEngine, "fromUnixtime", 1);
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
            Map<Object, Object> map = new LinkedHashMap<>();
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

    public static String getString(Object instance) {
        instance = unwrapJavaObject(instance);
        if (!(instance instanceof Instance)) {
            return null;
        }
        return StringInstanceUtils.getDetailsString(instance);
    }

    public static Object getFieldValue(Object instance, String name) {
        instance = unwrapJavaObject(instance);
        if (!(instance instanceof Instance)) {
            return null;
        }

        return ((Instance) instance).getValueOfField(name);
    }

    public static String printId(Object instance) {
        if (instance instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) instance;
            Long lo = (Long) map.get("lo");
            if (lo == null) {
                lo = (Long) map.get("lo_");
            }
            Long hi = (Long) map.get("hi");
            if (hi == null) {
                hi = (Long) map.get("hi_");
            }
            return printId(hi, lo);
        } else if (instance instanceof Instance) {
            Long lo = (Long) ((Instance) instance).getValueOfField("lo");
            if (lo == null) {
                lo = (Long) ((Instance) instance).getValueOfField("lo_");
            }
            Long hi = (Long) ((Instance) instance).getValueOfField("hi");
            if (hi == null) {
                hi = (Long) ((Instance) instance).getValueOfField("hi_");
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
}
