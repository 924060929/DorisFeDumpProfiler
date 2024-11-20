package org.graalvm.visualvm.lib.jfluid.heap;

import org.apache.doris.fe.dump.util.ScriptUtils;

import org.graalvm.visualvm.lib.profiler.heapwalk.details.api.StringDecoder;
import org.graalvm.visualvm.lib.profiler.heapwalk.details.spi.DetailsProvider;
import org.graalvm.visualvm.lib.profiler.heapwalk.details.spi.DetailsUtils;
import org.graalvm.visualvm.lib.profiler.oql.engine.api.OQLEngine;
import org.openide.util.Lookup;
import org.openide.util.LookupEvent;
import org.openide.util.LookupListener;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class StringInstanceUtils {

    private static final LinkedHashMap<Long, List<ProviderClassPair>> PROVIDERS_CACHE =
            new LinkedHashMap<Long, List<ProviderClassPair>>(10000) {
                protected boolean removeEldestEntry(Map.Entry eldest) {
                    return size() > 5000;
                }
            };

    public static final String STRING_MASK = "java.lang.String";                           // NOI18N
    public static final String BUILDERS_MASK = "java.lang.AbstractStringBuilder+";         // NOI18N

    public static String getDetailsString(Object instance) {
        if (instance instanceof Map && instance.getClass().getSimpleName().equals("ScriptObjectMirror")) {
            OQLEngine oqlEngine = ScriptUtils.engine.get();
            if (oqlEngine != null) {
                instance = oqlEngine.unwrapJavaObject(instance, true);
            }
        }

        if (instance == null) {
            return null;
        }
        if (!(instance instanceof Instance)) {
            return null;
        }

        // TODO [Tomas]: cache computed string per heap
        Collection<ProviderClassPair> pairs = getCompatibleProviders(((Instance) instance).getJavaClass());
        for (ProviderClassPair pair : pairs) {
            String classKey = pair.classKey;
            if (pair.subclasses) classKey += "+";                               // NOI18N
            String string = getDetailsString(classKey, (Instance) instance);
            if (string != null) return string;
        }
        return null;
    }

    public static String getDetailsString(String className, Instance instance) {
        if (STRING_MASK.equals(className)) {                                        // String
            byte coder = DetailsUtils.getByteFieldValue(instance, "coder", (byte) -1);     // NOI18N
            if (coder == -1) {
                int offset = DetailsUtils.getIntFieldValue(instance, "offset", 0);      // NOI18N
                int count = DetailsUtils.getIntFieldValue(instance, "count", -1);       // NOI18N
                return getPrimitiveArrayFieldString(instance, "value",     // NOI18N
                        offset, count, null);                // NOI18N
            } else {
                return getJDK9String(instance, "value", coder, null);          // NOI18N
            }
        } else if (BUILDERS_MASK.equals(className)) {                               // AbstractStringBuilder+
            byte coder = DetailsUtils.getByteFieldValue(instance, "coder", (byte) -1);  // NOI18N
            if (coder == -1) {
                int count = DetailsUtils.getIntFieldValue(instance, "count", -1);       // NOI18N
                return getPrimitiveArrayFieldString(instance, "value",     // NOI18N
                        0, count, null);                // NOI18N
            } else {
                return getJDK9String(instance, "value", coder, null);          // NOI18N
            }
        }
        return null;
    }

    private static String getJDK9String(Instance instance, String field, byte coder, String separator) {
        Object byteArray = instance.getValueOfField(field);
        if (byteArray instanceof PrimitiveArrayInstance) {
            List<String> values = ((PrimitiveArrayInstance) byteArray).getValues();
            if (values != null) {
                Heap heap = instance.getJavaClass().getHeap();
                StringDecoder decoder = new StringDecoder(heap, coder, values);
                int valuesCount = decoder.getStringLength();
                int separatorLength = separator == null ? 0 : separator.length();
                int estimatedSize = valuesCount * (1 + separatorLength);
                StringBuilder value = new StringBuilder(estimatedSize);
                int lastValue = valuesCount - 1;
                for (int i = 0; i <= lastValue; i++) {
                    value.append(decoder.getValueAt(i));
                    if (separator != null && i < lastValue) {
                        value.append(separator);
                    }
                }
                return value.toString();
            }
        }
        return null;
    }

    public static String getPrimitiveArrayFieldString(Instance instance, String field, int offset, int count, String separator) {
        Object value = instance.getValueOfField(field);
        return value instanceof Instance ? getPrimitiveArrayString((Instance)value, offset, count, separator) : null;
    }

    public static String getPrimitiveArrayString(Instance instance, int offset, int count, String separator) {
        List<String> values = getPrimitiveArrayValues(instance);
        if (values != null) {
            int valuesCount = count < 0 ? values.size() - offset : Math.min(count, values.size() - offset);
            int separatorLength = separator == null ? 0 : separator.length();
            int estimatedSize = valuesCount * (1 + separatorLength);
            StringBuilder value = new StringBuilder(estimatedSize);
            int lastValue = offset + valuesCount - 1;
            for (int i = offset; i <= lastValue; i++) {
                value.append(values.get(i));
                if (separator != null && i < lastValue) value.append(separator);
            }
            return value.toString();
        }
        return null;
    }

    public static List<String> getPrimitiveArrayValues(Instance instance) {
        if (instance instanceof PrimitiveArrayInstance) {
            PrimitiveArrayInstance array = (PrimitiveArrayInstance)instance;
            return array.getValues();
        }
        return null;
    }

    private static List<ProviderClassPair> getCompatibleProviders(JavaClass cls) {
        Long classId = cls.getJavaClassId();

        // Query the cache for already computed DetailsProviders
        List<ProviderClassPair> cachedPairs = PROVIDERS_CACHE.get(classId);
        if (cachedPairs != null) return cachedPairs;

        // All registered className|DetailsProvider pairs
        List<ProviderClassPair> allPairs = new ArrayList<>();
        List<ProviderClassPair> simplePairs = new ArrayList<>();
        Collection<? extends DetailsProvider> providers = getProviders();
        for (DetailsProvider provider : providers) {
            String[] classes = provider.getSupportedClasses();
            if (classes != null && classes.length > 0)
                for (String classs : classes)
                    allPairs.add(new ProviderClassPair(provider, classs));
            else simplePairs.add(new ProviderClassPair(provider, null));
        }

        List<ProviderClassPair> pairs = new ArrayList<>();

        // Only compatible className|DetailsProvider pairs
        if (!allPairs.isEmpty()) {
            boolean superClass = false;
            while (cls != null) {
                String clsName = cls.getName();
                for (ProviderClassPair pair : allPairs)
                    if ((pair.subclasses || !superClass) &&
                            clsName.equals(pair.classKey))
                        pairs.add(pair);
                cls = cls.getSuperClass();
                superClass = true;
            }
        }

        // DetailsProviders without className definitions
        pairs.addAll(simplePairs);

        // Cache the computed DetailsProviders
        PROVIDERS_CACHE.put(classId, pairs);

        return pairs;
    }

    private static class ProviderClassPair {

        final DetailsProvider provider;
        final String classKey;
        final boolean subclasses;

        ProviderClassPair(DetailsProvider provider, String classKey) {
            subclasses = classKey != null && classKey.endsWith("+");            // NOI18N
            this.provider = provider;
            this.classKey = !subclasses ? classKey :
                    classKey.substring(0, classKey.length() - 1);
        }

    }

    private static Lookup.Result<DetailsProvider> PROVIDERS;
    private static Collection<? extends DetailsProvider> getProviders() {
        if (PROVIDERS == null) {
            PROVIDERS = Lookup.getDefault().lookupResult(DetailsProvider.class);
            PROVIDERS.addLookupListener(new LookupListener() {
                public void resultChanged(LookupEvent ev) { PROVIDERS_CACHE.clear(); }
            });
        }
        return PROVIDERS.allInstances();
    }
}
