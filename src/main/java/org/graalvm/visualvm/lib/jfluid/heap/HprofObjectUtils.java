package org.graalvm.visualvm.lib.jfluid.heap;

public class HprofObjectUtils {
    public static String getName(Value value) {
        if (value instanceof FieldValue) {
            return ((FieldValue) value).getField().getDeclaringClass().getName() + "." + ((FieldValue) value).getField().getName() + "#" + value.getDefiningInstance().getInstanceId();
        } else if (value instanceof ArrayItemValue) {
            return value.getDefiningInstance().getJavaClass().getName() + "." + ((ArrayItemValue) value).getIndex() + "#" + value.getDefiningInstance().getInstanceId();
        } else {
            Instance definingInstance = value.getDefiningInstance();
            return definingInstance.getJavaClass().getName() + ".?#" + definingInstance.getInstanceId();
        }
    }
}
