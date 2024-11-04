package org.graalvm.visualvm.lib.jfluid.heap;

public class StringInstanceUtils {
    public static String getString(Instance instance) {
        PrimitiveArrayDump array = (PrimitiveArrayDump) instance.getValueOfField("value");
        StringBuilder str = new StringBuilder();
        for (String ch : array.getValues()) {
            str.append(ch);
        }
        return str.toString();
    }
}
