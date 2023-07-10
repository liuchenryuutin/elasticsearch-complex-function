package org.lccy.elasticsearch.plugin.util;

/**
 * StringUtil
 *
 * @author liuchen <br>
 * @date 2023-07-08
 */
public final class StringUtil {

    private StringUtil() {
    }

    public static boolean isEmpty(String str) {
        if (null == str) {
            return true;
        } else {
            return "".equals(str.trim());
        }
    }

    public static boolean isNotEmpty(String str) {
        return !isEmpty(str);
    }

    public static String toString(Object obj) {
        return obj == null ? "" : obj.toString();
    }
}
