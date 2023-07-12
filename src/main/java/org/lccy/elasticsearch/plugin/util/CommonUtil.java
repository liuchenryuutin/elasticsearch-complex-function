package org.lccy.elasticsearch.plugin.util;

import java.util.Collection;

/**
 * StringUtil
 *
 * @author liuchen <br>
 * @date 2023-07-08
 */
public final class CommonUtil {

    private CommonUtil() {
    }

    public static boolean isEmpty(String str) {
        if (null == str) {
            return true;
        } else {
            return "".equals(str.trim());
        }
    }

    public static boolean isEmpty(Collection<?> list) {
        return list == null || list.isEmpty();
    }

    public static boolean isNotEmpty(String str) {
        return !isEmpty(str);
    }

    public static String toString(Object obj) {
        return obj == null ? "" : obj.toString();
    }
}
