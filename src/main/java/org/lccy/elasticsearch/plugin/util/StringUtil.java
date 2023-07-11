package org.lccy.elasticsearch.plugin.util;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.lccy.elasticsearch.plugin.function.ComplexFieldFunctionBuilder;

import java.util.Map;

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
