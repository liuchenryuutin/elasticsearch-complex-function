package org.lccy.elasticsearch.plugin.function;

/**
 * constants class <br>
 *
 * @author liuchen <br>
 * @date 2023-07-11
 */
public interface Constants {

    String SPLIT = "&_&"; //分隔符（多个field在一起时使用）

    interface FieldMode {
        String SUM = "sum";
        String MULT = "mult";
        String MAX = "max";
        String MIN = "min";
    }

    interface SortMode {
        String MAX = "max";
        String MIN = "min";
    }


    interface SortValueType {
        String EQUAL = "equal"; //相等为true
        String NOT = "not"; //不相等时为true
        String ANY = "any"; //任何时候都为true
        String EXISTS = "exists"; //存在时为true
        String NOT_EXISTS = "not_exists"; //不存在时为true
        String IN = "in"; //包含时为true
        String NOT_IN = "not_in"; //不包含为true
    }
}
