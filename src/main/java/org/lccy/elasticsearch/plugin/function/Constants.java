package org.lccy.elasticsearch.plugin.function;

/**
 * 类名称： <br>
 * 类描述： <br>
 *
 * @Date: 2023/07/09 21:13 <br>
 * @author: liuchen11
 */
public interface Constants {


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
}
