package org.lccy.elasticsearch.plugin.function;

/**
 * constants class <br>
 *
 * @author liuchen <br>
 * @date 2023-07-11
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


    interface SortValueType {
        String NOT = "not";
    }
}
