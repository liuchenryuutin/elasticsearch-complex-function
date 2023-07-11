package org.lccy.elasticsearch.plugin;

import org.junit.Test;
import org.lccy.elasticsearch.plugin.function.bo.FieldScoreComputeWapper;

/**
 * 类名称： <br>
 * 类描述： <br>
 *
 * @Date: 2023/07/11 16:16 <br>
 * @author: liuchen11
 */
public class MathUtil {

    @Test
    public void Test_1() {
        double data = (0.000000 + 0.125000 * FieldScoreComputeWapper.Modifier.fromString("log1p").apply(2500.0)) * 50.000000;
        System.out.printf(data + "");
    }

    @Test
    public void Test_2() {
        double data = (1.000000 + -0.200000 * FieldScoreComputeWapper.Modifier.fromString("log1p").apply(600.0)) * 50.000000;
        System.out.printf(data + "");

    }
}
