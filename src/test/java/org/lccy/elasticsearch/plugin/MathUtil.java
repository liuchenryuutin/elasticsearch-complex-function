package org.lccy.elasticsearch.plugin;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.script.ScoreScriptUtils;
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

    @Test
    public void Test_3() {
        double data = (0.000000 + 1.000000 * decaygeoexp("31, 33")) * 50.000000;
        System.out.printf(data + "");

    }

    private double decaygeoexp(String pot) {
        GeoPoint point = new GeoPoint(pot);
        String origin = "31,33";
        String offset = "500m";
        String scale = "5km";
        double decay = 0.5;
        double fieldScore = new ScoreScriptUtils.DecayGeoExp(origin, scale, offset, decay).decayGeoExp(point);
        return fieldScore;
    }
}
