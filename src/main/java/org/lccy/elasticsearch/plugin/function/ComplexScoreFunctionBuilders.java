package org.lccy.elasticsearch.plugin.function;

import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.lccy.elasticsearch.plugin.function.bo.CategoryScoreWapper;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * build tool class <br>
 *
 * @author liuchen <br>
 * @date 2023-07-11
 */
public class ComplexScoreFunctionBuilders extends ScoreFunctionBuilders {

    public static ComplexFieldFunctionBuilder complexFieldFunction(Double funcScoreFactor, Double originalScoreFactor, List<Map> categorys
            , String categoryField) {
        if(categorys == null || categoryField.isEmpty()) {
            throw new IllegalArgumentException("require param is not set, please check.");
        }
        Map<String, CategoryScoreWapper> categoryScoreWapperMap = categorys.stream().map(x -> new CategoryScoreWapper(null, x)).collect(Collectors.toMap(x -> x.getName(), x -> x, (x1, x2) -> x2));
        return new ComplexFieldFunctionBuilder(funcScoreFactor, originalScoreFactor, categoryScoreWapperMap, categoryField);
    }
}
