package org.lccy.elasticsearch.plugin.function;

import org.elasticsearch.index.query.functionscore.FieldValueFactorFunctionBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.lccy.elasticsearch.plugin.function.bo.CategoryScoreWapper;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 类名称： <br>
 * 类描述： <br>
 *
 * @Date: 2023/07/11 17:51 <br>
 * @author: liuchen11
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
