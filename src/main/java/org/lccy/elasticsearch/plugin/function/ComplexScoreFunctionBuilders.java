package org.lccy.elasticsearch.plugin.function;

import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.lccy.elasticsearch.plugin.function.bo.CategoryScoreWapper;
import org.lccy.elasticsearch.plugin.util.CommonUtil;

import java.util.Map;

/**
 * build tool class <br>
 *
 * @author liuchen <br>
 * @date 2023-07-11
 */
public class ComplexScoreFunctionBuilders extends ScoreFunctionBuilders {

    public static ComplexFieldFunctionBuilder complexFieldFunction(Map<String, Object> categorys) {
        if (CommonUtil.isEmpty(categorys)) {
            throw new IllegalArgumentException("require param is not set, please check.");
        }
        CategoryScoreWapper categoryScoreWapper = new CategoryScoreWapper(null, categorys);
        return new ComplexFieldFunctionBuilder(categoryScoreWapper);
    }
}
