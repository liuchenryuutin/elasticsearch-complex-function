package org.lccy.elasticsearch.plugin.function.bo;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.lccy.elasticsearch.plugin.function.ComplexFieldFunctionBuilder;
import org.lccy.elasticsearch.plugin.function.Constants;
import org.lccy.elasticsearch.plugin.util.StringUtil;
import sun.tools.java.Type;

import java.util.Map;
import java.util.Objects;

/**
 * 类名称： <br>
 * 类描述： <br>
 *
 * @Date: 2023/07/11 09:48 <br>
 * @author: liuchen11
 */
public class SortScoreComputeWapper {

    public static final String WEIGHT = "weight";
    public static final String FIELD = "field";
    public static final String TYPE = "type";
    public static final String VALUE = "value";

    private Map<String, Object> sortScore;

    public SortScoreComputeWapper(XContentParser parser, Map<String, Object> st) {
        Integer weight = st.get(WEIGHT) == null ? null : Integer.parseInt(st.get(WEIGHT).toString());
        String field = (String) st.get(FIELD);
        if (StringUtil.isEmpty(field) || weight == null) {
            throwsException(parser, ComplexFieldFunctionBuilder.NAME + " query param [categorys] [sort_score] setting has error, please check.");
        }
        this.sortScore = st;
    }

    public Integer getWeight() {
        return Integer.parseInt(sortScore.get(WEIGHT).toString());
    }

    public String getField() {
        return (String) sortScore.get(FIELD);
    }

    public String getType() {
        return (String) sortScore.get(TYPE);
    }

    public String getValue() {
        return (String) sortScore.get(VALUE);
    }

    private void throwsException(XContentParser parser, String msg) {
        if(parser != null) {
            throw new ParsingException(parser.getTokenLocation(), msg);
        } else {
            throw new IllegalArgumentException(msg);
        }
    }

    public boolean match(String[] values) {
        for (String val : values) {
            if (this.getValue().equals(val)) {
                return !Constants.SortValueType.NOT.equals(this.getType());
            }
        }

        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SortScoreComputeWapper that = (SortScoreComputeWapper) o;
        return Objects.equals(sortScore, that.sortScore);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sortScore);
    }
}
