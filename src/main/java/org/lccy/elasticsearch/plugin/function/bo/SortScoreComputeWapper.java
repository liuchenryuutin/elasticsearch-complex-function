package org.lccy.elasticsearch.plugin.function.bo;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.lccy.elasticsearch.plugin.function.ComplexFieldFunctionBuilder;
import org.lccy.elasticsearch.plugin.function.Constants;
import org.lccy.elasticsearch.plugin.util.CommonUtil;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * wrap sort score query param <br>
 *
 * @author liuchen <br>
 * @date 2023-07-11
 */
public class SortScoreComputeWapper {

    public static final String WEIGHT = "weight";
    public static final String FIELD = "field";
    public static final String TYPE = "type";
    public static final String VALUE = "value";

    private Map<String, Object> sortScore;
    private String field;
    private String type;
    private String value;
    private Integer weight;

    public SortScoreComputeWapper(XContentParser parser, Map<String, Object> st) {
        Integer weight = st.get(WEIGHT) == null ? null : Integer.parseInt(st.get(WEIGHT).toString());
        String field = CommonUtil.toString(st.get(FIELD));
        String type = CommonUtil.toString(st.get(TYPE));
        String value = CommonUtil.toString(st.get(VALUE));
        if (weight == null) {
            throwsException(parser, ComplexFieldFunctionBuilder.NAME + " query param [categorys] [sort_score] must has [weight], please check.");
        }
        if (!Constants.SortValueType.ANY.equals(type) && (CommonUtil.isEmpty(field) || CommonUtil.isEmpty(value))) {
            throwsException(parser, ComplexFieldFunctionBuilder.NAME + " query param [categorys] [sort_score], When the [type] is not [any], [field] [value] must be set.");
        }
        this.sortScore = st;
        this.field = field;
        this.type = type;
        this.value = value;
        this.weight = weight;
    }

    public Integer getWeight() {
        return weight;
    }

    public String getField() {
        return field;
    }

    public String getType() {
        return type;
    }

    public String getValue() {
        return value;
    }

    private void throwsException(XContentParser parser, String msg) {
        if (parser != null) {
            throw new ParsingException(parser.getTokenLocation(), msg);
        } else {
            throw new IllegalArgumentException(msg);
        }
    }

    public String getExpression(double sortBaseSocre) {
        String oper = Constants.SortValueType.NOT.equals(this.getType()) ? "!=" : "=";
        return String.format(Locale.ROOT, "if %s %s %s, then exec %s * %f, else do nothing.", getField(), oper, getValue(), getWeight().toString(), sortBaseSocre);
    }

    public boolean match(String[] values) {
        for (String val : values) {
            if (this.getValue().equals(val)) {
                return !Constants.SortValueType.NOT.equals(this.getType());
            }
        }

        if (Constants.SortValueType.NOT.equals(this.getType())) {
            return true;
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
