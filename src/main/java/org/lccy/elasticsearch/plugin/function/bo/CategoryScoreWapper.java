package org.lccy.elasticsearch.plugin.function.bo;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.lccy.elasticsearch.plugin.function.ComplexFieldFunction;
import org.lccy.elasticsearch.plugin.function.ComplexFieldFunctionBuilder;
import org.lccy.elasticsearch.plugin.function.Constants;
import org.lccy.elasticsearch.plugin.util.StringUtil;

import java.util.*;

/**
 * wrap category query param <br>
 *
 * @Date: 2023/07/11 09:18 <br>
 * @author: liuchen
 */
public class CategoryScoreWapper {

    public static final String NAME = "name";
    public static final String FILED_MODE = "filed_mode";
    public static final String FIELDS_SCORE = "fields_score";
    public static final String SORT_MODE = "sort_mode";
    public static final String SORT_BASE_SCORE = "sort_base_score";
    public static final String SORT_SCORE = "sort_score";

    // wrap data
    private Map<String, Object> categorys;

    private List<FieldScoreComputeWapper> fieldScoreWappers;
    private List<SortScoreComputeWapper> scoreComputeWappers;

    public CategoryScoreWapper(XContentParser parser, Map<String, Object> categorys) {
        String catCode = (String) categorys.get(NAME);
        String fieldMode = (String) categorys.get(FILED_MODE);
        List<Map> fieldsScore = (List) categorys.get(FIELDS_SCORE);
        String sortMode = (String) categorys.get(SORT_MODE);
        Double sortBaseSocre = categorys.get(SORT_BASE_SCORE) == null ? null : Double.parseDouble(categorys.get(SORT_BASE_SCORE).toString());
        List<Map> sortScore = (List) categorys.get(SORT_SCORE);
        if (StringUtil.isEmpty(catCode) || StringUtil.isEmpty(fieldMode) || StringUtil.isEmpty(sortMode)
                || ((fieldsScore == null || fieldsScore.isEmpty()) && (sortScore == null || sortScore.isEmpty()))) {
            throwsException(parser, ComplexFieldFunctionBuilder.NAME + " query param [categorys] set error, please check.");
        }
        if (sortScore != null && !sortScore.isEmpty() && (StringUtil.isEmpty(sortMode) || sortBaseSocre == null)) {
            throwsException(parser, NAME + " query param [categorys.sort_config] must has [sort_mode] and [sort_base_score].");
        }

        if(fieldsScore != null && !fieldsScore.isEmpty()) {
            fieldScoreWappers = new ArrayList<>();
        }
        for (Map fd : fieldsScore) {
            FieldScoreComputeWapper fscw = new FieldScoreComputeWapper(parser, fd);
            fieldScoreWappers.add(fscw);
        }

        if(sortScore != null && !sortScore.isEmpty()) {
            scoreComputeWappers = new ArrayList<>();
        }
        for (Map st : sortScore) {
            SortScoreComputeWapper sscw = new SortScoreComputeWapper(parser, st);
            scoreComputeWappers.add(sscw);
        }

        this.categorys = categorys;
    }


    public String getName() {
        return (String) categorys.get(NAME);
    }

    public String getFieldMode() {
        return (String) categorys.get(FILED_MODE);
    }

    public String getSortMode() {
        return (String) categorys.get(SORT_MODE);
    }

    public Double getSortBaseScore() {
        return Double.parseDouble(categorys.get(SORT_BASE_SCORE).toString());
    }

    public Map<String, Object> unwrap() {
        return categorys;
    }

    public List<FieldScoreComputeWapper> getFieldScoreWappers() {
        return fieldScoreWappers;
    }

    public List<SortScoreComputeWapper> getScoreComputeWappers() {
        return scoreComputeWappers;
    }

    public Map<String, Boolean> getAllFiled() {
        Map<String, Boolean> result = new HashMap<>();
        if(fieldScoreWappers != null && !fieldScoreWappers.isEmpty()) {
            fieldScoreWappers.forEach(x -> result.put(x.getField(), x.getRequire() && x.getMissing() == null));
        }

        if(scoreComputeWappers != null && !scoreComputeWappers.isEmpty()) {
            scoreComputeWappers.forEach(x -> result.put(x.getField(), false));
        }
        return result;
    }

    private void throwsException(XContentParser parser, String msg) {
        if(parser != null) {
            throw new ParsingException(parser.getTokenLocation(), msg);
        } else {
            throw new IllegalArgumentException(msg);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CategoryScoreWapper that = (CategoryScoreWapper) o;
        return Objects.equals(categorys, that.categorys);
    }

    @Override
    public int hashCode() {
        return Objects.hash(categorys);
    }
}
