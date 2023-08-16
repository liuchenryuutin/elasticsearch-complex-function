package org.lccy.elasticsearch.plugin.function.bo;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.lccy.elasticsearch.plugin.function.ComplexFieldFunctionBuilder;
import org.lccy.elasticsearch.plugin.util.CommonUtil;

import java.util.*;

/**
 * wrap category query param <br>
 *
 * @author liuchen <br>
 * @date 2023-07-11
 */
public class CategoryScoreWapper {

    public static final String CATEGORY_FIELD = "category_field";
    public static final String FUNC_SCORE_FACTOR = "func_score_factor";
    public static final String ORIGINAL_SCORE_FACTOR = "original_score_factor";
    public static final String FILED_MODE = "filed_mode";
    public static final String FIELDS_SCORE = "fields_score";
    public static final String SORT_BASE_SCORE = "sort_base_score";
    public static final String SORT_SCORE = "sort_score";
    // wrap data
    private Map<String, Object> categorys;

    private Double funcScoreFactor;
    private Double originalScoreFactor;
    private String categoryField;
    private String fieldMode;
    private Double sortBaseScore;

    private Map<String, List<FieldScoreComputeWapper>> fieldScoreWapperMap;
    private Map<String, List<SortScoreComputeWapper>> scoreComputeWapperMap;
    private Map<String, Boolean> allFiled;

    public CategoryScoreWapper(XContentParser parser, Map<String, Object> categorys) {
        if (categorys.get(FUNC_SCORE_FACTOR) == null) {
            throwsException(parser, ComplexFieldFunctionBuilder.NAME + " query must has field [func_score_factor]");
        } else {
            funcScoreFactor = Double.parseDouble(categorys.get("func_score_factor").toString());
        }
        if (categorys.get(ORIGINAL_SCORE_FACTOR) == null) {
            throwsException(parser, ComplexFieldFunctionBuilder.NAME + " query must has field [original_score_factor]");
        } else {
            originalScoreFactor = Double.parseDouble(categorys.get("original_score_factor").toString());
        }
        if (funcScoreFactor < 0 || originalScoreFactor < 0) {
            throwsException(parser, ComplexFieldFunctionBuilder.NAME + " query param [original_score_factor] or [func_score_factor] must be greater than 0.");
        }
        if(categorys.get(CATEGORY_FIELD) == null) {
            throwsException(parser, ComplexFieldFunctionBuilder.NAME + " query must has field [category_field]");
        } else {
            categoryField = CommonUtil.toString(categorys.get("category_field"));
        }

        String fieldMode = CommonUtil.toString(categorys.get(FILED_MODE));
        Map<String, Object> fieldsScore = (Map<String, Object>) categorys.get(FIELDS_SCORE);
        Double sortBaseScore = categorys.get(SORT_BASE_SCORE) == null ? null : Double.parseDouble(categorys.get(SORT_BASE_SCORE).toString());
        Map<String, Object> sortScore = (Map<String, Object>) categorys.get(SORT_SCORE);
        if(CommonUtil.isEmpty(fieldsScore) && CommonUtil.isEmpty(sortScore)) {
            throwsException(parser, ComplexFieldFunctionBuilder.NAME + " query must has [name] and [fields_score] or [sort_score], please check.");
        }
        if(!CommonUtil.isEmpty(fieldsScore) && CommonUtil.isEmpty(fieldMode)) {
            throwsException(parser, ComplexFieldFunctionBuilder.NAME + " query param [fields_score] must has sibling element [filed_mode], please check.");
        }
        if(!CommonUtil.isEmpty(sortScore) && sortBaseScore == null) {
            throwsException(parser, ComplexFieldFunctionBuilder.NAME + " query param [sort_score] must has sibling element [sort_base_score], please check.");
        }
        this.fieldMode = fieldMode;
        this.sortBaseScore = sortBaseScore;

        this.allFiled = new HashMap<>();
        this.allFiled.put(categoryField, true);
        if (!CommonUtil.isEmpty(fieldsScore)) {
            this.fieldScoreWapperMap = new HashMap<>();
            for (String cateCodes : fieldsScore.keySet()) {
                List<Map> value = (List<Map>) fieldsScore.get(cateCodes);
                if(CommonUtil.isEmpty(value)) {
                    throwsException(parser, ComplexFieldFunctionBuilder.NAME + " query param [fields_score] must has attributes, please check.");
                }
                List<FieldScoreComputeWapper> fieldScoreComputeWappers = new ArrayList<>();
                value.stream().forEach(x -> {
                    FieldScoreComputeWapper fscw = new FieldScoreComputeWapper(parser, x);
                    fieldScoreComputeWappers.add(fscw);
                    this.allFiled.put(fscw.getField(), fscw.getRequire() && fscw.getMissing() == null);
                });

                for (String cateCode : cateCodes.split(",", -1)) {
                    this.fieldScoreWapperMap.put(cateCode, fieldScoreComputeWappers);
                }
            }
        }

        if (!CommonUtil.isEmpty(sortScore)) {
            this.scoreComputeWapperMap = new HashMap<>();
            for (String cateCodes : sortScore.keySet()) {
                List<Map> value = (List<Map>) sortScore.get(cateCodes);
                if(CommonUtil.isEmpty(value)) {
                    throwsException(parser, ComplexFieldFunctionBuilder.NAME + " query param [sort_score] must has attributes, please check.");
                }
                List<SortScoreComputeWapper> scoreComputeWappers = new ArrayList<>();
                value.stream().forEach(x -> {
                    SortScoreComputeWapper sscw = new SortScoreComputeWapper(parser, x);
                    scoreComputeWappers.add(sscw);
                    this.allFiled.put(sscw.getField(), false);
                });

                if(!CommonUtil.isEmpty(scoreComputeWappers)) {
                    scoreComputeWappers.sort(Comparator.comparingInt(SortScoreComputeWapper::getWeight).reversed());
                }

                for (String cateCode : cateCodes.split(",", -1)) {
                    this.scoreComputeWapperMap.put(cateCode, scoreComputeWappers);
                }
            }
        }

        this.categorys = categorys;
    }

    public Double getFuncScoreFactor() {
        return funcScoreFactor;
    }

    public Double getOriginalScoreFactor() {
        return originalScoreFactor;
    }

    public String getCategoryField() {
        return categoryField;
    }

    public String getFieldMode() {
        return fieldMode;
    }

    public Double getSortBaseScore() {
        return sortBaseScore;
    }

    public Map<String, Object> unwrap() {
        return categorys;
    }

    public List<FieldScoreComputeWapper> getFieldScoreWappers(String cateCode) {
        return fieldScoreWapperMap == null ? null : fieldScoreWapperMap.get(cateCode);
    }

    public List<SortScoreComputeWapper> getScoreComputeWappers(String cateCode) {
        return scoreComputeWapperMap == null ? null : scoreComputeWapperMap.get(cateCode);
    }

    public Map<String, Boolean> getAllFiled() {
        return allFiled;
    }

    private void throwsException(XContentParser parser, String msg) {
        if (parser != null) {
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
