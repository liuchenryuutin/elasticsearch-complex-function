/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.lccy.elasticsearch.plugin.query;

import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.function.FieldValueFactorFunction;
import org.elasticsearch.common.lucene.search.function.ScoreFunction;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilder;
import org.lccy.elasticsearch.plugin.util.StringUtil;

import java.io.IOException;
import java.util.*;

/**
 * Builder to construct {@code complex_field_score} functions for a function
 * score query.
 *
 * @author liuchen <br>
 * @date 2023-07-08
 */
public class ComplexFieldFunctionBuilder extends ScoreFunctionBuilder<ComplexFieldFunctionBuilder> {
    public static final String NAME = "complex_field_score";
    public static final String DEFAULT_CATEGORY = "categoryCode.keyword";

    private final Double funcScoreFactor;
    private final Double originalScoreFactor;
    private final String categoryField;
    private final Map<String, CategoryScoreBo> categorys;
    private final Map<String, FieldComputeBo> fieldMap;


    public ComplexFieldFunctionBuilder(Double funcScoreFactor, Double originalScoreFactor, Map<String, CategoryScoreBo> categorys,
                                       Map<String, FieldComputeBo> fieldMap, String categoryField) {
        this.funcScoreFactor = funcScoreFactor;
        this.originalScoreFactor = originalScoreFactor;
        this.categorys = categorys;
        this.fieldMap = fieldMap;
        this.categoryField = categoryField;
    }

    /**
     * Read from a stream.
     */
    public ComplexFieldFunctionBuilder(StreamInput in) throws IOException {
        super(in);
        Double funcScoreFactor;
        Double originalScoreFactor;
        final Map<String, CategoryScoreBo> categorys = new HashMap<>();
        final Map<String, FieldComputeBo> fields = new HashMap<>();
        String categoryField;

        Map<String, Object> request = in.readMap();
        if(request == null || request.isEmpty()) {
            throw new IllegalArgumentException(NAME + " query is empty.");
        }
        if((funcScoreFactor = (Double) request.get("func_score_factor")) == null) {
            throw new IllegalArgumentException(NAME + " query must has field [func_score_factor]");
        }
        if((originalScoreFactor = (Double) request.get("original_score_factor")) == null) {
            throw new IllegalArgumentException(NAME + " query must has field [original_score_factor]");
        }
        List<Object> categorysList;
        if((categorysList = (List) request.get("original_score_factor")) == null) {
            throw new IllegalArgumentException(NAME + " query must has field [categorys]");
        }
        categoryField = (String) request.get("category_field");
        if(StringUtil.isEmpty(categoryField)) {
            categoryField = DEFAULT_CATEGORY;
        }
        if(funcScoreFactor < 0 || originalScoreFactor < 0) {
            throw new IllegalArgumentException(NAME + " query param [original_score_factor] or [func_score_factor] must be greater than 0.");
        }
        categorysList.stream().forEach(x -> {
            if(x instanceof Map) {
                final Map<String, Object> cat = (Map) x;
                String catCode = (String) cat.get("name");
                String fieldMode = (String) cat.get("filed_mode");
                List<Map> fieldsScore = (List) cat.get("fields_score");
                String sortMode = (String) cat.get("sort_mode");
                Double sortBaseSocre = (Double) cat.get("sort_base_socre");
                List<Map> sortScore = (List) cat.get("sort_score");
                if(StringUtil.isEmpty(catCode) || StringUtil.isEmpty(fieldMode) || StringUtil.isEmpty(sortMode)
                        || ((fieldsScore == null || fieldsScore.isEmpty()) && (sortScore == null || sortScore.isEmpty()))) {
                    throw new IllegalArgumentException(NAME + " query param [categorys] has error, please check.");
                }
                if(sortScore != null && !sortScore.isEmpty() && (StringUtil.isEmpty(sortMode) || sortBaseSocre == null)) {
                    throw new IllegalArgumentException(NAME + " query param [categorys] [sort_config] must has [sort_mode] and [sort_base_socre].");
                }

                List<FieldComputeBo> fbos = new ArrayList<>();
                for(Map fd : fieldsScore) {
                    String field = (String) fd.get("field");
                    Double factor = (Double) fd.get("factor");
                    String modifier = (String) fd.get("modifier");
                    Double weight = (Double) fd.get("weight");
                    Double addNum = (Double) fd.get("add_num");
                    Double missing = (Double) fd.get("missing");
                    if(StringUtil.isEmpty(field) || StringUtil.isEmpty(modifier)) {
                        throw new IllegalArgumentException(NAME + " query param [categorys] [fields] setting has error, please check.");
                    }
                    FieldComputeBo fbo = new FieldComputeBo().field(field).factor(factor).modifier(modifier);
                    if(weight == null) {
                        fbo.weight(1);
                    } else {
                        fbo.weight(weight);
                    }
                    if(addNum == null) {
                        fbo.addNum(0);
                    } else {
                        fbo.addNum(addNum);
                    }
                    if(missing != null) {
                        fbo.missing(missing);
                    }
                    fbos.add(fbo);
                    fields.put(field, fbo);
                }

                List<SortComputeBo> sbos = new ArrayList<>();
                for(Map st : sortScore) {
                    Integer weight = (Integer) st.get("weight");
                    String field = (String) st.get("field");
                    String type = (String) st.get("type");
                    String value = (String) st.get("value");
                    if(StringUtil.isEmpty(field) || weight == null) {
                        throw new IllegalArgumentException(NAME + " query param [categorys] [fields] has error, please check.");
                    }
                    sbos.add(new SortComputeBo().field(field).weight(weight).value(value).type(type));
                }
                if(Constants.SortMode.MAX.equals(sortMode)) {
                    sbos.sort(Comparator.comparingInt(SortComputeBo::getWeight).reversed());
                } else {
                    sbos.sort(Comparator.comparingInt(SortComputeBo::getWeight));
                }

                for(String code : catCode.split(",")){
                    CategoryScoreBo cbo = new CategoryScoreBo().name(code).fieldMode(fieldMode).sortMode(sortMode).sortBaseSocre(sortBaseSocre);
                    cbo.fieldsScore(fbos);
                    cbo.sortScore(sbos);
                    categorys.put(code, cbo);
                }
            } else {
                throw new IllegalArgumentException(NAME + " query param [categorys] must be Map");
            }
        });

        this.funcScoreFactor = funcScoreFactor;
        this.originalScoreFactor = originalScoreFactor;
        this.categorys = categorys;
        this.fieldMap = fields;
        this.categoryField = categoryField;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeOptionalDouble(funcScoreFactor);
        out.writeOptionalDouble(originalScoreFactor);
        out.writeMap((Map) categorys);
        out.writeString(categoryField);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getName());
        builder.field("func_score_factor", funcScoreFactor);
        builder.field("original_score_factor", originalScoreFactor);
        builder.field("categorys", categorys);
        builder.field("category_field", categoryField);
        builder.endObject();
    }

    @Override
    protected boolean doEquals(ComplexFieldFunctionBuilder functionBuilder) {
        return Objects.equals(this.funcScoreFactor, functionBuilder.funcScoreFactor) &&
                Objects.equals(this.originalScoreFactor, functionBuilder.originalScoreFactor) &&
                Objects.equals(this.categorys, functionBuilder.categorys) &&
                Objects.equals(this.categoryField, functionBuilder.categoryField);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(this.funcScoreFactor, this.originalScoreFactor, this.categorys);
    }

    @Override
    protected ScoreFunction doToFunction(QueryShardContext context) {
        Map<String, IndexFieldData> fieldDataMap = new HashMap<>();
        for(Map.Entry<String, FieldComputeBo> entry : this.fieldMap.entrySet()) {
            MappedFieldType fieldType = context.getMapperService().fullName(entry.getKey());
            IndexFieldData fieldData = null;
            if (fieldType == null) {
                if(entry.getValue().require() && entry.getValue().missing() == null) {
                    throw new ElasticsearchException("Unable to find a field mapper for field [" + entry.getKey() + "]. No 'missing' value defined.");
                }
            } else {
                fieldData = context.getForField(fieldType);
            }
            fieldDataMap.put(entry.getKey(), fieldData);
        }

        MappedFieldType fieldType = context.getMapperService().fullName(DEFAULT_CATEGORY);
        if(fieldType == null) {
            throw new ElasticsearchException("Unable to find a field mapper for field [categoryCode].");
        }
        IndexFieldData fieldData = context.getForField(fieldType);
        fieldDataMap.put("categoryCode", fieldData);

        return new ComplexFieldFunction(funcScoreFactor, originalScoreFactor, categorys, fieldDataMap, categoryField);
    }

    public static ComplexFieldFunctionBuilder fromXContent(XContentParser parser)
            throws IOException, ParsingException {
        Double funcScoreFactor;
        Double originalScoreFactor;
        final Map<String, CategoryScoreBo> categorys = new HashMap<>();
        final Map<String, FieldComputeBo> fields = new HashMap<>();

        Map<String, Object> request = parser.map();
        if(request == null || request.isEmpty()) {
            throw new ParsingException(parser.getTokenLocation(), NAME + " query is empty.");
        }
        if((funcScoreFactor = (Double) request.get("func_score_factor")) == null) {
            throw new ParsingException(parser.getTokenLocation(), NAME + " query must has field [func_score_factor]");
        }
        if((originalScoreFactor = (Double) request.get("original_score_factor")) == null) {
            throw new ParsingException(parser.getTokenLocation(), NAME + " query must has field [original_score_factor]");
        }
        List<Object> categorysList;
        if((categorysList = (List) request.get("original_score_factor")) == null) {
            throw new ParsingException(parser.getTokenLocation(), NAME + " query must has field [categorys]");
        }

        String categoryField = (String) request.get("category_field");
        if(StringUtil.isEmpty(categoryField)) {
            categoryField = DEFAULT_CATEGORY;
        }
        if(funcScoreFactor < 0 || originalScoreFactor < 0) {
            throw new ParsingException(parser.getTokenLocation(), NAME + " query param [original_score_factor] or [func_score_factor] must be greater than 0.");
        }
        categorysList.stream().forEach(x -> {
            if(x instanceof Map) {
                final Map<String, Object> cat = (Map) x;
                String catCode = (String) cat.get("name");
                String fieldMode = (String) cat.get("filed_mode");
                List<Map> fieldsScore = (List) cat.get("fields_score");
                String sortMode = (String) cat.get("sort_mode");
                Double sortBaseSocre = (Double) cat.get("sort_base_socre");
                List<Map> sortScore = (List) cat.get("sort_score");
                if(StringUtil.isEmpty(catCode) || StringUtil.isEmpty(fieldMode) || StringUtil.isEmpty(sortMode)
                        || ((fieldsScore == null || fieldsScore.isEmpty()) && (sortScore == null || sortScore.isEmpty()))) {
                    throw new ParsingException(parser.getTokenLocation(), NAME + " query param [categorys] has error, please check.");
                }
                if(sortScore != null && !sortScore.isEmpty() && (StringUtil.isEmpty(sortMode) || sortBaseSocre == null)) {
                    throw new ParsingException(parser.getTokenLocation(), NAME + " query param [categorys] [sort_config] must has [sort_mode] and [sort_base_socre].");
                }

                List<FieldComputeBo> fbos = new ArrayList<>();
                for(Map fd : fieldsScore) {
                    String field = (String) fd.get("field");
                    Double factor = (Double) fd.get("factor");
                    String modifier = (String) fd.get("modifier");
                    Double weight = (Double) fd.get("weight");
                    Double addNum = (Double) fd.get("add_num");
                    Double missing = (Double) fd.get("missing");
                    Boolean require = (Boolean) fd.get("require");
                    if(StringUtil.isEmpty(field) || StringUtil.isEmpty(modifier)) {
                        throw new ParsingException(parser.getTokenLocation(), NAME + " query param [categorys] [fields] setting has error, please check.");
                    }
                    FieldComputeBo fbo = new FieldComputeBo().field(field).factor(factor).modifier(modifier).require(require).missing(missing);
                    if(weight == null) {
                        fbo.weight(1);
                    } else {
                        fbo.weight(weight);
                    }
                    if(addNum == null) {
                        fbo.addNum(0);
                    } else {
                        fbo.addNum(addNum);
                    }
                    fbos.add(fbo);
                    fields.put(field, fbo);
                }

                List<SortComputeBo> sbos = new ArrayList<>();
                for(Map st : sortScore) {
                    Integer weight = (Integer) st.get("weight");
                    String field = (String) st.get("field");
                    String type = (String) st.get("type");
                    String value = (String) st.get("value");
                    if(StringUtil.isEmpty(field) || weight == null) {
                        throw new ParsingException(parser.getTokenLocation(), NAME + " query param [categorys] [fields] has error, please check.");
                    }
                    sbos.add(new SortComputeBo().field(field).weight(weight).value(value).type(type));
                }
                sbos.sort(Comparator.comparingInt(SortComputeBo::weight));
                for(String code : catCode.split(",")){
                    CategoryScoreBo cbo = new CategoryScoreBo().name(code).fieldMode(fieldMode).sortMode(sortMode).sortBaseSocre(sortBaseSocre);
                    cbo.fieldsScore(fbos);
                    cbo.sortScore(sbos);
                    categorys.put(code, cbo);
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(), NAME + " query param [categorys] must be Map");
            }
        });

        ComplexFieldFunctionBuilder complexFieldFunctionBuilder = new ComplexFieldFunctionBuilder(funcScoreFactor, originalScoreFactor
                , categorys, fields, categoryField);
        return complexFieldFunctionBuilder;
    }
}
