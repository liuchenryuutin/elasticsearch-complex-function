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

package org.lccy.elasticsearch.plugin.function;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.function.ScoreFunction;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilder;
import org.lccy.elasticsearch.plugin.function.bo.CategoryScoreBo;
import org.lccy.elasticsearch.plugin.function.bo.FieldComputeBo;
import org.lccy.elasticsearch.plugin.function.bo.SortComputeBo;
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
        if(funcScoreFactor == null || funcScoreFactor < 0|| originalScoreFactor == null || originalScoreFactor < 0
                || categorys == null || categorys.isEmpty() || StringUtil.isEmpty(categoryField)) {
            throw new IllegalArgumentException("error params, please check.");
        }
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

        Map<String, Object> request = in.readMap();
        if(request == null || request.isEmpty()) {
            throw new IllegalArgumentException(NAME + " query is empty.");
        }
        if(request.get("func_score_factor") == null) {
            throw new IllegalArgumentException(NAME + " query must has field [func_score_factor]");
        } else {
            funcScoreFactor = Double.parseDouble(request.get("func_score_factor").toString());
        }
        if(request.get("original_score_factor") == null) {
            throw new IllegalArgumentException(NAME + " query must has field [original_score_factor]");
        } else {
            originalScoreFactor = Double.parseDouble(request.get("original_score_factor").toString());
        }
        List<Object> categorysList;
        if((categorysList = (List) request.get("categorys")) == null) {
            throw new IllegalArgumentException(NAME + " query must has field [categorys]");
        }
        if(funcScoreFactor < 0 || originalScoreFactor < 0) {
            throw new IllegalArgumentException(NAME + " query param [original_score_factor] or [func_score_factor] must be greater than 0.");
        }

        String categoryField = (String) request.get("category_field");
        if(StringUtil.isEmpty(categoryField)) {
            categoryField = DEFAULT_CATEGORY;
        }
        fields.put(categoryField, new FieldComputeBo().setField(categoryField).setRequire(true).setMissing(null));
        categorysList.stream().forEach(x -> {
            if(x instanceof Map) {
                final Map<String, Object> cat = (Map) x;
                String catCode = (String) cat.get("name");
                String fieldMode = (String) cat.get("filed_mode");
                List<Map> fieldsScore = (List) cat.get("fields_score");
                String sortMode = (String) cat.get("sort_mode");
                Double sortBaseSocre = cat.get("sort_base_socre") == null ? null : Double.parseDouble(cat.get("sort_base_socre").toString());
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
                    Double factor = fd.get("factor") == null ? null : Double.parseDouble(fd.get("factor").toString());
                    String modifier = (String) fd.get("modifier");
                    Double weight = fd.get("weight") == null ? null : Double.parseDouble(fd.get("weight").toString());
                    Double addNum = fd.get("add_num") == null ? null : Double.parseDouble(fd.get("add_num").toString());
                    Double missing = fd.get("missing") == null ? null : Double.parseDouble(fd.get("missing").toString());
                    Boolean require = (Boolean) fd.get("require");
                    if(StringUtil.isEmpty(field) || StringUtil.isEmpty(modifier)) {
                        throw new IllegalArgumentException(NAME + " query param [categorys] [fields] setting has error, please check.");
                    }
                    FieldComputeBo fbo = new FieldComputeBo()
                            .setField(field)
                            .setFactor(factor)
                            .setModifier(modifier)
                            .setMissing(missing)
                            .setRequire(require == null ? false : require);
                    if(weight == null) {
                        fbo.setWeight(1);
                    } else {
                        fbo.setWeight(weight);
                    }
                    if(addNum == null) {
                        fbo.setAddNum(0);
                    } else {
                        fbo.setAddNum(addNum);
                    }
                    fbos.add(fbo);
                    fields.put(field, fbo);
                }

                List<SortComputeBo> sbos = new ArrayList<>();
                for(Map st : sortScore) {
                    Integer weight = st.get("weight") == null ? null : Integer.parseInt(st.get("weight").toString());
                    String field = (String) st.get("field");
                    String type = (String) st.get("type");
                    String value = (String) st.get("value");
                    if(StringUtil.isEmpty(field) || weight == null) {
                        throw new IllegalArgumentException(NAME + " query param [categorys] [fields] has error, please check.");
                    }
                    sbos.add(new SortComputeBo().setField(field).setWeight(weight).setValue(value).setType(type));

                    fields.put(field, new FieldComputeBo().setField(field).setRequire(false));
                }
                if(Constants.SortMode.MAX.equals(sortMode)) {
                    sbos.sort(Comparator.comparingInt(SortComputeBo::getWeight).reversed());
                } else {
                    sbos.sort(Comparator.comparingInt(SortComputeBo::getWeight));
                }

                for(String code : catCode.split(",")){
                    CategoryScoreBo cbo = new CategoryScoreBo()
                            .setName(code).setFieldMode(fieldMode).setSortMode(sortMode).setSortBaseSocre(sortBaseSocre)
                            .setFieldsScore(fbos).setSortScore(sbos);
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
                if(entry.getValue().getRequire() && entry.getValue().getMissing() == null) {
                    throw new ElasticsearchException("Unable to find a field mapper for field [" + entry.getKey() + "]. No 'missing' value defined.");
                }
            } else {
                fieldData = context.getForField(fieldType);
            }
            fieldDataMap.put(entry.getKey(), fieldData);
        }

//        MappedFieldType fieldType = context.getMapperService().fullName(this.categoryField);
//        if(fieldType == null) {
//            throw new ElasticsearchException("Unable to find a field mapper for field [" + this.categoryField + "].");
//        }
//        IndexFieldData fieldData = context.getForField(fieldType);
//        fieldDataMap.put(this.categoryField, fieldData);

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
        if(request.get("func_score_factor") == null) {
            throw new ParsingException(parser.getTokenLocation(), NAME + " query must has field [func_score_factor]");
        } else {
            funcScoreFactor = Double.parseDouble(request.get("func_score_factor").toString());
        }
        if(request.get("original_score_factor") == null) {
            throw new ParsingException(parser.getTokenLocation(), NAME + " query must has field [original_score_factor]");
        } else {
            originalScoreFactor = Double.parseDouble(request.get("original_score_factor").toString());
        }
        List<Object> categorysList;
        if((categorysList = (List) request.get("categorys")) == null) {
            throw new ParsingException(parser.getTokenLocation(), NAME + " query must has field [categorys]");
        }

        if(funcScoreFactor < 0 || originalScoreFactor < 0) {
            throw new ParsingException(parser.getTokenLocation(), NAME + " query param [original_score_factor] or [func_score_factor] must be greater than 0.");
        }

        String categoryField = (String) request.get("category_field");
        if(StringUtil.isEmpty(categoryField)) {
            categoryField = DEFAULT_CATEGORY;
        }
        fields.put(categoryField, new FieldComputeBo().setField(categoryField).setRequire(true).setMissing(null));
        categorysList.stream().forEach(x -> {
            if(x instanceof Map) {
                final Map<String, Object> cat = (Map) x;
                String catCode = (String) cat.get("name");
                String fieldMode = (String) cat.get("filed_mode");
                List<Map> fieldsScore = (List) cat.get("fields_score");
                String sortMode = (String) cat.get("sort_mode");
                Double sortBaseSocre = cat.get("sort_base_socre") == null ? null : Double.parseDouble(cat.get("sort_base_socre").toString());
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
                    Double factor = fd.get("factor") == null ? null : Double.parseDouble(fd.get("factor").toString());
                    String modifier = (String) fd.get("modifier");
                    Double weight = fd.get("weight") == null ? null : Double.parseDouble(fd.get("weight").toString());
                    Double addNum = fd.get("add_num") == null ? null : Double.parseDouble(fd.get("add_num").toString());
                    Double missing = fd.get("missing") == null ? null : Double.parseDouble(fd.get("missing").toString());
                    Boolean require = (Boolean) fd.get("require");
                    if(StringUtil.isEmpty(field) || StringUtil.isEmpty(modifier)) {
                        throw new ParsingException(parser.getTokenLocation(), NAME + " query param [categorys] [fields_score] setting has error, please check.");
                    }
                    FieldComputeBo fbo = new FieldComputeBo()
                            .setField(field)
                            .setFactor(factor)
                            .setModifier(modifier)
                            .setMissing(missing)
                            .setRequire(require == null ? false : require);
                    if(weight == null) {
                        fbo.setWeight(1);
                    } else {
                        fbo.setWeight(weight);
                    }
                    if(addNum == null) {
                        fbo.setAddNum(0);
                    } else {
                        fbo.setAddNum(addNum);
                    }
                    fbos.add(fbo);
                    fields.put(field, fbo);
                }

                List<SortComputeBo> sbos = new ArrayList<>();
                for(Map st : sortScore) {
                    Integer weight = st.get("weight") == null ? null : Integer.parseInt(st.get("weight").toString());
                    String field = (String) st.get("field");
                    String type = (String) st.get("type");
                    String value = (String) st.get("value");
                    if(StringUtil.isEmpty(field) || weight == null) {
                        throw new ParsingException(parser.getTokenLocation(), NAME + " query param [categorys] [sort_score] setting has error, please check.");
                    }
                    sbos.add(new SortComputeBo().setField(field).setWeight(weight).setValue(value).setType(type));

                    fields.put(field, new FieldComputeBo().setField(field).setRequire(false));
                }
                if(Constants.SortMode.MAX.equals(sortMode)) {
                    sbos.sort(Comparator.comparingInt(SortComputeBo::getWeight).reversed());
                } else {
                    sbos.sort(Comparator.comparingInt(SortComputeBo::getWeight));
                }
                for(String code : catCode.split(",")){
                    CategoryScoreBo cbo = new CategoryScoreBo().setName(code).setFieldMode(fieldMode).setSortMode(sortMode)
                            .setSortBaseSocre(sortBaseSocre).setFieldsScore(fbos).setSortScore(sbos);
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
