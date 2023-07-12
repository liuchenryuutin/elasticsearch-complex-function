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
import org.lccy.elasticsearch.plugin.function.bo.CategoryScoreWapper;
import org.lccy.elasticsearch.plugin.function.bo.SortScoreComputeWapper;
import org.lccy.elasticsearch.plugin.util.CommonUtil;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

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
    private final Map<String, CategoryScoreWapper> categorys;
    private Map<String, Boolean> fieldMap;

    public ComplexFieldFunctionBuilder(Double funcScoreFactor, Double originalScoreFactor, Map<String, CategoryScoreWapper> categorys
            , String categoryField, Map<String, Boolean> fieldMap) {
        if (funcScoreFactor == null || funcScoreFactor < 0 || originalScoreFactor == null || originalScoreFactor < 0
                || categorys == null || categorys.isEmpty() || CommonUtil.isEmpty(categoryField)) {
            throw new IllegalArgumentException("require param is not set, please check.");
        }
        this.funcScoreFactor = funcScoreFactor;
        this.originalScoreFactor = originalScoreFactor;
        this.categorys = categorys;
        this.categoryField = categoryField;
        this.fieldMap = fieldMap;
    }

    public ComplexFieldFunctionBuilder(Double funcScoreFactor, Double originalScoreFactor, Map<String, CategoryScoreWapper> categorys
            , String categoryField) {
        if (funcScoreFactor == null || funcScoreFactor < 0 || originalScoreFactor == null || originalScoreFactor < 0
                || categorys == null || categorys.isEmpty() || CommonUtil.isEmpty(categoryField)) {
            throw new IllegalArgumentException("require param is not set, please check.");
        }
        this.funcScoreFactor = funcScoreFactor;
        this.originalScoreFactor = originalScoreFactor;
        this.categorys = categorys;
        this.categoryField = categoryField;
    }

    /**
     * Read from a stream.
     */
    public ComplexFieldFunctionBuilder(StreamInput in) throws IOException {
        super(in);
        Double funcScoreFactor;
        Double originalScoreFactor;
        final Map<String, CategoryScoreWapper> categorys = new HashMap<>();
        final Map<String, Boolean> fields = new HashMap<>();

        Map<String, Object> request = in.readMap();
        if (request == null || request.isEmpty()) {
            throw new IllegalArgumentException(NAME + " query is empty.");
        }
        if (request.get("func_score_factor") == null) {
            throw new IllegalArgumentException(NAME + " query must has field [func_score_factor]");
        } else {
            funcScoreFactor = Double.parseDouble(request.get("func_score_factor").toString());
        }
        if (request.get("original_score_factor") == null) {
            throw new IllegalArgumentException(NAME + " query must has field [original_score_factor]");
        } else {
            originalScoreFactor = Double.parseDouble(request.get("original_score_factor").toString());
        }
        if (funcScoreFactor < 0 || originalScoreFactor < 0) {
            throw new IllegalArgumentException(NAME + " query param [original_score_factor] or [func_score_factor] must be greater than 0.");
        }

        List<Object> categorysList = (List) request.get("categorys");
        if (categorysList == null || categorysList.isEmpty()) {
            throw new IllegalArgumentException(NAME + " query must has field [categorys]");
        }

        String categoryField = (String) request.get("category_field");
        if (CommonUtil.isEmpty(categoryField)) {
            categoryField = DEFAULT_CATEGORY;
        }
        categorysList.stream().forEach(x -> {
            if (x instanceof Map) {
                final Map<String, Object> cat = (Map) x;
                CategoryScoreWapper csw = new CategoryScoreWapper(null, cat);

                if (Constants.SortMode.MAX.equals(csw.getSortMode())) {
                    csw.getScoreComputeWappers().sort(Comparator.comparingInt(SortScoreComputeWapper::getWeight).reversed());
                } else {
                    csw.getScoreComputeWappers().sort(Comparator.comparingInt(SortScoreComputeWapper::getWeight));
                }
                for (String code : csw.getName().split(",")) {
                    categorys.put(code, csw);
                }
                fields.putAll(csw.getAllFiled());
            } else {
                throw new IllegalArgumentException(NAME + " query param [categorys] must be Map");
            }
        });
        fields.put(categoryField, true);

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
        for (CategoryScoreWapper x : categorys.values()) {
            out.writeMap(x.unwrap());
        }
        out.writeOptionalString(categoryField);
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
        builder.field("categorys", categorys.values().stream().map(x -> x.unwrap()).distinct().collect(Collectors.toList()));
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
        return Objects.hash(this.funcScoreFactor, this.originalScoreFactor, this.categorys, this.categoryField);
    }

    @Override
    protected ScoreFunction doToFunction(QueryShardContext context) {
        Map<String, IndexFieldData> fieldDataMap = new HashMap<>();
        for (Map.Entry<String, Boolean> entry : this.fieldMap.entrySet()) {
            MappedFieldType fieldType = context.getMapperService().fullName(entry.getKey());
            if (fieldType == null) {
                if (entry.getValue()) {
                    throw new ElasticsearchException("Unable to find a field mapper for field [" + entry.getKey() + "]. No 'missing' value defined.");
                }
            } else {
                IndexFieldData fieldData = context.getForField(fieldType);
                if (fieldData == null) {
                    if (entry.getValue()) {
                        throw new ElasticsearchException("Unable to find a field mapper for field [" + entry.getKey() + "]. No 'missing' value defined.");
                    }
                } else {
                    fieldDataMap.put(entry.getKey(), fieldData);
                }
            }
        }

        return new ComplexFieldFunction(funcScoreFactor, originalScoreFactor, categorys, fieldDataMap, categoryField);
    }

    public static ComplexFieldFunctionBuilder fromXContent(XContentParser parser)
            throws IOException, ParsingException {
        Double funcScoreFactor;
        Double originalScoreFactor;
        final Map<String, CategoryScoreWapper> categorys = new HashMap<>();
        final Map<String, Boolean> fields = new HashMap<>();

        Map<String, Object> request = parser.map();
        if (request == null || request.isEmpty()) {
            throw new ParsingException(parser.getTokenLocation(), NAME + " query is empty.");
        }
        if (request.get("func_score_factor") == null) {
            throw new ParsingException(parser.getTokenLocation(), NAME + " query must has field [func_score_factor]");
        } else {
            funcScoreFactor = Double.parseDouble(request.get("func_score_factor").toString());
        }
        if (request.get("original_score_factor") == null) {
            throw new ParsingException(parser.getTokenLocation(), NAME + " query must has field [original_score_factor]");
        } else {
            originalScoreFactor = Double.parseDouble(request.get("original_score_factor").toString());
        }
        if (funcScoreFactor < 0 || originalScoreFactor < 0) {
            throw new ParsingException(parser.getTokenLocation(), NAME + " query param [original_score_factor] or [func_score_factor] must be greater than 0.");
        }

        List<Object> categorysList = (List) request.get("categorys");
        if (categorysList == null || categorysList.isEmpty()) {
            throw new ParsingException(parser.getTokenLocation(), NAME + " query must has field [categorys]");
        }

        String categoryField = (String) request.get("category_field");
        if (CommonUtil.isEmpty(categoryField)) {
            categoryField = DEFAULT_CATEGORY;
        }
        categorysList.stream().forEach(x -> {
            if (x instanceof Map) {
                final Map<String, Object> cat = (Map) x;
                CategoryScoreWapper csw = new CategoryScoreWapper(parser, cat);

                if (Constants.SortMode.MAX.equals(csw.getSortMode())) {
                    csw.getScoreComputeWappers().sort(Comparator.comparingInt(SortScoreComputeWapper::getWeight).reversed());
                } else {
                    csw.getScoreComputeWappers().sort(Comparator.comparingInt(SortScoreComputeWapper::getWeight));
                }
                for (String code : csw.getName().split(",")) {
                    categorys.put(code, csw);
                }
                fields.putAll(csw.getAllFiled());
            } else {
                throw new ParsingException(parser.getTokenLocation(), NAME + " query param [categorys] must be Map");
            }
        });

        fields.put(categoryField, true);
        ComplexFieldFunctionBuilder complexFieldFunctionBuilder = new ComplexFieldFunctionBuilder(funcScoreFactor, originalScoreFactor
                , categorys, categoryField, fields);
        return complexFieldFunctionBuilder;
    }
}
