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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.Explanation;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.LeafScoreFunction;
import org.elasticsearch.common.lucene.search.function.ScoreFunction;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.fielddata.plain.ConstantIndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedSetDVOrdinalsIndexFieldData;
import org.lccy.elasticsearch.plugin.util.StringUtil;

import java.io.IOException;
import java.util.*;

/**
 * A complex_field_score function that multiplies the score with the param settings.
 *
 * @author liuchen <br>
 * @date 2023-07-08
 */
public class ComplexFieldFunction extends ScoreFunction {
    private final double funcScoreFactor;
    private final double originalScoreFactor;
    private final String categoryField;
    private final Map<String, CategoryScoreBo> categorys;
    private final Map<String, IndexFieldData> fieldMap;

    public ComplexFieldFunction(double funcScoreFactor, double originalScoreFactor, Map<String, CategoryScoreBo> categorys,
                                Map<String, IndexFieldData> fieldMap, String categoryField) {
        super(CombineFunction.MULTIPLY);
        this.funcScoreFactor = funcScoreFactor;
        this.originalScoreFactor = originalScoreFactor;
        this.categorys = categorys;
        this.fieldMap = fieldMap;
        this.categoryField = categoryField;
    }

    @Override
    public LeafScoreFunction getLeafScoreFunction(LeafReaderContext ctx) {
        final Map<String, Object> fieldDataMap = new HashMap<>();
        for(Map.Entry<String, IndexFieldData> entry : fieldMap.entrySet()) {
            IndexFieldData val = entry.getValue();
            String field = entry.getKey();
            if(val == null) {
                fieldDataMap.put(field, null);
            } else {
                if(val instanceof SortedSetDVOrdinalsIndexFieldData) {
                    fieldDataMap.put(field, ((SortedSetDVOrdinalsIndexFieldData) val).load(ctx).getOrdinalsValues());
                } else if(val instanceof IndexNumericFieldData) {
                    fieldDataMap.put(field, ((IndexNumericFieldData) val).load(ctx).getDoubleValues());
                } else {
                    throw new ElasticsearchException("Not support mapping type for field [" + field + "], type:" + val.getClass());
                }
            }
        }

        return new LeafScoreFunction() {

            @Override
            public double score(int docId, float subQueryScore) throws IOException {
                String categoryCode = getStrVal(docId, (SortedSetDocValues) fieldDataMap.get(categoryField));
                CategoryScoreBo cbo;
                if(StringUtil.isEmpty(categoryCode) || (cbo = categorys.get(categoryCode)) == null) {
                    return subQueryScore;
                }

                double fieldScoreTotal = 0;
                if(cbo.fieldsScore() != null && !cbo.fieldsScore().isEmpty()) {
                    String fieldMode = cbo.fieldMode();
                    for(FieldComputeBo fbo : cbo.fieldsScore()) {
                        // get field value.
                        Double fVal = getDoubleVal(docId, (SortedNumericDoubleValues) fieldDataMap.get(fbo.field()));
                        if(fVal == null) {
                            if(!fbo.require()) {
                                continue;
                            } else if(fbo.require() && fbo.missing() == null){
                                throw new IllegalArgumentException("require field " + fbo.field() + "must has a value or has a missing value");
                            } else {
                                fVal = fbo.missing();
                            }
                        }
                        mergeFieldScore(fieldMode, fieldScoreTotal, fbo.computeScore(fVal));
                    }
                }

                double sortScoreTotal = 0;
                if(cbo.sortScore() != null && !cbo.sortScore().isEmpty()) {
                    String sortMode = cbo.sortMode();
                    double sortBaseScore = cbo.sortBaseSocre();
                    for(SortComputeBo sbo : cbo.sortScore()) {
                        String[] fVal = getStrValArray(docId, (SortedSetDocValues) fieldDataMap.get(sbo.field()));
                        if(fVal == null) {
                            continue;
                        }
                        if(sbo.match(fVal)) {
                            mergeSortScore(sortMode, sortScoreTotal, sbo.weight() * sortBaseScore);
                            break;
                        }
                    }
                }

                return funcScoreFactor * (fieldScoreTotal + sortScoreTotal) + originalScoreFactor * subQueryScore;
            }

            @Override
            public Explanation explainScore(int docId, Explanation subQueryScore) throws IOException {
                double score = score(docId, subQueryScore.getValue().floatValue());
                return Explanation.match(
                        (float) score,
                        String.format(Locale.ROOT,
                                "complex_field_score function: subQueryScore: %f, complexScore: %f", subQueryScore.getValue().floatValue(), score));
            }
        };
    }

    public String getStrVal(int docId, SortedSetDocValues values) throws IOException{
        long next;
        if(values.advanceExact(docId) && (next = values.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
            return values.lookupOrd(next).utf8ToString();
        } else {
            return null;
        }
    }

    public String[] getStrValArray(int docId, SortedSetDocValues values) throws IOException{
        if(values.advanceExact(docId) && values.getValueCount() <= 0) {
            return null;
        }
        String[] result = new String[(int) values.getValueCount()];
        int i = 0;
        long next;
        while ((next = values.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
            result[i++] = values.lookupOrd(next).utf8ToString();
        }

        return result;
    }

    public Double getDoubleVal(int docId, SortedNumericDoubleValues values) throws IOException{
        if(values.advanceExact(docId)) {
            return values.nextValue();
        } else {
            return null;
        }
    }

    public double mergeFieldScore(String fieldMode, double total, double target) {
        switch (fieldMode) {
            case Constants.FieldMode.SUM:
                total += target;
                break;
            case Constants.FieldMode.MULT:
                total *= target;
                break;
            case Constants.SortMode.MAX:
                total = total < target ? target : total;
                break;
            case Constants.SortMode.MIN:
                total = total > target ? target : total;
                break;
            default:
                total += target;
        }
        return total;
    }

    public double mergeSortScore(String sortMode, double total, double target) {
        switch (sortMode) {
            case Constants.SortMode.MAX:
                total = total < target ? target : total;
                break;
            case Constants.SortMode.MIN:
                total = total > target ? target : total;
                break;
            default:
                total = total < target ? target : total;
        }
        return total;
    }

    @Override
    public boolean needsScores() {
        return false;
    }

    @Override
    protected boolean doEquals(ScoreFunction other) {
        ComplexFieldFunction complexFieldFunction = (ComplexFieldFunction) other;
        return this.funcScoreFactor == complexFieldFunction.funcScoreFactor &&
                this.originalScoreFactor == complexFieldFunction.originalScoreFactor &&
                Objects.equals(this.categorys, complexFieldFunction.categorys);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(funcScoreFactor, originalScoreFactor, categorys);
    }

}
