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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.Explanation;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.LeafScoreFunction;
import org.elasticsearch.common.lucene.search.function.ScoreFunction;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.fielddata.plain.SortedSetDVOrdinalsIndexFieldData;
import org.lccy.elasticsearch.plugin.function.bo.CategoryScoreBo;
import org.lccy.elasticsearch.plugin.function.bo.FieldComputeBo;
import org.lccy.elasticsearch.plugin.function.bo.SortComputeBo;
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
                long start = System.currentTimeMillis();

                String categoryCode = getStrVal(docId, (SortedSetDocValues) fieldDataMap.get(categoryField));
                CategoryScoreBo cbo;
                if(StringUtil.isEmpty(categoryCode) || (cbo = categorys.get(categoryCode)) == null) {
                    return subQueryScore;
                }
                System.out.println("categoryCode:" + categoryCode + ", sub socre:" +  subQueryScore);

                double fieldScoreTotal = 0;
                if(cbo.getFieldsScore() != null && !cbo.getFieldsScore().isEmpty()) {
                    String fieldMode = cbo.getFieldMode();
                    for(FieldComputeBo fbo : cbo.getFieldsScore()) {
                        System.out.println("field:" + fbo.getField() + ", fieldMode:" +  fieldMode + ", values class:" + fieldDataMap.get(fbo.getField()));
                        // get field value.
                        Double fVal = getDoubleVal(docId, (SortedNumericDoubleValues) fieldDataMap.get(fbo.getField()));
                        System.out.println("field:" + fbo.getField() + ", value:" +  fVal);
                        if(fVal == null) {
                            if(!fbo.getRequire()) {
                                continue;
                            } else if(fbo.getRequire() && fbo.getMissing() == null){
                                throw new IllegalArgumentException("require field " + fbo.getField() + "must has a value or has a missing value");
                            } else {
                                fVal = fbo.getMissing();
                            }
                        }
                        fieldScoreTotal = mergeFieldScore(fieldMode, fieldScoreTotal, fbo.computeScore(fVal));
                    }
                }
                System.out.println("fieldScoreTotal:" + fieldScoreTotal);

                double sortScoreTotal = 0;
                if(cbo.getSortScore() != null && !cbo.getSortScore().isEmpty()) {
                    String sortMode = cbo.getSortMode();
                    double sortBaseScore = cbo.getSortBaseSocre();
                    for(SortComputeBo sbo : cbo.getSortScore()) {
                        System.out.println("sortField:" + sbo.getField() + ", sortBaseScore:" +  sortBaseScore + ", values class:" + fieldDataMap.get(sbo.getField()));
                        String[] fVal = getStrValArray(docId, (SortedSetDocValues) fieldDataMap.get(sbo.getField()));
                        System.out.println("field:" + sbo.getField() + ", value:" +  Arrays.toString(fVal));
                        if(fVal == null) {
                            continue;
                        }
                        if(sbo.match(fVal)) {
                            sortScoreTotal = mergeSortScore(sortMode, sortScoreTotal, sbo.getWeight() * sortBaseScore);
                            break;
                        }
                    }
                }
                System.out.println("sortScoreTotal:" + sortScoreTotal);
                long end = System.currentTimeMillis();
                System.out.println("compute score cost:" + (end -start));

                return funcScoreFactor * fieldScoreTotal + originalScoreFactor * subQueryScore + sortScoreTotal;
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
        if(values == null) {
            return null;
        }
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
        if(target < 0) {
            return total;
        }
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
        if(target < 0) {
            return total;
        }
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
