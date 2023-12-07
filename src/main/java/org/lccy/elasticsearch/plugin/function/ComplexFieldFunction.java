package org.lccy.elasticsearch.plugin.function;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.Explanation;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.LeafScoreFunction;
import org.elasticsearch.common.lucene.search.function.ScoreFunction;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.fielddata.plain.AbstractLatLonPointDVIndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedSetDVOrdinalsIndexFieldData;
import org.lccy.elasticsearch.plugin.function.bo.CategoryScoreWapper;
import org.lccy.elasticsearch.plugin.function.bo.FieldScoreComputeWapper;
import org.lccy.elasticsearch.plugin.function.bo.SortScoreComputeWapper;
import org.lccy.elasticsearch.plugin.util.CommonUtil;

import java.io.IOException;
import java.util.*;

/**
 * A complex_field_score function that multiplies the score with the param settings.
 *
 * @author liuchen <br>
 * @date 2023-07-08
 */
public class ComplexFieldFunction extends ScoreFunction {

    private final CategoryScoreWapper categorys;
    private final Map<String, IndexFieldData> fieldMap;

    public ComplexFieldFunction(CategoryScoreWapper categorys, Map<String, IndexFieldData> fieldMap) {
        super(CombineFunction.MULTIPLY);
        this.categorys = categorys;
        this.fieldMap = fieldMap;
    }

    @Override
    public LeafScoreFunction getLeafScoreFunction(LeafReaderContext ctx) {
        final Map<String, Object> fieldDataMap = new HashMap<>();
        for (Map.Entry<String, IndexFieldData> entry : fieldMap.entrySet()) {
            IndexFieldData val = entry.getValue();
            String field = entry.getKey();
            if (val == null) {
                fieldDataMap.put(field, null);
            } else {
                if (val instanceof SortedSetDVOrdinalsIndexFieldData) {
                    // string type
                    fieldDataMap.put(field, ((SortedSetDVOrdinalsIndexFieldData) val).load(ctx).getOrdinalsValues());
                } else if (val instanceof IndexNumericFieldData) {
                    // int、float、double type
                    fieldDataMap.put(field, ((IndexNumericFieldData) val).load(ctx).getDoubleValues());
                } else if (val instanceof AbstractLatLonPointDVIndexFieldData) {
                    // geo type
                    fieldDataMap.put(field, ((AbstractLatLonPointDVIndexFieldData) val).load(ctx).getGeoPointValues());
                } else {
                    throw new ElasticsearchException("Not support mapping type for field [" + field + "], type:" + val.getClass());
                }
            }
        }
        final CategoryScoreWapper csw = this.categorys;

        return new LeafScoreFunction() {

            @Override
            public double score(int docId, float subQueryScore) throws IOException {
//                long start = System.currentTimeMillis();

                String categoryCode = getStrVal(docId, (SortedSetDocValues) fieldDataMap.get(csw.getCategoryField()));
                if (CommonUtil.isEmpty(categoryCode)) {
                    return csw.getOriginalScoreFactor() * subQueryScore;
                }
//                System.out.println("categoryCode:" + categoryCode + ", sub socre:" +  subQueryScore);
                List<FieldScoreComputeWapper> fieldScores = csw.getFieldScoreWappers(categoryCode);
                List<SortScoreComputeWapper> sortScores = csw.getScoreComputeWappers(categoryCode);

                double fieldScoreTotal = 0;
                if (!CommonUtil.isEmpty(fieldScores)) {
                    String fieldMode = csw.getFieldMode();
                    for (FieldScoreComputeWapper fbo : fieldScores) {
//                        System.out.println("field:" + fbo.getField() + ", fieldMode:" +  fieldMode + ", values class:" + fieldDataMap.get(fbo.getField()));

                        // get field value.
                        Object fVal;
                        if (FieldScoreComputeWapper.Modifier.DECAYGEOEXP.equals(fbo.getModifier())) {
                            fVal = getGeoPoint(docId, (MultiGeoPointValues) fieldDataMap.get(fbo.getField()));
                            if (fVal == null) {
                                if (!fbo.getRequire()) {
                                    continue;
                                } else if (fbo.getRequire() && CommonUtil.isEmpty(fbo.getMissing())) {
                                    throw new IllegalArgumentException("require field " + fbo.getField() + "must has a value or has a missing value");
                                } else {
                                    GeoPoint missing = new GeoPoint();
                                    missing.resetFromString(fbo.getMissing());
                                    fVal = missing;
                                }
                            }
                        } else {
                            fVal = getDoubleVal(docId, (SortedNumericDoubleValues) fieldDataMap.get(fbo.getField()));
                            if (fVal == null) {
                                if (!fbo.getRequire()) {
                                    continue;
                                } else if (fbo.getRequire() && CommonUtil.isEmpty(fbo.getMissing())) {
                                    throw new IllegalArgumentException("require field " + fbo.getField() + "must has a value or has a missing value");
                                } else {
                                    fVal = Double.parseDouble(fbo.getMissing());
                                }
                            }
                        }
//                        System.out.println("field:" + fbo.getField() + ", value:" +  fVal.toString());
                        fieldScoreTotal = mergeFieldScore(fieldMode, fieldScoreTotal, fbo.computeScore(fVal));
                    }
                }
//                System.out.println("fieldScoreTotal:" + fieldScoreTotal);

                double sortScoreTotal = 0;
                if (!CommonUtil.isEmpty(sortScores)) {
                    double sortBaseScore = csw.getSortBaseScore();
                    for (SortScoreComputeWapper sbo : sortScores) {
                        if(Constants.SortValueType.ANY.equals(sbo.getType())) {
//                            System.out.println("sortType any.");
                            sortScoreTotal = mergeSortScore(Constants.SortMode.MAX, sortScoreTotal, sbo.getWeight() * sortBaseScore);
                            break;
                        }
//                        System.out.println("sortField:" + sbo.getField() + ", sortBaseScore:" +  sortBaseScore + ", values class:" + fieldDataMap.get(sbo.getField()));
                        String field = sbo.getField();
                        boolean match = false;
                        if(field.indexOf(Constants.SPLIT) > 0) {
                            // 多个字段时，按照Constants.SPLIT后处理
                            String[] fields = field.split(Constants.SPLIT);
                            String[] types = sbo.getType().split(Constants.SPLIT);
                            String[] values = sbo.getValue().split(Constants.SPLIT);
                            for(int i = 0; i < fields.length; i++) {
                                String f = fields[i];
                                String type = types.length > i ? types[i] : null;
                                String value = values.length > i ? values[i] : null;
                                String[] val = getStrValArray(docId, (SortedSetDocValues) fieldDataMap.get(f));
                                match = SortScoreComputeWapper.matchNew(type, value, val);
//                                System.out.println("split分类名:" + categoryCode + "字段名:" + f + ",期待值:" + value + ",实际值:" + Arrays.toString(val) + ",是否匹配:" + match);
                                if(!match) {
                                    break;
                                }
                            }
                        } else {
                            String[] fVal = getStrValArray(docId, (SortedSetDocValues) fieldDataMap.get(field));
                            match = sbo.match(fVal);
//                            System.out.println("分类名:" + categoryCode + "字段名:" + field + ",期待值:" + sbo.getValue() + ",实际值:" + Arrays.toString(fVal) + ",是否匹配:" + match);
                        }
                        if (match) {
                            sortScoreTotal = mergeSortScore(Constants.SortMode.MAX, sortScoreTotal, sbo.getWeight() * sortBaseScore);
                            break;
                        }
                    }
                }
//                System.out.println("sortScoreTotal:" + sortScoreTotal);
//                long end = System.currentTimeMillis();
//                System.out.println("compute score cost:" + (end - start));

                return csw.getFuncScoreFactor() * fieldScoreTotal + csw.getOriginalScoreFactor() * subQueryScore + sortScoreTotal;
            }

            @Override
            public Explanation explainScore(int docId, Explanation subQueryScore) throws IOException {

                String categoryCode = getStrVal(docId, (SortedSetDocValues) fieldDataMap.get(csw.getCategoryField()));
                if (CommonUtil.isEmpty(categoryCode)) {
                    return Explanation.match(csw.getOriginalScoreFactor() * subQueryScore.getValue().floatValue()
                            , String.format("category is empty. subQueryScore:[%f], expression:[%f * subScore]", subQueryScore.getValue().floatValue(), csw.getOriginalScoreFactor()));
                }
                List<FieldScoreComputeWapper> fieldScores = csw.getFieldScoreWappers(categoryCode);
                List<SortScoreComputeWapper> sortScores = csw.getScoreComputeWappers(categoryCode);

                double fieldScoreTotal = 0;
                Explanation fieldsExplain = null;
                if (!CommonUtil.isEmpty(fieldScores)) {
                    String fieldMode = csw.getFieldMode();
                    List<Explanation> fieldExplanList = new ArrayList<>();
                    for (FieldScoreComputeWapper fbo : fieldScores) {

                        // get field value.
                        Object fVal;
                        boolean useMissing = false;
                        if (FieldScoreComputeWapper.Modifier.DECAYGEOEXP.equals(fbo.getModifier())) {
                            fVal = getGeoPoint(docId, (MultiGeoPointValues) fieldDataMap.get(fbo.getField()));
                            if (fVal == null) {
                                if (!fbo.getRequire()) {
                                    continue;
                                } else if (fbo.getRequire() && CommonUtil.isEmpty(fbo.getMissing())) {
                                    throw new IllegalArgumentException("require field " + fbo.getField() + "must has a value or has a missing value");
                                } else {
                                    GeoPoint missing = new GeoPoint();
                                    missing.resetFromString(fbo.getMissing());
                                    fVal = missing;
                                    useMissing = true;
                                }
                            }
                        } else {
                            fVal = getDoubleVal(docId, (SortedNumericDoubleValues) fieldDataMap.get(fbo.getField()));
                            if (fVal == null) {
                                if (!fbo.getRequire()) {
                                    continue;
                                } else if (fbo.getRequire() && CommonUtil.isEmpty(fbo.getMissing())) {
                                    throw new IllegalArgumentException("require field " + fbo.getField() + "must has a value or has a missing value");
                                } else {
                                    fVal = Double.parseDouble(fbo.getMissing());
                                    useMissing = true;
                                }
                            }
                        }

                        double fieldScore = fbo.computeScore(fVal);

                        fieldScoreTotal = mergeFieldScore(fieldMode, fieldScoreTotal, fieldScore);

                        Explanation fex = Explanation.match(fieldScore, String.format(Locale.ROOT, "Compute field:[%s], using missing:[%s], expression:[%s].",
                                fbo.getField(), useMissing, fbo.getExpression(fVal)));
                        fieldExplanList.add(fex);
                    }

                    fieldsExplain = Explanation.match(fieldScoreTotal, String.format(Locale.ROOT, "Compute fieldScoreTotal, filed_mode:[%s].", fieldMode), fieldExplanList);
                }

                double sortScoreTotal = 0;
                Explanation sortExplain = null;
                if (!CommonUtil.isEmpty(sortScores)) {
                    double sortBaseScore = csw.getSortBaseScore();
                    List<Explanation> sortExplanList = new ArrayList<>();
                    for (SortScoreComputeWapper sbo : sortScores) {
                        if(Constants.SortValueType.ANY.equals(sbo.getType())) {
                            double sortScore = sbo.getWeight() * sortBaseScore;
                            sortScoreTotal = mergeSortScore(Constants.SortMode.MAX, sortScoreTotal, sortScore);
                            Explanation sortEx = Explanation.match(sortScore, "Compute sort type:[any], expression:[it's always true].");
                            sortExplanList.add(sortEx);
                            break;
                        }

                        String field = sbo.getField();
                        boolean match = false;
                        StringBuilder fVals = new StringBuilder();
                        if(field.indexOf(Constants.SPLIT) > 0) {
                            // 多个字段时，按照Constants.SPLIT后处理
                            String[] fields = field.split(Constants.SPLIT);
                            String[] types = sbo.getType().split(Constants.SPLIT);
                            String[] values = sbo.getValue().split(Constants.SPLIT);
                            for(int i = 0; i < fields.length; i++) {
                                String f = fields[i];
                                String type = types.length > i ? types[i] : null;
                                String value = values.length > i ? values[i] : null;
                                String[] val = getStrValArray(docId, (SortedSetDocValues) fieldDataMap.get(f));
                                if(fVals.length() != 0) {
                                    fVals.append(Constants.SPLIT);
                                }
                                fVals.append(Arrays.toString(val));
                                match = SortScoreComputeWapper.matchNew(type, value, val);
//                                System.out.println("split分类名:" + categoryCode + "字段名:" + f + ",期待值:" + value + ",实际值:" + Arrays.toString(val) + ",是否匹配:" + match);
                                if(!match) {
                                    break;
                                }
                            }
                        } else {
                            String[] fVal = getStrValArray(docId, (SortedSetDocValues) fieldDataMap.get(field));
                            fVals.append(Arrays.toString(fVal));
                            match = sbo.match(fVal);
//                            System.out.println("分类名:" + categoryCode + "字段名:" + field + ",期待值:" + sbo.getValue() + ",实际值:" + Arrays.toString(fVal) + ",是否匹配:" + match);
                        }

                        if (match) {
                            double sortScore = sbo.getWeight() * sortBaseScore;
                            sortScoreTotal = mergeSortScore(Constants.SortMode.MAX, sortScoreTotal, sbo.getWeight() * sortBaseScore);
                            Explanation sortEx = Explanation.match(sortScore, String.format(Locale.ROOT, "Compute sort field:[%s], value:[%s], expression:[%s].",
                                    sbo.getField(), fVals, sbo.getExpression(sortBaseScore)));
                            sortExplanList.add(sortEx);
                            break;
                        }
                    }
                    sortExplain = Explanation.match(sortScoreTotal, String.format(Locale.ROOT, "Compute sortScoreTotal, sort_mode:[max], sort_base_score:[%f] ", sortBaseScore), sortExplanList);
                }

                float subScore = subQueryScore.getValue().floatValue();
                double score = csw.getFuncScoreFactor() * fieldScoreTotal + csw.getOriginalScoreFactor() * subScore + sortScoreTotal;
                List<Explanation> resList = new ArrayList<>();
                if (fieldsExplain != null) {
                    resList.add(fieldsExplain);
                }
                if (sortExplain != null) {
                    resList.add(sortExplain);
                }
                Explanation result = Explanation.match(
                        (float) score,
                        String.format(Locale.ROOT,
                                "Compute complex_field_score, subScore:[%f] expression: [%f * fieldScoreTotal + %f * subScore + sortScoreTotal]",
                                subScore, csw.getFuncScoreFactor(), csw.getOriginalScoreFactor()), resList);
                return result;
            }
        };
    }

    public String getStrVal(int docId, SortedSetDocValues values) throws IOException {
        if (values == null) {
            return null;
        }
        long next;
        if (values.advanceExact(docId) && (next = values.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
            return values.lookupOrd(next).utf8ToString();
        } else {
            return null;
        }
    }

    public String[] getStrValArray(int docId, SortedSetDocValues values) throws IOException {
        if (values == null || !values.advanceExact(docId)) {
            return null;
        }
        List<String> result = new ArrayList<>();
        long next;
        while ((next = values.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
            result.add(values.lookupOrd(next).utf8ToString());
        }

        return result.toArray(new String[result.size()]);
    }

    public Double getDoubleVal(int docId, SortedNumericDoubleValues values) throws IOException {
        if (values == null) {
            return null;
        }
        if (values.advanceExact(docId)) {
            return values.nextValue();
        } else {
            return null;
        }
    }

    public GeoPoint getGeoPoint(int docId, MultiGeoPointValues values) throws IOException {
        if (values == null) {
            return null;
        }
        if (values.advanceExact(docId)) {
            return values.nextValue();
        } else {
            return null;
        }
    }

    public double mergeFieldScore(String fieldMode, double total, double target) {
        if (target < 0) {
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
        if (target < 0) {
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
        return true;
    }

    @Override
    protected boolean doEquals(ScoreFunction other) {
        if(other instanceof ComplexFieldFunction) {
            ComplexFieldFunction complexFieldFunction = (ComplexFieldFunction) other;
            return Objects.equals(this.categorys, complexFieldFunction.categorys);
        } else {
            return false;
        }
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(categorys);
    }

}
