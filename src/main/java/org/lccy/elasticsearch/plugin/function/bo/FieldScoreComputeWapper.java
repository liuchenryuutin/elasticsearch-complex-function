package org.lccy.elasticsearch.plugin.function.bo;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.ScoreScriptUtils;
import org.lccy.elasticsearch.plugin.function.ComplexFieldFunctionBuilder;
import org.lccy.elasticsearch.plugin.util.CommonUtil;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * wrap field score query param <br>
 *
 * @author liuchen <br>
 * @date 2023-07-11
 */
public class FieldScoreComputeWapper {

    public static final String FIELD = "field";
    public static final String FACTOR = "factor";
    public static final String MODIFIER = "modifier";
    public static final String WEIGHT = "weight";
    public static final String ADD_NUM = "add_num";
    public static final String MISSING = "missing";
    public static final String REQUIRE = "require";
    public static final String ORIGIN = "origin";
    public static final String SCALE = "scale";
    public static final String OFFSET = "offset";
    public static final String DECAY = "decay";

    private Map<String, Object> fieldScore;

    private String field;
    private Double factor;
    private Modifier modifier;
    private double weight;
    private double addNum;
    private String missing;
    private boolean require;
    private String origin;
    private String scale;
    private String offset;
    private Double decay;

    public FieldScoreComputeWapper(XContentParser parser, Map<String, Object> fd) {
        String field = CommonUtil.toString(fd.get(FIELD));
        Double factor = fd.get(FACTOR) == null ? null : Double.parseDouble(fd.get(FACTOR).toString());
        String modifier = CommonUtil.toString(fd.get(MODIFIER));
        if (CommonUtil.isEmpty(field) || CommonUtil.isEmpty(modifier) || Modifier.checkExist(modifier) || factor == null) {
            throwsException(parser, ComplexFieldFunctionBuilder.NAME + " query param [categorys.fields_score] set error, please check.");
        }
        if (Modifier.DECAYGEOEXP.toString().equals(modifier)) {
            String origin = CommonUtil.toString(fd.get(ORIGIN));
            String scale = CommonUtil.toString(fd.get(SCALE));
            String offset = CommonUtil.toString(fd.get(OFFSET));
            Double decay = fd.get(DECAY) == null ? null : Double.parseDouble(fd.get(DECAY).toString());
            if (CommonUtil.isEmpty(origin) || CommonUtil.isEmpty(scale) || CommonUtil.isEmpty(offset) || decay == null) {
                throwsException(parser, ComplexFieldFunctionBuilder.NAME + " query param [categorys.fields_score.modifier.decaygeoexp] set error, please check.");
            }
            this.origin = origin;
            this.scale = scale;
            this.offset = offset;
            this.decay = decay;
        }
        this.fieldScore = fd;
        this.field = field;
        this.factor = factor;
        this.modifier = Modifier.fromString(modifier);
        this.weight = fd.get(WEIGHT) == null ? 1 : Double.parseDouble(fd.get(WEIGHT).toString());
        this.addNum = fd.get(ADD_NUM) == null ? 0 : Double.parseDouble(fd.get(ADD_NUM).toString());
        this.missing = CommonUtil.toString(fd.get(MISSING));
        this.require = fd.get(REQUIRE) == null ? false : (Boolean) fd.get(REQUIRE);
    }

    public String getField() {
        return field;
    }

    public double getFactor() {
        return factor;
    }

    public Modifier getModifier() {
        return modifier;
    }


    public double getWeight() {
        return weight;
    }

    public double getAddNum() {
        return addNum;
    }

    public String getMissing() {
        return missing;
    }


    public boolean getRequire() {
        return require;
    }


    public String getOrigin() {
        return origin;
    }


    public String getScale() {
        return scale;
    }


    public String getOffset() {
        return offset;
    }


    public Double getDecay() {
        return decay;
    }

    private void throwsException(XContentParser parser, String msg) {
        if (parser != null) {
            throw new ParsingException(parser.getTokenLocation(), msg);
        } else {
            throw new IllegalArgumentException(msg);
        }
    }

    public String getExpression(Object val) {
        return String.format("(%f + %f * %s(%s)) * %f", getAddNum(), getFactor(), getModifier().toString(), val.toString(), getWeight());
    }

    /**
     * calculate score based on value
     *
     * @param value
     * @return
     */
    public double computeScore(Object value) {
        double fieldScore;
        if (value instanceof GeoPoint) {
            fieldScore = new ScoreScriptUtils.DecayGeoExp(getOrigin(), getScale(), getOffset(), getDecay()).decayGeoExp((GeoPoint) value);
        } else {
            fieldScore = this.getModifier().apply((Double) value);
        }
        return (this.getAddNum() + this.getFactor() * fieldScore) * this.getWeight();
    }


    /**
     * The Type class encapsulates the modification types that can be applied
     * to the score/value product.
     */
    public enum Modifier implements Writeable {
        NONE {
            @Override
            public double apply(double n) {
                return n;
            }
        },
        LOG {
            @Override
            public double apply(double n) {
                return Math.log10(n);
            }
        },
        LOG1P {
            @Override
            public double apply(double n) {
                return Math.log10(n + 1);
            }
        },
        LOG2P {
            @Override
            public double apply(double n) {
                return Math.log10(n + 2);
            }
        },
        LN {
            @Override
            public double apply(double n) {
                return Math.log(n);
            }
        },
        LN1P {
            @Override
            public double apply(double n) {
                return Math.log1p(n);
            }
        },
        LN2P {
            @Override
            public double apply(double n) {
                return Math.log1p(n + 1);
            }
        },
        SQUARE {
            @Override
            public double apply(double n) {
                return Math.pow(n, 2);
            }
        },
        SQRT {
            @Override
            public double apply(double n) {
                return Math.sqrt(n);
            }
        },
        RECIPROCAL {
            @Override
            public double apply(double n) {
                return 1.0 / n;
            }
        },
        DECAYGEOEXP {
            @Override
            public double apply(double n) {
                throw new ElasticsearchException("decaygeoexp no support");
            }
        };

        public abstract double apply(double n);

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(this);
        }

        public static Modifier readFromStream(StreamInput in) throws IOException {
            return in.readEnum(Modifier.class);
        }

        @Override
        public String toString() {
            return super.toString().toLowerCase(Locale.ROOT);
        }

        public static Modifier fromString(String modifier) {
            return valueOf(modifier.toUpperCase(Locale.ROOT));
        }

        public static boolean checkExist(String modifier) {
            for (Modifier mo : values()) {
                if (mo.toString().equals(modifier.toUpperCase(Locale.ROOT))) {
                    return true;
                }
            }
            return false;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldScoreComputeWapper that = (FieldScoreComputeWapper) o;
        return Objects.equals(fieldScore, that.fieldScore);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldScore);
    }
}
