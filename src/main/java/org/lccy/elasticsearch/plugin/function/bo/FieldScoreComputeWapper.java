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
import org.lccy.elasticsearch.plugin.util.StringUtil;

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

    public FieldScoreComputeWapper(XContentParser parser, Map<String, Object> fd) {
        String field = (String) fd.get(FIELD);
        Double factor = fd.get(FACTOR) == null ? null : Double.parseDouble(fd.get(FACTOR).toString());
        String modifier = (String) fd.get(MODIFIER);
        if (StringUtil.isEmpty(field) || StringUtil.isEmpty(modifier) || factor == null) {
            throwsException(parser, ComplexFieldFunctionBuilder.NAME + " query param [categorys.fields_score] set error, please check.");
        }
        if (Modifier.DECAYGEOEXP.toString().equals(modifier)) {
            String origin = (String) fd.get(ORIGIN);
            String scale = (String) fd.get(SCALE);
            String offset = (String) fd.get(OFFSET);
            Double decay = fd.get(DECAY) == null ? null : Double.parseDouble(fd.get(DECAY).toString());
            if (StringUtil.isEmpty(origin) || StringUtil.isEmpty(scale) || StringUtil.isEmpty(offset) || decay == null) {
                throwsException(parser, ComplexFieldFunctionBuilder.NAME + " query param [categorys.fields_score.modifier.decaygeoexp] set error, please check.");
            }
        }
        this.fieldScore = fd;
    }

    public String getField() {
        return (String) fieldScore.get(FIELD);
    }

    public double getFactor() {
        return Double.parseDouble(fieldScore.get(FACTOR).toString());
    }

    public Modifier getModifier() {
        return Modifier.fromString((String) fieldScore.get(MODIFIER));
    }


    public double getWeight() {
        return fieldScore.get(WEIGHT) == null ? 1 : Double.parseDouble(fieldScore.get(WEIGHT).toString());
    }

    public double getAddNum() {
        return fieldScore.get(ADD_NUM) == null ? 0 : Double.parseDouble(fieldScore.get(ADD_NUM).toString());
    }

    public String getMissing() {
        return (String) fieldScore.get(MISSING);
    }


    public boolean getRequire() {
        return fieldScore.get(REQUIRE) == null ? false : (Boolean) fieldScore.get(REQUIRE);
    }


    public String getOrigin() {
        return (String) fieldScore.get(ORIGIN);
    }


    public String getScale() {
        return (String) fieldScore.get(SCALE);
    }


    public String getOffset() {
        return (String) fieldScore.get(OFFSET);
    }


    public Double getDecay() {
        return Double.parseDouble(fieldScore.get(DECAY).toString());
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
