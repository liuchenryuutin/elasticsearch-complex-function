package org.lccy.elasticsearch.plugin.function.bo;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.script.ScoreScriptUtils;

import java.io.IOException;
import java.io.Serializable;
import java.util.Locale;
import java.util.Objects;

/**
 * The configuration pojo of fields_score
 *
 * @author liuchen <br>
 * @date 2023-07-08
 */
public class FieldComputeBo implements Serializable, Writeable {
    private String field;
    private double factor;
    private Modifier modifier;
    private double weight;
    private double addNum;
    private String missing;
    private boolean require = false;
    // distance compute
    private String origin;
    private String scale;
    private String offset;
    private Double decay;

    public String getField() {
        return field;
    }

    public FieldComputeBo setField(String field) {
        this.field = field;
        return this;
    }

    public double getFactor() {
        return factor;
    }

    public FieldComputeBo setFactor(double factor) {
        this.factor = factor;
        return this;
    }

    public Modifier getModifier() {
        return modifier;
    }

    public FieldComputeBo setModifier(String modifier) {
        this.modifier = Modifier.fromString(modifier);
        return this;
    }

    public double getWeight() {
        return weight;
    }

    public FieldComputeBo setWeight(double weight) {
        this.weight = weight;
        return this;
    }

    public double getAddNum() {
        return addNum;
    }

    public FieldComputeBo setAddNum(double addNum) {
        this.addNum = addNum;
        return this;
    }

    public String getMissing() {
        return missing;
    }

    public FieldComputeBo setMissing(String missing) {
        this.missing = missing;
        return this;
    }

    public boolean getRequire() {
        return require;
    }

    public FieldComputeBo setRequire(boolean require) {
        this.require = require;
        return this;
    }

    public String getOrigin() {
        return origin;
    }

    public FieldComputeBo setOrigin(String origin) {
        this.origin = origin;
        return this;
    }

    public String getScale() {
        return scale;
    }

    public FieldComputeBo setScale(String scale) {
        this.scale = scale;
        return this;
    }

    public String getOffset() {
        return offset;
    }

    public FieldComputeBo setOffset(String offset) {
        this.offset = offset;
        return this;
    }

    public Double getDecay() {
        return decay;
    }

    public FieldComputeBo setDecay(Double decay) {
        this.decay = decay;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldComputeBo that = (FieldComputeBo) o;
        return Double.compare(that.factor, factor) == 0 && Double.compare(that.weight, weight) == 0 && Double.compare(that.addNum, addNum) == 0 && require == that.require && Objects.equals(field, that.field) && modifier == that.modifier && Objects.equals(missing, that.missing) && Objects.equals(origin, that.origin) && Objects.equals(scale, that.scale) && Objects.equals(offset, that.offset) && Objects.equals(decay, that.decay);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, factor, modifier, weight, addNum, missing, require, origin, scale, offset, decay);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeDouble(factor);
        modifier.writeTo(out);
        out.writeDouble(weight);
        out.writeDouble(addNum);
        out.writeOptionalString(missing);
        out.writeOptionalBoolean(require);
        out.writeOptionalString(origin);
        out.writeOptionalString(scale);
        out.writeOptionalString(offset);
        out.writeOptionalDouble(decay);
    }

    /**
     * calculate score based on value
     *
     * @param value
     * @return
     */
    public double computeScore(Object value) {
        double fieldScore;
        if(value instanceof GeoPoint) {
            fieldScore = new ScoreScriptUtils.DecayGeoExp(origin, scale, offset, decay).decayGeoExp((GeoPoint) value);
        } else {
            fieldScore = this.modifier.apply((Double) value);
        }
        return (this.addNum + this.factor * fieldScore) * this.weight;
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
}
