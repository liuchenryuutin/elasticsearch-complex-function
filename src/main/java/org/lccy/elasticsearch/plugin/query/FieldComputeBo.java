package org.lccy.elasticsearch.plugin.query;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Locale;

/**
 * The configuration pojo of fields_score
 *
 * @author liuchen <br>
 * @date 2023-07-08
 */
public class FieldComputeBo {
    private String field;
    private double factor;
    private Modifier modifier;
    private double weight;
    private double addNum;
    private Double missing;
    private boolean require = false;

    public String field() {
        return field;
    }

    public FieldComputeBo field(String field) {
        this.field = field;
        return this;
    }

    public double factor() {
        return factor;
    }

    public FieldComputeBo factor(double factor) {
        this.factor = factor;
        return this;
    }

    public Modifier modifier() {
        return modifier;
    }

    public FieldComputeBo modifier(Modifier modifier) {
        this.modifier = modifier;
        return this;
    }

    public FieldComputeBo modifier(String modifier) {
        this.modifier = Modifier.fromString(modifier);
        return this;
    }

    public double weight() {
        return weight;
    }

    public FieldComputeBo weight(double weight) {
        this.weight = weight;
        return this;
    }

    public double addNum() {
        return addNum;
    }

    public FieldComputeBo addNum(double addNum) {
        this.addNum = addNum;
        return this;
    }

    public Double missing() {
        return missing;
    }

    public FieldComputeBo missing(Double missing) {
        this.missing = missing;
        return this;
    }

    public boolean require() {
        return require;
    }

    public FieldComputeBo require(boolean require) {
        this.require = require;
        return this;
    }

    /**
     * calculate score based on value
     * @param value
     * @return
     */
    public double computeScore(double value) {
        return this.factor * this.modifier.apply(value) * this.weight + this.addNum;
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
