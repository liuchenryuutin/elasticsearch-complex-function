package org.lccy.elasticsearch.plugin.function.bo;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

/**
 * The configuration pojo of sort_socre
 *
 * @author liuchen <br>
 * @date 2023-07-08
 */
public class SortComputeBo implements Serializable, Writeable {
    private Integer weight;
    private String field;
    private String type;
    private String value;

    public Integer getWeight() {
        return weight;
    }

    public SortComputeBo setWeight(Integer weight) {
        this.weight = weight;
        return this;
    }

    public String getField() {
        return field;
    }

    public SortComputeBo setField(String field) {
        this.field = field;
        return this;
    }

    public String getType() {
        return type;
    }

    public SortComputeBo setType(String type) {
        this.type = type;
        return this;
    }

    public String getValue() {
        return value;
    }

    public SortComputeBo setValue(String value) {
        this.value = value;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SortComputeBo that = (SortComputeBo) o;
        return weight.equals(that.weight) && field.equals(that.field) && Objects.equals(type, that.type) && value.equals(that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(weight, field, type, value);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeInt(weight);
        out.writeOptionalString(type);
        out.writeOptionalString(value);
    }


    public boolean match(String[] values) {
        for (String val : values) {
            if (this.value.equals(val)) {
                return !Type.NOT.equals(this.type);
            }
        }

        return false;
    }

    public static class Type {
        private static final String NOT = "not";
    }
}
