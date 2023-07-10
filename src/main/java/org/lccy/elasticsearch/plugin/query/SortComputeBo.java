package org.lccy.elasticsearch.plugin.query;

import org.lccy.elasticsearch.plugin.util.StringUtil;

/**
 * The configuration pojo of sort_socre
 *
 * @author liuchen <br>
 * @date 2023-07-08
 */
public class SortComputeBo {
    private Integer weight;
    private String field;
    private String type;
    private String value;

    public Integer weight() {
        return weight;
    }

    public SortComputeBo weight(Integer weight) {
        this.weight = weight;
        return this;
    }

    public Integer getWeight() {
        return weight;
    }

    public String field() {
        return field;
    }

    public SortComputeBo field(String field) {
        this.field = field;
        return this;
    }

    public String type() {
        return type;
    }

    public SortComputeBo type(String type) {
        this.type = type;
        return this;
    }

    public String value() {
        return value;
    }

    public SortComputeBo value(String value) {
        this.value = value;
        return this;
    }

    public boolean match(String[] values) {
        for(String val : values) {
            if(this.value.equals(val)) {
                return !Type.NOT.equals(this.type);
            }
        }

        return false;
    }

    public static class Type {
        private static final String NOT = "not";
    }
}
