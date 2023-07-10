package org.lccy.elasticsearch.plugin.function.bo;

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
