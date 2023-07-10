package org.lccy.elasticsearch.plugin.function.bo;

import java.util.List;

/**
 * The configuration pojo of categorys
 *
 * @author liuchen <br>
 * @date 2023-07-08
 */
public class CategoryScoreBo {
    private String name;
    private String fieldMode;
    private List<FieldComputeBo> fieldsScore;
    private String sortMode;
    private Double sortBaseSocre;
    private List<SortComputeBo> sortScore;

    public String getName() {
        return name;
    }

    public CategoryScoreBo setName(String name) {
        this.name = name;
        return this;
    }

    public String getFieldMode() {
        return fieldMode;
    }

    public CategoryScoreBo setFieldMode(String fieldMode) {
        this.fieldMode = fieldMode;
        return this;
    }

    public List<FieldComputeBo> getFieldsScore() {
        return fieldsScore;
    }

    public CategoryScoreBo setFieldsScore(List<FieldComputeBo> fieldsScore) {
        this.fieldsScore = fieldsScore;
        return this;
    }

    public String getSortMode() {
        return sortMode;
    }

    public CategoryScoreBo setSortMode(String sortMode) {
        this.sortMode = sortMode;
        return this;
    }

    public Double getSortBaseSocre() {
        return sortBaseSocre;
    }

    public CategoryScoreBo setSortBaseSocre(Double sortBaseSocre) {
        this.sortBaseSocre = sortBaseSocre;
        return this;
    }

    public List<SortComputeBo> getSortScore() {
        return sortScore;
    }

    public CategoryScoreBo setSortScore(List<SortComputeBo> sortScore) {
        this.sortScore = sortScore;
        return this;
    }
}
