package org.lccy.elasticsearch.plugin.query;

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

    public String name() {
        return name;
    }

    public CategoryScoreBo name(String name) {
        this.name = name;
        return this;
    }

    public String fieldMode() {
        return fieldMode;
    }

    public CategoryScoreBo fieldMode(String fieldMode) {
        this.fieldMode = fieldMode;
        return this;
    }

    public List<FieldComputeBo> fieldsScore() {
        return fieldsScore;
    }

    public CategoryScoreBo fieldsScore(List<FieldComputeBo> fieldsScore) {
        this.fieldsScore = fieldsScore;
        return this;
    }

    public String sortMode() {
        return sortMode;
    }

    public CategoryScoreBo sortMode(String sortMode) {
        this.sortMode = sortMode;
        return this;
    }

    public Double sortBaseSocre() {
        return sortBaseSocre;
    }

    public CategoryScoreBo sortBaseSocre(Double sortBaseSocre) {
        this.sortBaseSocre = sortBaseSocre;
        return this;
    }

    public List<SortComputeBo> sortScore() {
        return sortScore;
    }

    public CategoryScoreBo sortScore(List<SortComputeBo> sortScore) {
        this.sortScore = sortScore;
        return this;
    }
}
