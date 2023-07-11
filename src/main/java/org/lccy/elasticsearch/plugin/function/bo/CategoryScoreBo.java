package org.lccy.elasticsearch.plugin.function.bo;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * The configuration pojo of categorys
 *
 * @author liuchen <br>
 * @date 2023-07-08
 */
public class CategoryScoreBo implements Serializable, Writeable {
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

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(fieldMode);
        if(fieldsScore != null && !fieldsScore.isEmpty()) {
            for(FieldComputeBo x : fieldsScore) {
                x.writeTo(out);
            }
        }
        out.writeString(sortMode);
        out.writeDouble(sortBaseSocre);
        if(sortScore != null && !sortScore.isEmpty()) {
            for(SortComputeBo x : sortScore) {
                x.writeTo(out);
            }
        }
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CategoryScoreBo that = (CategoryScoreBo) o;
        return name.equals(that.name) && fieldMode.equals(that.fieldMode) && Objects.equals(fieldsScore, that.fieldsScore) && sortMode.equals(that.sortMode) && sortBaseSocre.equals(that.sortBaseSocre) && Objects.equals(sortScore, that.sortScore);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, fieldMode, fieldsScore, sortMode, sortBaseSocre, sortScore);
    }

}
