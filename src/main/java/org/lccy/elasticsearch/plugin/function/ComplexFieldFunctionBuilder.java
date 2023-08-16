package org.lccy.elasticsearch.plugin.function;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.function.ScoreFunction;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilder;
import org.lccy.elasticsearch.plugin.function.bo.CategoryScoreWapper;
import org.lccy.elasticsearch.plugin.function.bo.SortScoreComputeWapper;
import org.lccy.elasticsearch.plugin.util.CommonUtil;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Builder to construct {@code complex_field_score} functions for a function
 * score query.
 *
 * @author liuchen <br>
 * @date 2023-07-08
 */
public class ComplexFieldFunctionBuilder extends ScoreFunctionBuilder<ComplexFieldFunctionBuilder> {
    public static final String NAME = "complex_field_score";

    private final CategoryScoreWapper categorys;

    public ComplexFieldFunctionBuilder(CategoryScoreWapper categorys) {
        if (categorys == null) {
            throw new IllegalArgumentException("require param is not set, please check.");
        }
        this.categorys = categorys;
    }

    /**
     * Read from a stream.
     */
    public ComplexFieldFunctionBuilder(StreamInput in) throws IOException {
        super(in);
        Map<String, Object> request = in.readMap();
        if (request == null || request.isEmpty()) {
            throw new IllegalArgumentException(NAME + " query is empty.");
        }
        CategoryScoreWapper categorys = new CategoryScoreWapper(null, request);
        this.categorys = categorys;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeMap(categorys.unwrap());
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getName());
        for(Map.Entry<String, Object> entry : categorys.unwrap().entrySet()) {
            builder.field(entry.getKey(), entry.getValue());
        }
        builder.endObject();
    }

    @Override
    protected boolean doEquals(ComplexFieldFunctionBuilder functionBuilder) {
        return Objects.equals(this.categorys, functionBuilder.categorys);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(this.categorys);
    }

    @Override
    protected ScoreFunction doToFunction(QueryShardContext context) {
        Map<String, IndexFieldData> fieldDataMap = new HashMap<>();
        for (Map.Entry<String, Boolean> entry : this.categorys.getAllFiled().entrySet()) {
            MappedFieldType fieldType = context.getMapperService().fullName(entry.getKey());
            if (fieldType == null) {
                // require field's mapping must exists
                if (entry.getValue()) {
                    throw new ElasticsearchException("Unable to find a field mapper for field [" + entry.getKey() + "]. No 'missing' value defined.");
                }
            } else {
                IndexFieldData fieldData = context.getForField(fieldType);
                if (fieldData == null) {
                    // require field's mapping must exists
                    if (entry.getValue()) {
                        throw new ElasticsearchException("Unable to find a field mapper for field [" + entry.getKey() + "]. No 'missing' value defined.");
                    }
                } else {
                    fieldDataMap.put(entry.getKey(), fieldData);
                }
            }
        }

        return new ComplexFieldFunction(categorys, fieldDataMap);
    }

    public static ComplexFieldFunctionBuilder fromXContent(XContentParser parser)
            throws IOException, ParsingException {
        CategoryScoreWapper categorys;
        Map<String, Object> request = parser.map();
        if (CommonUtil.isEmpty(request)) {
            throw new ParsingException(parser.getTokenLocation(), NAME + " query is empty.");
        }
        categorys = new CategoryScoreWapper(parser, request);

        ComplexFieldFunctionBuilder complexFieldFunctionBuilder = new ComplexFieldFunctionBuilder(categorys);
        return complexFieldFunctionBuilder;
    }
}
