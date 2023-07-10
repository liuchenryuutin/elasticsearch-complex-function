package org.lccy.elasticsearch.plugin;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.lccy.elasticsearch.plugin.query.ComplexFieldFunctionBuilder;

import java.util.Collections;
import java.util.List;


/**
 * Register complex_field_score plugin for elasticsearch.
 *
 * @author liuchen <br>
 * @date 2023-07-08
 */
public class ComplexFieldFunctionPlugin extends Plugin implements SearchPlugin {

    @Override
    public List<ScoreFunctionSpec<?>> getScoreFunctions() {
        return Collections.singletonList(new ScoreFunctionSpec<>(ComplexFieldFunctionBuilder.NAME, ComplexFieldFunctionBuilder::new, ComplexFieldFunctionBuilder::fromXContent));
    }
}