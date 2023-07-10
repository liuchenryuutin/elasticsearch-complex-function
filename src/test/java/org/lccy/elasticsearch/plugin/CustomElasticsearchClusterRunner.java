package org.lccy.elasticsearch.plugin;

import org.codelibs.elasticsearch.runner.ClusterRunnerException;
import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;

/**
 * 类名称： <br>
 * 类描述： <br>
 *
 * @Date: 2023/07/10 09:20 <br>
 * @author: liuchen11
 */
public class CustomElasticsearchClusterRunner extends ElasticsearchClusterRunner {


    public AcknowledgedResponse createIndex(final String index, final CreateIndexRequest request) {
        final AcknowledgedResponse actionGet = client().admin().indices().create(request).actionGet(10000);
        if (!actionGet.isAcknowledged()) {
            onFailure("Failed to close " + index + ".", actionGet);
        }
        return actionGet;
    }

    private void onFailure(final String message, final ActionResponse response) {
        if (printOnFailure) {
            print(message);
        } else {
            throw new ClusterRunnerException(message, response);
        }
    }
}
