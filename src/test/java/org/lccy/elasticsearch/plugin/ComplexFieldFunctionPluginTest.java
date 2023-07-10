///*
// * Copyright (c) 2019, guanquan.wang@yandex.com All Rights Reserved.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.lccy.elasticsearch.plugin;
//
//import com.alibaba.fastjson.JSONObject;
//import org.apache.logging.log4j.core.util.JsonUtils;
//import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
//import org.elasticsearch.action.ActionFuture;
//import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;
//import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
//import org.elasticsearch.action.search.SearchResponse;
//import org.elasticsearch.common.settings.Settings;
//import org.elasticsearch.common.xcontent.XContentFactory;
//import org.elasticsearch.common.xcontent.XContentType;
//import org.elasticsearch.index.query.QueryBuilder;
//import org.elasticsearch.index.query.QueryBuilders;
//import org.junit.After;
//import org.junit.Before;
//import org.junit.Test;
//
//import java.io.IOException;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.TimeUnit;
//
//import static org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.newConfigs;
//
///**
// * Create by guanquan.wang at 2019-09-18 16:55
// */
//public class ComplexFieldFunctionPluginTest {
//    private CustomElasticsearchClusterRunner runner;
//
//    @Before
//    public void setUp() {
//        // create runner instance
//        runner = new CustomElasticsearchClusterRunner();
//        // create ES nodes
//        runner.build(newConfigs()
//                .baseHttpPort(9210)
//                .basePath("F:\\workspace-pom-demo\\elasticsearch-lccy-query\\elasticsearch-home")
//                .numOfNode(1) // Create a test node, default number of node is 3.
//                .pluginTypes("org.lccy.elasticsearch.plugin.ComplexFieldFunctionPlugin")
//        );
//    }
//
//    @After
//    public void tearDown() throws IOException {
//        // close runner
//        runner.close();
//        // delete all files
//        runner.clean();
//    }
//
//    private void createIndexWithLocal(String indexName) {
//
//        final String indexSetting = "{\n" +
//                "    \"index\": {\n" +
//                "        \"analysis\": {\n" +
//                "            \"analyzer\": {\n" +
//                "                \"douhao\": {\n" +
//                "                    \"type\": \"pattern\",\n" +
//                "                    \"pattern\": \",\"\n" +
//                "                }\n" +
//                "            }\n" +
//                "        }\n" +
//                "    }\n" +
//                "}";
//
//
//
//        final String mappings = "{\n" +
//                "    \"provCode\": {\n" +
//                "        \"analyzer\": \"douhao\",\n" +
//                "        \"type\": \"text\",\n" +
//                "        \"fields\": {\n" +
//                "            \"keyword\": {\n" +
//                "                \"type\": \"keyword\"\n" +
//                "            }\n" +
//                "        },\n" +
//                "        \"doc_values\": false\n" +
//                "    },\n" +
//                "    \"provName\": {\n" +
//                "        \"analyzer\": \"douhao\",\n" +
//                "        \"type\": \"text\",\n" +
//                "        \"fields\": {\n" +
//                "            \"keyword\": {\n" +
//                "                \"type\": \"keyword\"\n" +
//                "            }\n" +
//                "        },\n" +
//                "        \"doc_values\": false\n" +
//                "    },\n" +
//                "    \"title\": {\n" +
//                "        \"analyzer\": \"douhao\",\n" +
//                "        \"type\": \"text\",\n" +
//                "        \"fields\": {\n" +
//                "            \"keyword\": {\n" +
//                "                \"type\": \"keyword\"\n" +
//                "            }\n" +
//                "        },\n" +
//                "        \"doc_values\": false\n" +
//                "    },\n" +
//                "    \"currentPrice\": {\n" +
//                "        \"type\": \"float\"\n" +
//                "    },\n" +
//                "    \"saleQuantity\": {\n" +
//                "        \"type\": \"long\"\n" +
//                "    },\n" +
//                "    \"startTime\": {\n" +
//                "        \"type\": \"date\",\n" +
//                "        \"format\": \"yyyy-MM-dd HH:mm:ss||yyyy-MM-dd\"\n" +
//                "    },\n" +
//                "    \"endTime\": {\n" +
//                "        \"type\": \"date\",\n" +
//                "        \"format\": \"yyyy-MM-dd HH:mm:ss||yyyy-MM-dd\"\n" +
//                "    },\n" +
//                "    \"categoryCode\": {\n" +
//                "        \"analyzer\": \"douhao\",\n" +
//                "        \"type\": \"text\",\n" +
//                "        \"fields\": {\n" +
//                "            \"keyword\": {\n" +
//                "                \"type\": \"keyword\"\n" +
//                "            }\n" +
//                "        }\n" +
//                "    },\n" +
//                "    \"state\": {\n" +
//                "        \"type\": \"keyword\"\n" +
//                "    },\n" +
//                "    \"exposure\": {\n" +
//                "        \"type\": \"long\"\n" +
//                "    },\n" +
//                "    \"clickQuantity\": {\n" +
//                "        \"type\": \"long\"\n" +
//                "    },\n" +
//                "    \"playersNum\": {\n" +
//                "        \"type\": \"long\"\n" +
//                "    },\n" +
//                "    \"readingsNum\": {\n" +
//                "        \"type\": \"long\"\n" +
//                "    },\n" +
//                "    \"location\": {\n" +
//                "        \"type\": \"geo_point\"\n" +
//                "    },\n" +
//                "    \"cornerMark\": {\n" +
//                "        \"type\": \"keyword\"\n" +
//                "    },\n" +
//                "    \"sourceProvCode\": {\n" +
//                "        \"analyzer\": \"douhao\",\n" +
//                "        \"type\": \"text\",\n" +
//                "        \"fields\": {\n" +
//                "            \"keyword\": {\n" +
//                "                \"type\": \"keyword\"\n" +
//                "            }\n" +
//                "        },\n" +
//                "        \"doc_values\": false\n" +
//                "    },\n" +
//                "    \"iopType\": {\n" +
//                "        \"analyzer\": \"douhao\",\n" +
//                "        \"type\": \"text\",\n" +
//                "        \"fields\": {\n" +
//                "            \"keyword\": {\n" +
//                "                \"type\": \"keyword\"\n" +
//                "            }\n" +
//                "        },\n" +
//                "        \"doc_values\": false\n" +
//                "    },\n" +
//                "    \"remark\": {\n" +
//                "        \"type\": \"keyword\",\n" +
//                "        \"doc_values\": false\n" +
//                "    }\n" +
//                "}";
//
//        CreateIndexRequest request = new CreateIndexRequest(indexName);
//        request.settings(Settings.builder().loadFromSource(indexSetting, XContentType.JSON).build());
//        Map<String, Object> mappingMap = JSONObject.parseObject(mappings, Map.class);
////        for(Map.Entry<String, Object> entry : mappingMap.entrySet()) {
////            Map<String, Object> val = (Map) entry.getValue();
////
////        }
//        request.mapping("_doc", mappingMap);
//
//        runner.createIndex(indexName, request);
//
//        // wait for yellow status
//        runner.ensureYellow();
//    }
//
//    private void insertData(String indexName, String id, String data) {
//        runner.insert(indexName, "_doc", id, data);
//        runner.flush();
//    }
//
//    @Test
//    public void createIndex() {
//        String index = "lccy_search_index";
//        // create an index
//        createIndexWithLocal(index);
//
//        insertData(index, "A1001", data1);
//
//        QueryBuilder ids = QueryBuilders.idsQuery().addIds("A1001");
//        SearchResponse response = runner.search(index, ids, null, 0, 1);
//
//        System.out.println(response.getHits());
//    }
//
//
//    @Test
//    public void testLocal() {
//        String index = "lccy_search_index";
//        // create an index
//        createIndexWithLocal(index);
//
//        insertData(index, "A1001", data1);
//
//        QueryBuilder ids = QueryBuilders.idsQuery().addIds("A1001");
//        SearchResponse response = runner.search(index, ids, null, 0, 1);
//
//        System.out.println(response.getHits());
//    }
//
//
//    public static final String data1 = "{\n" +
//            "    \"provCode\": \"250\",\n" +
//            "    \"provName\": \"江苏\",\n" +
//            "    \"title\": \"刘德华\",\n" +
//            "    \"currentPrice\": 600,\n" +
//            "    \"saleQuantity\": 200,\n" +
//            "    \"startTime\": \"2022-01-02 10:01:10\",\n" +
//            "    \"endTime\": \"2024-07-02 10:01:10\",\n" +
//            "    \"categoryCode\": \"F1001\",\n" +
//            "    \"state\": \"1\",\n" +
//            "    \"exposure\": 2500,\n" +
//            "    \"clickQuantity\": 1200,\n" +
//            "    \"playersNum\": 20,\n" +
//            "    \"readingsNum\": 100,\n" +
//            "    \"location\": 0,\n" +
//            "    \"cornerMark\": \"1,2\",\n" +
//            "    \"sourceProvCode\": \"250\",\n" +
//            "    \"iopType\": \"2\"\n" +
//            "}";
//
//}
