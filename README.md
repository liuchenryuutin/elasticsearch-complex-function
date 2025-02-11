# Complex field function score for ElasticSearch

Custom score plugin, supports percentage merging with relevance scores.

## Version

| complex function score | ES version    |
|------------------------|---------------|
| master                 | 7.x -> master |

## Installation

1. `mvn package`

2. copy and unzip `target/releases/elasticsearch-complex-function-{version}.zip` to `your-es-root/plugins/elasticsearch-complex-function`

## Example

```json
{
  "from": 0,
  "size": 10,
  "explain": false,
  "query": {
    "function_score": {
      "query": {
        "bool": {
          "must": [
            {
              "multi_match": {
                "query": "刘德华",
                "fields": [
                  "title^2.0",
                  "titleFullPy^1.0",
                  "titleSimPy^1.0"
                ],
                "type": "best_fields",
                "operator": "OR",
                "slop": 0,
                "prefix_length": 0,
                "max_expansions": 50,
                "zero_terms_query": "NONE",
                "auto_generate_synonyms_phrase_query": true,
                "fuzzy_transpositions": true,
                "boost": 1.0
              }
            }
          ],
          "filter": [
            {
              "range": {
                "startTime": {
                  "from": null,
                  "to": "2023-05-10 16:54:20",
                  "include_lower": true,
                  "include_upper": true,
                  "boost": 1.0
                }
              }
            },
            {
              "range": {
                "endTime": {
                  "from": "2023-05-10 16:54:20",
                  "to": null,
                  "include_lower": true,
                  "include_upper": true,
                  "boost": 1.0
                }
              }
            }
          ],
          "adjust_pure_negative": true,
          "boost": 1.0
        }
      },
      "functions": [
        {
          "complex_field_score": {
            "category_field": "categoryCode.keyword",
            "field_mode": "sum",
            "fields_score": {
              "F1001,F1002,F1005": [
                {
                  "field": "exposure",
                  "factor": 0.125,
                  "modifier": "log1p",
                  "weight": "50"
                },
                {
                  "field": "currentPrice",
                  "factor": -0.2,
                  "modifier": "log1p",
                  "weight": "50",
                  "add_num": 1,
                  "missing": 0
                },
                {
                  "field": "location",
                  "factor": 1,
                  "modifier": "decaygeoexp",
                  "weight": "50",
                  "origin": "31,33",
                  "scale": "50km",
                  "offset": "500m",
                  "decay": 0.6
                }
              ]
            },
            "sort_base_score": 20000,
            "sort_score": {
              "F1006,F1002,F1004": [
                {
                  "weight": 1,
                  "field": "cornerMark",
                  "value": "1"
                },
                {
                  "weight": 2,
                  "field": "cornerMark",
                  "value": "2"
                },
                {
                  "weight": 3,
                  "field": "sourceProvCode.keyword",
                  "value": "9999"
                },
                {
                  "weight": 4,
                  "field": "sourceProvCode.keyword",
                  "type": "not",
                  "value": "9999"
                },
                {
                  "weight": 5,
                  "field": "iopType.keyword",
                  "value": "1"
                },
                {
                  "weight": 6,
                  "field": "iopType.keyword",
                  "value": "2"
                }
              ],
              "F1004": [
                {
                  "weight": 2,
                  "field": "cornerMark",
                  "value": "1"
                },
                {
                  "weight": 3,
                  "field": "cornerMark",
                  "value": "2"
                }
              ],
              "F1005,F1001": [
                {
                  "weight": 2,
                  "type": "any"
                }
              ],
              "F1008": [
                {
                  "weight": 9,
                  "field": "cornerMark&_&state",
                  "type": "equal&_&not_in",
                  "value": "1&_&2,3"
                }
              ]
            },
            "func_score_factor": 0.7,
            "original_score_factor": 0.3
          }
        }
      ],
      "score_mode": "sum",
      "boost_mode": "replace"
    }
  },
  "aggregations": {
    "categoryCode": {
      "filters": {
        "filters": {
          "F1001": {
            "bool": {
              "filter": [
                {
                  "terms": {
                    "categoryCode.keyword": [
                      "F1001",
                      "F1005",
                      "F1008"
                    ],
                    "boost": 1.0
                  }
                }
              ]
            }
          }
        },
        "other_bucket": false,
        "other_bucket_key": "_other_"
      },
      "aggregations": {
        "top": {
          "top_hits": {
            "from": 0,
            "size": 10,
            "version": false,
            "seq_no_primary_term": false,
            "explain": false,
            "_source": {
              "includes": [],
              "excludes": [
                "shieldInfo",
                "remark",
                "createTime",
                "startTime",
                "endTime",
                "titleFullPy",
                "atomTagFullPy",
                "atomTagSimPy",
                "titleSimPy",
                "titleStandardPy"
              ]
            },
            "sort": [
              {
                "_score": {
                  "order": "desc"
                }
              },
              {
                "updateTime": {
                  "order": "desc"
                }
              }
            ]
          }
        }
      }
    }
  }
}
```

## Java Example

```java

public class ESSearchTest {

    @Test
    public void Test_complex_function() {
        double func_score_factor = 0.7;
        double original_score_factor = 0.3;
        Map<String, CategoryScoreWapper> categoryMap = new HashMap<>();
        List<Map> categoryList = new ArrayList<>();
        Map<String, Object> category = new HashMap<>();
        category.put(CategoryScoreWapper.NAME, "F1001,F1002");
        category.put(CategoryScoreWapper.FILED_MODE, Constants.FieldMode.SUM);
        category.put(CategoryScoreWapper.SORT_MODE, Constants.SortMode.MAX);
        category.put(CategoryScoreWapper.SORT_BASE_SCORE, 20000);
        List<Map> fieldScoreList = new ArrayList<>();
        Map<String, Object> exposure = new HashMap<>();
        exposure.put(FieldScoreComputeWapper.FIELD, "exposure");
        exposure.put(FieldScoreComputeWapper.FACTOR, 0.125);
        exposure.put(FieldScoreComputeWapper.MODIFIER, FieldScoreComputeWapper.Modifier.LOG1P.toString());
        exposure.put(FieldScoreComputeWapper.WEIGHT, 50);
        exposure.put(FieldScoreComputeWapper.ADD_NUM, 0);
        exposure.put(FieldScoreComputeWapper.MISSING, "10");
        fieldScoreList.add(exposure);
        Map<String, Object> currentPrice = new HashMap<>();
        currentPrice.put(FieldScoreComputeWapper.FIELD, "price");
        currentPrice.put(FieldScoreComputeWapper.FACTOR, -0.2);
        currentPrice.put(FieldScoreComputeWapper.MODIFIER, FieldScoreComputeWapper.Modifier.LOG1P.toString());
        currentPrice.put(FieldScoreComputeWapper.WEIGHT, 50);
        currentPrice.put(FieldScoreComputeWapper.ADD_NUM, 1);
        currentPrice.put(FieldScoreComputeWapper.MISSING, "1");
        fieldScoreList.add(currentPrice);
        category.put(CategoryScoreWapper.FIELDS_SCORE, fieldScoreList);

        List<Map> sortScore = new ArrayList<>();
        Map<String, Object> sort1 = new HashMap<>();
        sort1.put(SortScoreComputeWapper.WEIGHT, "1");
        sort1.put(SortScoreComputeWapper.FIELD, "cornerMark");
        sort1.put(SortScoreComputeWapper.VALUE, "1");
        sortScore.add(sort1);
        Map<String, Object> sort2 = new HashMap<>();
        sort2.put(SortScoreComputeWapper.WEIGHT, "2");
        sort2.put(SortScoreComputeWapper.FIELD, "cornerMark");
        sort2.put(SortScoreComputeWapper.VALUE, "2");
        sortScore.add(sort2);
        Map<String, Object> sort3 = new HashMap<>();
        sort3.put(SortScoreComputeWapper.WEIGHT, "3");
        sort3.put(SortScoreComputeWapper.FIELD, "provCode");
        sort3.put(SortScoreComputeWapper.VALUE, "9999");
        sortScore.add(sort3);
        Map<String, Object> sort4 = new HashMap<>();
        sort4.put(SortScoreComputeWapper.WEIGHT, "4");
        sort4.put(SortScoreComputeWapper.FIELD, "provCode");
        sort4.put(SortScoreComputeWapper.TYPE, Constants.SortValueType.NOT);
        sort4.put(SortScoreComputeWapper.VALUE, "9999");
        sortScore.add(sort4);
        Map<String, Object> sort5 = new HashMap<>();
        sort5.put(SortScoreComputeWapper.WEIGHT, "5");
        sort5.put(SortScoreComputeWapper.FIELD, "type");
        sort5.put(SortScoreComputeWapper.VALUE, "1");
        sortScore.add(sort5);
        Map<String, Object> sort6 = new HashMap<>();
        sort6.put(SortScoreComputeWapper.WEIGHT, "6");
        sort6.put(SortScoreComputeWapper.FIELD, "type");
        sort6.put(SortScoreComputeWapper.VALUE, "2");
        sortScore.add(sort6);
        category.put(CategoryScoreWapper.SORT_SCORE, sortScore);
        categoryList.add(category);
        ComplexFieldFunctionBuilder builder = ComplexScoreFunctionBuilders.complexFieldFunction(func_score_factor, original_score_factor,
                categoryList, ComplexFieldFunctionBuilder.DEFAULT_CATEGORY);

        SearchRequest request = new SearchRequest(indexName);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        FunctionScoreQueryBuilder.FilterFunctionBuilder functionBuilder = new FunctionScoreQueryBuilder.FilterFunctionBuilder(builder);

        QueryBuilder queryBuilder = new MatchAllQueryBuilder();
        FunctionScoreQueryBuilder functionScoreQuery = new FunctionScoreQueryBuilder(queryBuilder, Stream.of(functionBuilder).toArray(FunctionScoreQueryBuilder.FilterFunctionBuilder::new));
        functionScoreQuery.boostMode(CombineFunction.SUM);
        functionScoreQuery.scoreMode(FunctionScoreQuery.ScoreMode.SUM);

        sourceBuilder.from(0).size(10);
        sourceBuilder.query(functionScoreQuery);
        request.source(sourceBuilder);

        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
    }
}


```