# Complex field function score for ElasticSearch

The dynamic synonym plugin adds a synonym token filter that reloads the synonym file (local file or remote file) at given intervals (default 60s).

## Version

complex function score | ES version
-----------|-----------
master| 7.x -> master

## Installation

1. `mvn package`

2. copy and unzip `target/releases/elasticsearch-complex-function-{version}.zip` to `your-es-root/plugins/elasticsearch-complex-function`

## Example

```json
{
  "from": 0,
  "size": 10,
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
            "categorys": [
              {
                "name": "F1001,F1002",
                "filed_mode": "sum",
                "fields_score": [
                  {
                    "field": "exposure",
                    "factor": 0.125,
                    "modifier": "log1p",
                    "weight": "50",
                    "add_num": 0,
                    "missing": 0
                  },
                  {
                    "field": "currentPrice",
                    "factor": -0.2,
                    "modifier": "log1p",
                    "weight": "50",
                    "add_num": 1,
                    "missing": 0
                  }
                ],
                "sort_mode": "max",
                "sort_base_socre": 20000,
                "sort_score": [
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
                    "field": "sourceProvCode",
                    "value": "9999"
                  },
                  {
                    "weight": 4,
                    "field": "sourceProvCode",
                    "type": "not",
                    "value": "9999"
                  },
                  {
                    "order": 5,
                    "field": "iopType",
                    "value": "1"
                  },
                  {
                    "weight": 6,
                    "field": "iopType",
                    "value": "2"
                  }
                ]
              }
            ],
            "func_score_factor": 0.7,
            "original_score_factor": 0.3
          }
        }
      ],
      "score_mode": "sum",
      "boost_mode": "replace"
    }
  }
}
```