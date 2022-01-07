# 신규 filed 추가에 따른 index mapping 수정/reindex/migration 수행 절차

수행일 - dev: 2021.12.08

- 기존 index에 신규 필드 맵핑
  - 작업 index: hourly-news-summary, daily-news-summary, basic-news-info
  - 작업 내용: 
    - news_id가 실제 JTBC 방송 프로그램에 노출 된 경우, prog_id, prog_name, contents_div를 추가 
  - 작업 절차:
    1. 신규 index(+mapping) 생성
    2. reindex
    3. 2021.10.22일 부터 데이터 migration 수행
    4. alias 적용
    5. 기존 job repeat 수행

## MappingJTBCTVProRunner를 crontab에 등록

1. index 확인:
    1. get _cat/indices/origin-mapping-tv-news-program-*
    
    ```bash
    # 모든 index template 검색
    GET /_index_template
    
    # daily-*로 시작되는 index template 검색
    GET /_index_template/mapping-*
    
    // index_template 지우기
    DELETE /_index_template/daily-summary-stat-template
    ```
    
2. kibana index pattern 등록 및 권한 적용
2. kibana index pattern 등록 및 권한 적용
    1. alias 기반으로 등록: 
        1. mapping-tv-news-program-*
    2. role 적용
        1. [DEV] shake_member에 mapping-tv-news-program-* 추가
3. 데이터 migration 수행
    1. vi test/main/job/test_MappingJTBCTVProRunner.py 에서 test_old_runner를 2021/10/22 부터 수행
4. discover를 통해 데이터 확인 + mapping 확인
    1. dev Tools
        1. get origin-mapping-tv-news-program-2021/_mapping
    2. discover 또는 dev 에서 쿼리로 확인
        1. 2021년 10월 22일부터 데이터 확인
5. dev tool
            
    ```bash
    get origin-mapping-tv-news-program-2021/_search
    {
      "size": 10,
      "query": {
        "match_all": {}
      },
      "sort": [
        {
          "update_date": {
            "order": "desc"
          }
        }
      ]
    }
    ```
            

## hourly-news-summary, daily-news-summary, basic-news-info에 신규 filed 추가로 인한 작업 수행

1. 각 index 별로 존재하는 index 확인 및 실제 alias 확인
    - hourly-news-summary
        
        ```python
        get _cat/indices/*hourly-news-summary*
        get _alias/*hourly-news-summary*
        ```
        
    - daily-news-summary
        
        ```python
        get _cat/indices/*daily-news-summary*
        get _alias/*daily-news-summary*
        ```
        
    - basic-news-info
        
        ```python
        get _cat/indices/*basic-news-info*
        get _alias/*basic-news-info*
        ```
        
2. 신규 필드 추가한 index 생성(+ mapping)
    - hourly-news-summary
        
        ```python
        PUT origin-hourly-news-summary-2021.12.08
        {
          "settings": {
            "number_of_shards": **[PROD 2, DEV 1]**
          },
          "mappings": {
            "properties" : {
              "comment_total" : {
                "type" : "long"
              },
              "contents_div" : {
                "type" : "keyword"
              },
              "jtbc" : {
                "properties" : {
                  "app_view" : {
                    "type" : "integer"
                  },
                  "comment" : {
                    "type" : "integer"
                  },
                  "mobile_view" : {
                    "type" : "integer"
                  },
                  "pc_view" : {
                    "type" : "integer"
                  },
                  "reaction" : {
                    "type" : "integer"
                  },
                  "view" : {
                    "type" : "integer"
                  }
                }
              },
              "kakao" : {
                "properties" : {
                  "comment" : {
                    "type" : "long"
                  },
                  "mobile_view" : {
                    "type" : "integer"
                  },
                  "pc_view" : {
                    "type" : "integer"
                  },
                  "reaction" : {
                    "type" : "long"
                  },
                  "view" : {
                    "type" : "long"
                  }
                }
              },
              "naver" : {
                "properties" : {
                  "click" : {
                    "type" : "long"
                  },
                  "comment" : {
                    "type" : "long"
                  },
                  "mobile_view" : {
                    "type" : "integer"
                  },
                  "pc_view" : {
                    "type" : "integer"
                  },
                  "reaction" : {
                    "type" : "long"
                  },
                  "view" : {
                    "type" : "long"
                  }
                }
              },
              "news_id" : {
                "type" : "keyword"
              },
              "news_name" : {
                "type" : "keyword"
              },
              "prog_id" : {
                "type" : "keyword"
              },
              "prog_name" : {
                "type" : "keyword"
              },
              "reaction_total" : {
                "type" : "long"
              },
              "reg_date" : {
                "type" : "date"
              },
              "update_date" : {
                "type" : "date"
              },
              "reporter" : {
                "properties" : {
                  "depart_name" : {
                    "type" : "keyword"
                  },
                  "depart_no" : {
                    "type" : "keyword"
                  },
                  "name" : {
                    "type" : "keyword"
                  },
                  "no" : {
                    "type" : "keyword"
                  }
                }
              },
              "section" : {
                "properties" : {
                  "id" : {
                    "type" : "keyword"
                  },
                  "name" : {
                    "type" : "keyword"
                  }
                }
              },
              "service_date" : {
                "type" : "date"
              },
              "source" : {
                "properties" : {
                  "code" : {
                    "type" : "keyword"
                  },
                  "id" : {
                    "type" : "text",
                    "fields" : {
                      "keyword" : {
                        "type" : "keyword",
                        "ignore_above" : 256
                      }
                    }
                  },
                  "name" : {
                    "type" : "keyword"
                  }
                }
              },
              "view_total" : {
                "type" : "long"
              }
            }
          }
        }
        ```
        
    - daily-news-summary
        
        ```python
        PUT origin-daily-news-summary-2021.12.08
        {
          "settings": {
            "number_of_shards": **[PROD 2, DEV 1]**
          },
          "mappings": {
            "properties" : {
              "comment_total" : {
                "type" : "long"
              },
              "contents_div" : {
                "type" : "keyword"
              },
              "jtbc" : {
                "properties" : {
                  "app_view" : {
                    "type" : "integer"
                  },
                  "comment" : {
                    "type" : "integer"
                  },
                  "mobile_view" : {
                    "type" : "integer"
                  },
                  "pc_view" : {
                    "type" : "integer"
                  },
                  "reaction" : {
                    "type" : "integer"
                  },
                  "view" : {
                    "type" : "integer"
                  }
                }
              },
              "kakao" : {
                "properties" : {
                  "comment" : {
                    "type" : "long"
                  },
                  "mobile_view" : {
                    "type" : "integer"
                  },
                  "pc_view" : {
                    "type" : "integer"
                  },
                  "reaction" : {
                    "type" : "long"
                  },
                  "view" : {
                    "type" : "long"
                  }
                }
              },
              "naver" : {
                "properties" : {
                  "click" : {
                    "type" : "long"
                  },
                  "comment" : {
                    "type" : "long"
                  },
                  "mobile_view" : {
                    "type" : "integer"
                  },
                  "pc_view" : {
                    "type" : "integer"
                  },
                  "reaction" : {
                    "type" : "long"
                  },
                  "view" : {
                    "type" : "long"
                  }
                }
              },
              "news_id" : {
                "type" : "keyword"
              },
              "news_name" : {
                "type" : "keyword"
              },
              "prog_id" : {
                "type" : "keyword"
              },
              "prog_name" : {
                "type" : "keyword"
              },
              "reaction_total" : {
                "type" : "long"
              },
              "reg_date" : {
                "type" : "date"
              },
              "reporter" : {
                "properties" : {
                  "depart_name" : {
                    "type" : "keyword"
                  },
                  "depart_no" : {
                    "type" : "keyword"
                  },
                  "name" : {
                    "type" : "keyword"
                  },
                  "no" : {
                    "type" : "keyword"
                  }
                }
              },
              "section" : {
                "properties" : {
                  "id" : {
                    "type" : "keyword"
                  },
                  "name" : {
                    "type" : "keyword"
                  }
                }
              },
              "service_date" : {
                "type" : "date"
              },
              "source" : {
                "properties" : {
                  "code" : {
                    "type" : "keyword"
                  },
                  "id" : {
                    "type" : "text",
                    "fields" : {
                      "keyword" : {
                        "type" : "keyword",
                        "ignore_above" : 256
                      }
                    }
                  },
                  "name" : {
                    "type" : "keyword"
                  }
                }
              },
              "view_total" : {
                "type" : "long"
              },
        			"update_date": {
        				"type" : "date"
        			}
            }
          }
        }
        ```
        
    - basic-news-summary
        
        ```python
        PUT origin-basic-news-info-2021.12.08
        {
          "settings": {
            "number_of_shards": **[PROD 2, DEV 1]**
          },
          "mappings": {
            "properties": {
                "contents_div" : {
                  "type" : "keyword"
                },
                "modified_date" : {
                  "type" : "date"
                },
                "news_id" : {
                  "type" : "keyword"
                },
                "news_name" : {
                  "type" : "keyword"
                },
                "prog_id" : {
                  "type" : "keyword"
                },
                "prog_name" : {
                  "type" : "keyword"
                },
                "reporter" : {
                  "properties" : {
                    "depart_name" : {
                      "type" : "keyword"
                    },
                    "depart_no" : {
                      "type" : "keyword"
                    },
                    "name" : {
                      "type" : "keyword"
                    },
                    "no" : {
                      "type" : "keyword"
                    }
                  }
                },
                "section" : {
                  "properties" : {
                    "id" : {
                      "type" : "keyword"
                    },
                    "name" : {
                      "type" : "keyword"
                    }
                  }
                },
                "service_date" : {
                  "type" : "date"
                },
                "source" : {
                  "properties" : {
                    "code" : {
                      "type" : "keyword"
                    },
                    "id" : {
                      "type" : "text",
                      "fields" : {
                        "keyword" : {
                          "type" : "keyword",
                          "ignore_above" : 256
                        }
                      }
                    },
                    "name" : {
                      "type" : "keyword"
                    }
                  }
                },
                "used_yn" : {
                  "type" : "keyword"
                },
                "update_date" : {
                  "type" : "date"
                }
              }
          }
        }
        ```
        
3. reindex 작업 수행 ⇒ 데이터 원천꺼 copy의 의미임
    1. reindex 명령어를 post로 수행 후, task _id가 나오면 해당 task_id 기준으로 task status확인 가능
        1. GET _tasks
        2. GET _tasks/{실제 task_id}
            1. 참고: [https://www.elastic.co/guide/en/elasticsearch/reference/current/tasks.html](https://www.elastic.co/guide/en/elasticsearch/reference/current/tasks.html)
    2. reindex로 데이터 잘 들어왔는지 확인 및 추가 filed에 대한 type 확인하기
        - origin-hourly-news-summary-2021.12.08
            
            ```python
            get _alias/*hourly-news-summary*
            
            POST _reindex?wait_for_completion=false
            {
              "source": {
                "index": "hourly-news-summary-2021.09.27"
              },
              "dest": {
                "index": "origin-hourly-news-summary-2021.12.08"
              }
            }
            # 위 reindex를 통해 전달 받은 task ID를 기반으로 task completed 체크
            get _tasks/UE6lnnACRme72S2lNfrkJA:58187829
            
            get origin-hourly-news-summary-2021.12.08/_mapping
            get origin-hourly-news-summary-2021.12.08/_search
            get origin-hourly-news-summary-2021.12.08/_search
            {
              "size": 10,
              "query": {
                "match_all": {}
              },
              "sort": [
            	{
            	    "update_date": {
            		    "order": "desc"
            		}
            	}
              ]
            }
            ```
            
        - origin-daily-news-summary-2021.12.08
            
            ```python
            get _alias/*daily-news-summary*
            
            POST _reindex?wait_for_completion=false
            {
              "source": {
                "index": "origin-daily-news-summary-2021.12.03"
              },
              "dest": {
                "index": "origin-daily-news-summary-2021.12.08"
              }
            }
            
            # 위 reindex를 통해 전달 받은 task ID를 기반으로 task completed 체크
            get _tasks/UE6lnnACRme72S2lNfrkJA:58187829
            
            get origin-daily-news-summary-2021.12.08/_mapping
            get origin-daily-news-summary-2021.12.08/_search
            get origin-daily-news-summary-2021.12.08/_search
            {
              "size": 10,
              "query": {
                "match_all": {}
              },
              "sort": [
            		{
            			"update_date": {
            				"order": "desc"
            			}
            		}
            	]
            }
            ```
            
        - origin-basic-news-info-2021.12.08
            
            ```python
            get _alias/*basic-news-info*
            
            POST _reindex?wait_for_completion=false
            {
              "source": {
                "index": "basic-news-info-2021.10.05"
              },
              "dest": {
                "index": "origin-basic-news-info-2021.12.08"
              }
            }
            
            # 위 reindex를 통해 전달 받은 task ID를 기반으로 task completed 체크
            get _tasks/UE6lnnACRme72S2lNfrkJA:58187829
            
            get origin-basic-news-info-2021.12.08/_mapping
            get origin-basic-news-info-2021.12.08/_search
            get origin-basic-news-info-2021.12.08/_search
            {
              "size": 10,
              "query": {
                "match_all": {}
              },
              "sort": [
            		{
            			"service_date": {
            				"order": "desc"
            			}
            		}
            	]
            }
            ```
            
4. migration 수행 (⇒ reindex된 index로 migration 수행)
    - hourly-news-summary
        
        데이터 migration 수행
        
        ```python
        # DEV or PROD terminal에서 수행
        
        # test/main/aggregation/test_JTBCData.py 안에서
        #  - test_repeat_mapping_prog_ids_for_news_in_hourly_news_summary의
        #  - index name 및 datetime 확인
        $ vi test/main/aggregation/test_JTBCData.py
        $ python3 test/main/aggregation/test_JTBCData.py
        ```
        
        migration 데이터 확인
        
        ```python
        # 원천 index 최신 데이터 쌓였는지 확인
        get origin-hourly-news-summary-2021.12.08/_search
        {
          "size": 10,
          "query": {
            "match_all": {}
          },
          "sort": [
        		{
        			"reg_date": {
        				"order": "desc"
        			}
        		}
        	]
        }
        
        # 또는 터미널에서 upsert되고 있는 로그를 확인하여 그걸 기반으로
        GET origin-hourly-news-summary-2021.12.08/_search
        {
          "size": 100,
          "query": {
            "bool": {
              "must": [
                {
                  "term": {
                    "news_id": {
                      "value": "NB12030936"
                    }
                  }
                }
              ],
              "filter": [
                {
                  "range": {
                    "reg_date": {
                      "gte": "2021-11-14T00:00:00+09:00",
                      "lte": "2021-11-14T23:59:59+09:00"
                    }
                  }
                }
              ]
            }
            
          }
        }
        
        # 추가한 filed 데이터 확인
        GET origin-hourly-news-summary-2021.12.08/_search
        {
          "size": 100,
          "query": {
            "bool": {
              "must": [
                {
                  "exists": {
                    "field": "prog_id"
                  }
                }
              ]
            }
          },
          "sort": [
            {
              "reg_date": {
                "order": "desc"
              }
            }
          ]
        }
        ```
        
    - daily-news-summary
        
        데이터 migration 수행
        
        ```python
        # DEV or PROD terminal에서 수행
        
        # test/main/module/test_NewsSummary.py 안에서
        #  - test_repeat_mapping_prog_ids_for_news_in_daily_news_summary의
        #  - index name 및 datetime 확인
        $ vi test/main/module/test_NewsSummary.py
        $ python3 test/main/module/test_NewsSummary.py
        ```
        
        migration 데이터 확인
        
        ```python
        # 원천 index 최신 데이터 쌓였는지 확인
        get origin-daily-news-summary-2021.12.08/_search
        {
          "size": 10,
          "query": {
            "match_all": {}
          },
          "sort": [
        		{
        			"reg_date": {
        				"order": "desc"
        			}
        		}
        	]
        }
        
        # 또는 터미널에서 upsert되고 있는 로그를 확인하여 그걸 기반으로
        GET origin-daily-news-summary-2021.12.08/_search
        {
          "size": 100,
          "query": {
            "bool": {
              "must": [
                {
                  "term": {
                    "news_id": {
                      "value": "NB12031080"
                    }
                  }
                }
              ],
              "filter": [
                {
                  "range": {
                    "reg_date": {
                      "gte": "2021-11-09T00:00:00+09:00",
                      "lte": "2021-11-11T23:59:59+09:00"
                    }
                  }
                }
              ]
            }
            
          }
        }
        
        # 추가한 filed 데이터 확인
        GET origin-daily-news-summary-2021.12.08/_search
        {
          "size": 100,
          "query": {
            "bool": {
              "must": [
                {
                  "exists": {
                    "field": "prog_id"
                  }
                }
              ]
            }
          },
          "sort": [
            {
              "reg_date": {
                "order": "desc"
              }
            }
          ]
        }
        ```
        
    - basic-news-info
        
        데이터 migration 수행
        
        ```python
        # DEV or PROD terminal에서 수행
        
        # test/main/module/test_UpdateNewsInfo.py 안에서
        #  - test_repeat_mapping_prog_ids_for_news_in_basic_news_info의
        #  - index name 및 datetime 확인
        $ vi test/main/module/test_UpdateNewsInfo.py
        $ python3 test/main/module/test_UpdateNewsInfo.py
        ```
        
        migration 데이터 확인
        
        ```python
        # 원천 index 최신 데이터 쌓였는지 확인
        get origin-basic-news-info-2021.12.08/_search
        {
          "size": 10,
          "query": {
            "match_all": {}
          },
          "sort": [
        		{
        			"reg_date": {
        				"order": "desc"
        			}
        		}
        	]
        }
        
        # 또는 터미널에서 upsert되고 있는 로그를 확인하여 그걸 기반으로
        GET origin-basic-news-info-2021.12.08/_search
        {
          "size": 100,
          "query": {
            "bool": {
              "must": [
                {
                  "term": {
                    "news_id": {
                      "value": "NB12028379"
                    }
                  }
                }
              ],
              "filter": [
                {
                  "range": {
                    "service_date": {
                      "gte": "2021-10-25T00:00:00+09:00",
                      "lte": "2021-10-26T23:59:59+09:00"
                    }
                  }
                }
              ]
            }
            
          }
        }
        
        # 원천 index 최신 데이터 쌓였는지 확인
        get origin-basic-news-info-2021.12.08/_search
        {
          "size": 10,
          "query": {
            "match_all": {}
          },
          "sort": [
        		{
        			"service_date": {
        				"order": "desc"
        			}
        		}
        	]
        }
        ```
        
5. alias 변경
    - hourly-news-summary
        
        ```python
        # alias 확인
        get _alias/*hourly-news-summary*
        
        # alias 변경
        POST _aliases
        {
            "actions": [
                {
                    "add": {
                        "index": "origin-hourly-news-summary-2021.12.08",
                        "alias": "hourly-news-summary",
                        "is_write_index": true
                    }
                },
                {
                    "remove": {
                        "index": "hourly-news-summary-2021.09.27",
                        "alias": "hourly-news-summary"
                    }
                }
            ]
        }
        
        # alias로 검색 데이터 확인
        get hourly-news-summary/_search
        {
          "size": 10,
          "query": {
            "match_all": {}
          },
          "sort": [
        		{
        			"reg_date": {
        				"order": "desc"
        			}
        		}
        	]
        }
        
        # alias로 특정 데이터 필드 있는지 확인
        GET hourly-news-summary/_search
        {
          "size": 100,
          "query": {
            "bool": {
              "must": [
                {
                  "exists": {
                    "field": "prog_id"
                  }
                }
              ]
            }
          },
          "sort": [
            {
              "update_date": {
                "order": "desc"
              }
            }
          ]
        }
        ```
        
    - daily-news-summary
        
        ```python
        # alias 확인
        get _alias/*daily-news-summary*
        
        # alias 변경
        POST _aliases
        {
            "actions": [
                {
                    "add": {
                        "index": "origin-daily-news-summary-2021.12.08",
                        "alias": "daily-news-summary",
                        "is_write_index": true
                    }
                },
                {
                    "remove": {
                        "index": "daily-news-summary-2021.12.01",
                        "alias": "daily-news-summary"
                    }
                }
            ]
        }
        
        # alias로 검색 데이터 확인
        get daily-news-summary/_search
        {
          "size": 10,
          "query": {
            "match_all": {}
          },
          "sort": [
        		{
        			"reg_date": {
        				"order": "desc"
        			}
        		}
        	]
        }
        
        # alias로 특정 데이터 필드 있는지 확인
        GET daily-news-summary/_search
        {
          "size": 100,
          "query": {
            "bool": {
              "must": [
                {
                  "exists": {
                    "field": "prog_id"
                  }
                }
              ]
            }
          },
          "sort": [
            {
              "update_date": {
                "order": "desc"
              }
            }
          ]
        }
        
        get daily-news-summary/_search
        {
          "size": 100,
          "query": {
            "bool": {
              "must": [
                {
                  "term": {
                    "news_id": {
                      "value": "NB12031080"
                    }
                  }
                }
              ],
              "filter": [
                {
                  "range": {
                    "reg_date": {
                      "gte": "2021-11-09T00:00:00+09:00",
                      "lte": "2021-11-11T23:59:59+09:00"
                    }
                  }
                }
              ]
            }
            
          }
        }
        ```
        
    - basic-news-info
        
        ```python
        # alias 확인
        get _alias/*basic-news-info*
        
        # alias 변경
        POST _aliases
        {
            "actions": [
                {
                    "add": {
                        "index": "origin-basic-news-info-2021.12.08",
                        "alias": "basic-news-info",
                        "is_write_index": true
                    }
                },
                {
                    "remove": {
                        "index": "basic-news-info-2021.10.05",
                        "alias": "basic-news-info"
                    }
                }
            ]
        }
        
        # alias로 검색 데이터 확인
        get basic-news-info/_search
        {
          "size": 10,
          "query": {
            "match_all": {}
          },
          "sort": [
        		{
        			"service_date": {
        				"order": "desc"
        			}
        		}
        	]
        }
        
        # alias로 특정 데이터 필드 있는지 확인
        GET basic-news-info/_search
        {
          "size": 100,
          "query": {
            "bool": {
              "must": [
                {
                  "term": {
                    "news_id": {
                      "value": "NB12028379"
                    }
                  }
                }
              ],
              "filter": [
                {
                  "range": {
                    "service_date": {
                      "gte": "2021-10-25T00:00:00+09:00",
                      "lte": "2021-10-26T23:59:59+09:00"
                    }
                  }
                }
              ]
            }
            
          }
        }
        
        # alias로 특정 데이터 필드 있는지 확인
        GET basic-news-info/_search
        {
          "size": 100,
          "query": {
            "bool": {
              "must": [
                {
                  "exists": {
                    "field": "prog_id"
                  }
                }
              ]
            }
          },
          "sort": [
            {
              "service_date": {
                "order": "desc"
              }
            }
          ]
        }
        ```
        
6. 실제 job repeat 1days 적용
    - hourly-news-summary
        
        ```python
        [asmanager@jtbcv127 jnd-batch]$ vi test/main/job/test_NewsSummary.py
        [asmanager@jtbcv127 jnd-batch]$ python3 test/main/job/test_NewsSummary.py
        ```
        
    - daily-news-summary
        
        ```python
        [asmanager@jtbcv127 jnd-batch]$ vi test/main/job/test_NewsSummary.py
        [asmanager@jtbcv127 jnd-batch]$ python3 test/main/job/test_NewsSummary.py
        ```
        
    - basic-news-info
        
        ```python
        [asmanager@jtbcv127 jnd-batch]$ vi test/main/job/test_ProduceNewsSummary.py
        [asmanager@jtbcv127 jnd-batch]$ python3 test/main/job/test_ProduceNewsSummary.py
        ```