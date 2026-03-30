GET _cat/indices?v&h=health,status,index,pri,rep,docs.count,store.size
GET _inference/text_embedding/_all
GET _ml/trained_models
#Downloading a trained model
PUT _ml/trained_models/.multilingual-e5-small_linux-x86_64
{
  "input": {
    "field_names": ["text_field"]
  }
}

# Deploy it
POST _ml/trained_models/.multilingual-e5-small_linux-x86_64/deployment/_start
{
  "number_of_allocations": 1,
  "threads_per_allocation": 1
}

GET _ml/trained_models/.multilingual-e5-small_linux-x86_64/_stats

PUT _inference/text_embedding/news-embedding-endpoint
{
  "service": "elasticsearch",
  "service_settings": {
    "model_id":              ".multilingual-e5-small_linux-x86_64",
    "num_allocations":       1,
    "num_threads":           1
  }
}

POST _inference/text_embedding/news-embedding-endpoint
{
  "input": "Latest news about AI technology"
}


PUT news_articles
{
    "settings": {   
        "refresh_interval": "30s"
    },
    "mappings": {
        "dynamic": "strict",             
        "properties": {
            "article_id": {
                "type": "keyword"       
            },
            "title": {
                "type": "text",
                "analyzer": "english"
            },
            "content": {
                "type": "text",
                "analyzer": "english",
                "fields": {
                    "semantic":{
                        "type" : "semantic_text",
                        "inference_id":"news-embedding-endpoint"
                    }
                }
            },
            "link": {
                "type": "keyword",
                "index": false    
            },
            "published_at": {
                "type": "date",
                "format": "strict_date_optional_time||epoch_millis"
            },
            "fetched_at": {
                "type": "date",
                "format": "strict_date_optional_time||epoch_millis"
                },
            "source": {
                "type": "keyword"
            }
        }
    }
}

//"number_of_shards": 1,
    //    "number_of_replicas": 0, 

{
  "error": {
    "root_cause": [
      {
        "type": "illegal_argument_exception",
        "reason": "Settings [index.number_of_replicas,index.number_of_shards] are not available when running in serverless mode"
      }
    ],
    "type": "illegal_argument_exception",
    "reason": "Settings [index.number_of_replicas,index.number_of_shards] are not available when running in serverless mode"
  },
  "status": 400
}

Creating Alias and settings is pending


POST _reindex?wait_for_completion=false
{
    "source": {
        "index":"news_articles"
    },
    "dest": {
        "index":"news_articles_v1"
    }
}

GET _tasks/6wH6JkbxRLu-cJK4i1VhIA:3256250
GET news_articles_v1/_search
GET news_articles_v1/_search
{
  "size": 1,
  "_source": ["content","_inference_fields"]
}
GET news_articles_v1/_count
GET _cat/indices?v&h=health,status,index,pri,rep,docs.count,dataset.size
POST _aliases
{
    "actions": [
      {
        "add": {
          "index": "news_articles_v1",
          "alias": "news_articles"
        }
      }
    ]
}


GET /news_articles_v1/_search
{
  "query": {
    "semantic": {
      "field": "content.semantic",
      "query": "latest news in food"
    }
  }
}

GET /news_articles/_search
{
  "retriever": {
    "rrf": {
      "retrievers": [
        {
          "standard": {
            "query": {
              "match": {
                "content": "lastest news in technology food"
              }
            }
          }
        },
        {
          "standard": {
            "query": {
              "semantic": {
                "field": "content.semantic",
                "query": "latest news in technology food"
              }
            }
          }
        }
      ]
    }
  }
}