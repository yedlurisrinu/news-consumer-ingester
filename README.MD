# News Consumer Ingester

[![Build Status](https://github.com/username/repo/actions/workflows/main.yml/badge.svg)](https://github.com//repo/actions)

![License](https://img.shields.io/badge/license-MIT-blue)

The project is part of a multi-agent RAG application. It consumes news articles published to Confluent Kafka by 
the [news-fetch-scheduler](../news_fetch_scheduler) and ingests them — along with their full content and vector 
embeddings — into an Elasticsearch cloud cluster. Anyone looking to understand an end-to-end RAG ingestion pipeline 
can start here.
---

## TODOs
- CI pipeline completion (no deploy job).
---

## Demo

> Demo assets (recordings, screenshots, and step-by-step walkthrough scripts) are located in the [`demo/`](./demo/) folder.

The demo folder will contain:

| File                                                           | Description                                                      |
|----------------------------------------------------------------|------------------------------------------------------------------|
| `demo/elasticsearch-project_page.png`                          | Main page that describes storage, cluster information            |
| `demo/elasticsearch_document_count.png`                        | Number of new articles indexes in the elasticseach index         |
| `demo/elasticsearch_inference_field_query_with_embeddings.png` | Searching index to see text and embedding content                |
| `demo/elasticsearch_reciprocal_rank_fusion_query_results.png`  | Executing reciprocal rank fusion query to fetch relavent result  |
| `demo/embeddings_inference_endpoint.png`                       | Verifying inference end point settings and validatin it          |
| `demo/elastic_search_nested_doc_count_data_size.png`           | Total number of documents, including nested and total index size |
| `demo/elastic_consumption_units.png`                           | Total Elastic Consumtion units for the complete project          |
| `demo/services_running_on_locale_docker.png`                   | Service running on local docker instance                         |
| `demo/scheduler_ingester_pipeline_logs.png`                    | Consumer and ingester pipeline logs                              |

### Chat UI — News Search & Summary
![Chat UI Screenshot](demo/chat-ui-screenshot.png)

### End-to-End Pipeline Walkthrough
<video src="demo/pipeline-demo.webm" controls width="800">
  Your browser does not support the video tag.
</video>

> **Note:** Demo assets are stored in the `demo/` folder. To add your own, drop `.png` screenshots and `.webm` screen recordings there and update the paths above.

---

## Features
- Consumes news articles from Confluent Kafka in configurable bulk batches
- Fetches full article content via HTTP with BeautifulSoup parsing (skips social/video domains)
- Falls back to RSS summary when full-content fetch fails
- Bulk indexes articles into Elasticsearch with explicit `_id` to prevent duplicates
- Retry with exponential backoff for transient ES failures
- Manual Kafka offset commit — only after a batch is successfully indexed
- Secrets loaded from HashiCorp Vault at startup (no plaintext credentials)

---

## Tech Stack
- **Messaging:** Confluent Kafka (SASL_SSL)
- **Search / Vector Store:** Elasticsearch Cloud (semantic text embeddings)
- **HTTP client:** httpx + BeautifulSoup4
- **Runtime:** Python 3.12, Uvicorn
- **Secrets:** HashiCorp Vault
- **Container:** Docker (multi-stage build)

---

## End-to-End Pipeline

```
News Articles
      ↓
Kafka Producer  (news-fetch-scheduler)
  • Dedup check via MD5 hash backed by PostgreSQL
  • New articles pushed to Kafka topic
      ↓
Kafka Consumer  (this service — BulkMessageProcessor)
  • poll_batch()  — up to batch_size messages per cycle
  • Manual offset commit after successful indexing
  • Handles partial last batch gracefully
      ↓
Content Fetcher  (content_feed.fetch_full_article)
  • HTTP GET with User-Agent header
  • Strips nav / footer / script noise via BeautifulSoup
  • Caps content at 5 000 chars
  • Falls back to Kafka-supplied summary on error
      ↓
Elasticsearch Ingester  (elastic_ingester.bulk_index_articles)
  • Bulk indexing with explicit _id (idempotent)
  • chunk_size matches Kafka batch size
  • Retry with exponential backoff (initial 2s, max 60s, 3 retries)
  • http_compress=True for cloud efficiency
      ↓
Elasticsearch Index  (news_articles)
  • Strict mapping  (dynamic: strict)
  • Semantic text field on content  (multilingual-e5-small)
  • Optimised for hybrid keyword + vector search
```

---

## Elasticsearch Schema

The `news_articles` index uses a **strict dynamic mapping** to enforce data quality and enables **semantic search** via the Elasticsearch hosted 
multilingual-e5-small model.

### 1. Inference endpoint setup

```http
PUT _ml/trained_models/.multilingual-e5-small_linux-x86_64
{
  "input": { "field_names": ["text_field"] }
}

POST _ml/trained_models/.multilingual-e5-small_linux-x86_64/deployment/_start
{
  "number_of_allocations": 1,
  "threads_per_allocation": 1
}

PUT _inference/text_embedding/news-embedding-endpoint
{
  "service": "elasticsearch",
  "service_settings": {
    "model_id":        ".multilingual-e5-small_linux-x86_64",
    "num_allocations": 1,
    "num_threads":     1
  }
}
```

### 2. Index mapping

```http
PUT news_articles
{
  "settings": {
    "refresh_interval": "30s"
  },
  "mappings": {
    "dynamic": "strict",
    "properties": {
      "article_id":   { "type": "keyword" },
      "title":        { "type": "text", "analyzer": "english" },
      "content": {
        "type": "text",
        "analyzer": "english",
        "fields": {
          "semantic": {
            "type":         "semantic_text",
            "inference_id": "news-embedding-endpoint"
          }
        }
      },
      "link":         { "type": "keyword", "index": false },
      "published_at": {
        "type":   "date",
        "format": "strict_date_optional_time||epoch_millis"
      },
      "fetched_at": {
        "type":   "date",
        "format": "strict_date_optional_time||epoch_millis"
      },
      "source":       { "type": "keyword" }
    }
  }
}
```

### 3. Field reference

| Field              | Type            | Notes                                               |
|--------------------|-----------------|-----------------------------------------------------|
| `article_id`       | `keyword`       | MD5 hash from producer — used as `_id` (dedup key)  |
| `title`            | `text`          | English analyser for full-text search               |
| `content`          | `text`          | English analyser + semantic sub-field for vectors   |
| `content.semantic` | `semantic_text` | Auto-vectorised via inference endpoint              |
| `link`             | `keyword`       | Not indexed — stored only                           |
| `published_at`     | `date`          | ISO-8601 or epoch millis                            |
| `fetched_at`       | `date`          | ISO-8601 or epoch millis                            |
| `source`           | `keyword`       | Feed source name (e.g. ESPN, CBS News)              |

> **Serverless note:** `number_of_shards` and `number_of_replicas` are not available in serverless mode — omit them from the settings block when targeting a serverless project.

---

## Installation

1. Clone the repo
```bash
git clone https://github.com/syedluri/news-consumer-ingester.git
cd news-consumer-ingester
```

2. Install dependencies
```bash
pip install -r requirements.txt
pip install -r requirements_locales.txt   # local private packages
```

3. Set up environment variables (injected via Vault in production)
```bash
export ELASTIC_URL=https://<your-cluster>.es.io:443
export ELASTIC_API_KEY=<your-api-key>
export CONFLUENT.bootstrap.servers=<broker>:9092
export CONFLUENT.sasl.username=<key>
export CONFLUENT.sasl.password=<secret>
export CONFLUENT.client.id=news-consumer-ingester
export VAULT_ADDR=http://localhost:8200
export VAULT_TOKEN=<token>
```

4. Run the service
```bash
cd src && python main.py
```

---

## Running Tests

```bash
pytest --cov=src --cov-report=term-missing
```

Tests are in `tests/` and mirror the `src/` module structure with `_test` suffix:

```
tests/
├── config/
│   ├── confluent_config_test.py
│   ├── elastic_setup_test.py
│   └── settings_test.py
├── consumer/
│   └── bulk_message_processor_test.py
├── contentfeed/
│   └── content_feed_test.py
└── ingester/
    └── elastic_ingester_test.py
```

---

## Environment Variables

| Variable                        | Description                          | Required |
|---------------------------------|--------------------------------------|----------|
| `ELASTIC_URL`                   | Elasticsearch cluster URL            | Yes      |
| `ELASTIC_API_KEY`               | Elasticsearch API key                | Yes      |
| `CONFLUENT.bootstrap.servers`   | Kafka broker address                 | Yes      |
| `CONFLUENT.sasl.username`       | Confluent API key                    | Yes      |
| `CONFLUENT.sasl.password`       | Confluent API secret                 | Yes      |
| `CONFLUENT.client.id`           | Kafka client identifier              | Yes      |
| `VAULT_ADDR`                    | HashiCorp Vault server URL           | Yes      |
| `VAULT_TOKEN`                   | Vault auth token                     | Yes      |
| `VAULT_SECRET_PATH`             | Path to secrets in Vault             | Yes      |

---

## Key Design Decisions

| Problem                        | Solution                                                     |
|--------------------------------|--------------------------------------------------------------|
| Duplicate articles             | Explicit `_id` from MD5 hash — ES `index` op is idempotent   |
| Partial Kafka batches          | `batch_size` threshold with graceful last-batch handling     |
| ES certificate errors          | Separate HTTP/transport certs with SANs                      |
| Cloud connection timeouts      | `http_compress=True`, dynamic `chunk_size`, retry backoff    |
| Large article content          | HTTP fetch capped at 5 000 chars; BeautifulSoup noise strip  |
| Secrets management             | HashiCorp Vault — no plaintext credentials in config         |

---

## Docker

```bash
# Build and run
docker compose up --build

# Production image only
docker build -t news-consumer-ingester .
docker run --env-file .env news-consumer-ingester
```

---

## Contributing

Not yet open for contributions.

---

## License

Distributed under the MIT License.

---

## Author

**Srini Yedluri** — yedlurisrinu@gmail.com