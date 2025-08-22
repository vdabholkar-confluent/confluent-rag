# 🚀 Confluent Documentation RAG System

A real-time RAG (Retrieval-Augmented Generation) application that scrapes web content, processes it through Confluent Cloud's AI-powered streaming pipeline, and provides intelligent question-answering capabilities over Confluent documentation.

This is a real-world example of how to build real-time RAG pipelines using **Confluent Kafka**, **Confluent Flink**, **OpenAI**, and **MongoDB Atlas**.

## ✨ Features

- **🧠 Smart Documentation Assistant**: Interactive chat interface with iterative query refinement
- **🕷️ Advanced Web Scraper**: High-quality content extraction and processing  
- **⚡ Real-time AI Processing**: Confluent Flink + OpenAI integration for live summarization and embeddings
- **🔍 Vector Search**: MongoDB Atlas vector search on AI-generated summaries
- **📊 Processing Analytics**: Track processed URLs and system performance
- **🎯 Streamlit UI**: Beautiful web interface for easy interaction

## 🏗️ Architecture Overview

![FluentRAG Architecture](FluentRAG_Architecture.jpg)

```
User Input → Web Scraper → Kafka → Flink AI Processing → MongoDB → RAG Assistant
                                      ↓
                                  OpenAI Models:
                                  • Document Summarizer (GPT-4o-mini)
                                  • Embedding Generator (text-embedding-ada-002)
```

## 📋 Prerequisites

Before you begin, ensure you have:

- **Python 3.8+** installed
- **Git** installed
- **Docker** installed (for the web scraper service)
- **Confluent Cloud** account
- **OpenAI API** account
- **MongoDB Atlas** account

## 🚀 Quick Start

### 1. Clone the Repository

```bash
git clone <your-repo-url>
cd flink_app_2
```

### 2. Install Python Dependencies

```bash
# Create and activate virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

**Note**: If `requirements.txt` doesn't exist, install these packages manually:

```bash
pip install streamlit confluent-kafka openai pymongo httpx tenacity
```

### 3. Set Up Required Services

#### 🔑 OpenAI API Setup

1. Go to [OpenAI Platform](https://platform.openai.com/)
2. Create an account or sign in
3. Navigate to **API Keys** section
4. Create a new API key
5. Copy the key (starts with `sk-proj-`)

#### ☁️ Confluent Cloud Setup

1. Go to [Confluent Cloud](https://confluent.cloud/)
2. Create an account or sign in
3. Create a new **Environment** and **Cluster**
4. Navigate to **API Keys** section
5. Create API keys for:
   - **Kafka Cluster** (for producing messages)
   - **Schema Registry** (for data schemas)
6. Note down:
   - Bootstrap servers (e.g., `pkc-xxxxx.region.aws.confluent.cloud:9092`)
   - API Key and Secret
   - Schema Registry URL

#### 🗄️ MongoDB Atlas Setup

1. Go to [MongoDB Atlas](https://www.mongodb.com/atlas)
2. Create an account or sign in
3. Create a new **Cluster**
4. Create a **Database User** with read/write permissions
5. Get your **Connection String** (looks like `mongodb+srv://username:password@cluster.mongodb.net/`)
6. Create a **Database** named `confluent_embeddings`
7. Create a **Collection** named `confluent_docs`

#### 🕷️ Web Scraper Service Setup

1. Clone the eGet Crawler repository:
```bash
git clone https://github.com/vishwajeetdabholkar/eGet-Crawler-for-ai
cd eGet-Crawler-for-ai
```

2. Start the Docker service:
```bash
docker-compose up -d
```

3. Verify the service is running at `http://localhost:8000`

### 4. Configure the Application

1. Copy the example configuration:
```bash
cp config.json.example config.json
```

**⚠️ Security Note**: The `mongo_sink.json` file contains real credentials and should NOT be committed to version control. Use `mongo_sink.example.json` as a template and create your own configuration file locally.

2. Edit `config.json` with your credentials:

```json
{
  "kafka_bootstrap_servers": "YOUR_KAFKA_BOOTSTRAP_SERVERS",
  "kafka_api_key": "YOUR_KAFKA_API_KEY",
  "kafka_api_secret": "YOUR_KAFKA_API_SECRET",
  "kafka_topic": "url_markdown_raw",
  "schema_registry_url": "YOUR_SCHEMA_REGISTRY_URL",
  "schema_registry_api_key": "YOUR_SR_API_KEY",
  "schema_registry_api_secret": "YOUR_SR_API_SECRET",
  "openai_api_key": "YOUR_OPENAI_API_KEY",
  "mongodb_uri": "YOUR_MONGODB_CONNECTION_STRING",
  "mongodb_database": "confluent_embeddings",
  "mongodb_collection": "confluent_docs",
  "scraper_api_url": "http://localhost:8000/api/v1/scrape",
  "function_model": "gpt-4o",
  "response_model": "gpt-4o-mini",
  "embedding_model": "text-embedding-ada-002"
}
```

### 5. Set Up Confluent Flink Pipeline

#### Create Kafka Topic

1. In Confluent Cloud Console, go to **Topics**
2. Create a new topic named `url_markdown_raw`
3. Set **Partitions** to 3
4. Set **Replication Factor** to 3

#### Create Schema

1. Go to **Schema Registry** in Confluent Cloud
2. Create a new schema with this Avro definition:

```avro
{
  "doc": "Schema for website content processed for RAG applications.",
  "fields": [
    {
      "doc": "Unique identifier for the document.",
      "name": "document_id",
      "type": "string"
    },
    {
      "doc": "Source URL where the content was extracted from.",
      "name": "url",
      "type": "string"
    },
    {
      "doc": "The actual text content of the document.",
      "name": "document_content",
      "type": "string"
    },
    {
      "doc": "ISO datetime when the document was processed.",
      "name": "timestamp",
      "type": "string"
    }
  ],
  "name": "WebsiteDocument",
  "namespace": "io.eget.documents",
  "type": "record"
}
```

#### Set Up Flink Workspace

1. Go to **Flink** in Confluent Cloud
2. Create a new **Workspace**
3. Create **OpenAI Connection**:
   ```bash
   confluent flink connection create openai-connection \
     --cloud GCP \
     --region us-central1 \
     --environment ${ENV_ID} \
     --type openai \
     --endpoint https://api.openai.com/v1/chat/completions \
     --api-key <your-openai-api-key>
   ```

4. Create **Document Summarizer Model**:
   ```sql
   CREATE MODEL `your-workspace`.`document_summarizer`
   INPUT (`text` VARCHAR(2147483647))
   OUTPUT (`summary` VARCHAR(2147483647))
   COMMENT 'Document summarization model for RAG applications'
   WITH (
     'openai.connection' = 'openai-connection',
     'openai.model_version' = 'gpt-4o-mini',
     'openai.system_prompt' = 'You are an expert document summarizer. Create a comprehensive summary that captures all major details, key concepts, technical terms, and important information from the provided document. The summary should be concise but complete, preserving essential facts, procedures, examples, and critical points. Maintain technical accuracy and include relevant terminology that would be useful for retrieval-augmented generation (RAG) applications. Focus on actionable information, key insights, and important details that users might search for.',
     'provider' = 'openai',
     'task' = 'text_generation'
   )
   ```

5. Create **Embedding Model**:
   ```sql
   CREATE MODEL `your-workspace`.`openai_embedding_model`
   INPUT (`text` VARCHAR(2147483647))
   OUTPUT (`response` ARRAY<FLOAT>)
   WITH (
     'openai.connection' = 'openai-connection',
     'provider' = 'openai',
     'task' = 'embedding'
   )
   ```

6. Create **Processing Pipeline**:
   ```sql
   CREATE TABLE document_with_summary_embeddings AS
   SELECT 
     r.document_id,
     r.url,
     r.document_content,
     s.summary AS document_summary,
     e.response AS document_summary_embedding,
     r.timestamp,
     CURRENT_TIMESTAMP AS processing_timestamp
   FROM url_markdown_raw r,
   LATERAL TABLE(ML_PREDICT('document_summarizer', r.document_content)) s,
   LATERAL TABLE(ML_PREDICT('openai_embedding_model', s.summary)) e;
   ```

### 6. Configure MongoDB Sink Connector

1. In Confluent Cloud, go to **Connectors**
2. Create a new **MongoDB Atlas Sink Connector**
3. Use the configuration from `mongo_sink.example.json` in this project
4. Update the connector with your MongoDB credentials

### 7. Set Up Vector Search Index

1. In MongoDB Atlas, go to your **Database** → **Collection**
2. Click **Search** tab
3. Create a new **Search Index** with this configuration:

```json
{
  "fields": [
    {
      "type": "vector",
      "path": "document_summary_embedding",
      "numDimensions": 1536,
      "similarity": "cosine"
    }
  ]
}
```

### 8. Launch the Application

```bash
# Make sure your virtual environment is activated
streamlit run app.py
```

The application will open in your browser at `http://localhost:8501`

## 🎯 Usage Guide

### Processing a URL

1. Navigate to the **🔗 URL Chunker** tab
2. Enter a URL (e.g., `https://docs.confluent.io/cloud/current/flink/overview.html`)
3. Click **Process URL**
4. Monitor the processing status
5. View the document preview

### Asking Questions

1. Go to the **🤖 Documentation Assistant** tab
2. Type your question about Confluent
3. The system will:
   - Analyze your query
   - Search the knowledge base
   - Provide context-aware answers
   - Show search process details (optional)

### Viewing Processed URLs

1. Check the **📋 Processed URLs** tab
2. See all URLs that have been processed
3. Monitor system performance

## 🔧 Troubleshooting

### Common Issues

**"Connection refused" errors:**
- Ensure the web scraper service is running (`docker-compose up -d`)
- Check if port 8000 is available

**"Invalid API key" errors:**
- Verify your OpenAI API key is correct
- Check if your Confluent Cloud API keys are valid
- Ensure MongoDB connection string is correct

**"Topic not found" errors:**
- Create the `url_markdown_raw` topic in Confluent Cloud
- Verify the topic name in your config matches exactly

**"Vector search failed" errors:**
- Ensure the vector search index is created in MongoDB Atlas
- Check if the embedding dimensions match (should be 1536)

### Logs

Check the application logs in `app.log` for detailed error information.

## 📁 Project Structure

```
confluent-rag/
├── app.py                 # Main application entry point
├── ui.py                  # Streamlit web interface
├── config_utils.py        # Configuration management
├── db_utils.py            # MongoDB Atlas vector search
├── kafka_utils.py         # Kafka producer and document streaming
├── openai_utils.py        # RAG system with iterative retrieval
├── model_utils.py         # Abstract model provider interface
├── config.json            # Application configuration (⚠️ contains real credentials)
├── config.json.example    # Example configuration template
├── mongo_sink.json        # MongoDB sink connector config (⚠️ contains real credentials)
├── mongo_sink.example.json # Example MongoDB sink connector template
├── flink_queries.sql      # Flink SQL queries
└── truncate_collections.py # Database utility script
```

## 🔄 Data Flow

1. **Web Scraping**: Content extraction via eGet crawler
2. **Kafka Streaming**: Real-time document publishing
3. **Flink Processing**: AI-powered summarization and embeddings
4. **MongoDB Storage**: Vector database persistence
5. **RAG Querying**: Intelligent question answering
