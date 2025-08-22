# kafka_utils.py
import json
import logging
import asyncio
import httpx
from uuid import uuid4
from datetime import datetime
from typing import Dict, Any, List, Optional

from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# Configure logging
logger = logging.getLogger("kafka")

# The combined schema for Avro serialization
AVRO_SCHEMA = """
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
"""

class KafkaService:
    """Service to manage Kafka producer and sending data to Kafka."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize Kafka service with config."""
        self.config = config
        self.producer = self._create_kafka_producer()
        self.topic = config.get("kafka_topic", "docs_chunks_raw")
        
        logger.info(f"Kafka service initialized with topic: {self.topic}")
    
    def _create_kafka_producer(self) -> Producer:
        """Create Kafka producer with config settings."""
        kafka_config = {
            'bootstrap.servers': self.config.get("kafka_bootstrap_servers"),
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': self.config.get("kafka_api_key"),
            'sasl.password': self.config.get("kafka_api_secret"),
            'client.id': 'scraper-producer-client'
        }
        
        try:
            # Initialize Schema Registry client
            schema_registry_conf = {
                'url': self.config.get("schema_registry_url"),
                'basic.auth.user.info': f"{self.config.get('schema_registry_api_key')}:{self.config.get('schema_registry_api_secret')}"
            }
            self.schema_registry_client = SchemaRegistryClient(schema_registry_conf)
            
            # Create serializers
            self.string_serializer = StringSerializer('utf_8')
            self.avro_serializer = AvroSerializer(
                self.schema_registry_client,
                AVRO_SCHEMA,
                lambda message, ctx: message
            )
            
            producer = Producer(kafka_config)
            logger.info("Kafka producer initialized with Avro serialization")
            return producer
        except Exception as e:
            logger.error(f"Failed to create Kafka producer: {str(e)}")
            raise RuntimeError(f"Kafka producer initialization failed: {str(e)}")
    
    def _delivery_report(self, err, msg):
        """Delivery callback for Kafka producer."""
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')
    
    def send_message(self, key: str, value: Dict[str, Any]) -> None:
        """Send a message to Kafka."""
        try:
            self.producer.produce(
                topic=self.topic,
                key=self.string_serializer(key),
                value=self.avro_serializer(
                    value, 
                    SerializationContext(self.topic, MessageField.VALUE)
                ),
                on_delivery=self._delivery_report
            )
            # Trigger delivery callbacks
            self.producer.poll(0)
            
        except Exception as e:
            logger.error(f"Error sending message to Kafka: {str(e)}")
    
    def flush(self, timeout: int = 10) -> int:
        """Flush all messages and return the number of messages still in queue."""
        return self.producer.flush(timeout)

async def fetch_and_scrape_url(url: str, scraper_api_url: str) -> Optional[Dict[str, Any]]:
    """Fetch and scrape content from a URL using the scraper API."""
    try:
        logger.info(f"Scraping URL: {url}")
        
        # Prepare scraper API request
        payload = {
            "url": url,
            "formats": ["markdown", "html"],
            "onlyMainContent": True,
            "includeRawHtml": False,
            "includeScreenshot": False,
            "timeout": 30,
            "mobile": False,
            "skipTlsVerification": False
        }
        
        # Call scraper API
        async with httpx.AsyncClient() as client:
            response = await client.post(scraper_api_url, json=payload, timeout=120.0)
            response.raise_for_status()
            result = response.json()
            
            if not result.get("success"):
                logger.error(f"Scraper API error for {url}: {result}")
                return None
            
            logger.info(f"Successfully scraped {url}")
            return result
                
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error for {url}: {e.response.status_code} - {e.response.text}")
    except httpx.RequestError as e:
        logger.error(f"Request error for {url}: {str(e)}")
    except Exception as e:
        logger.error(f"Error scraping {url}: {str(e)}", exc_info=True)
    
    return None

async def process_url_and_send_to_kafka(url: str, config: Dict[str, Any]) -> Dict[str, Any]:
    """Process URL, scrape it, and send document to Kafka."""
    try:
        # Initialize Kafka service
        kafka_service = KafkaService(config)
        
        # Get scraper API URL from config
        scraper_api_url = config.get("scraper_api_url", "http://localhost:8000/api/v1/scrape")
        
        # Process URL through scraper API
        result = await fetch_and_scrape_url(url, scraper_api_url)
        
        if not result or not result.get("success"):
            error_msg = "Failed to scrape content"
            return {"success": False, "error": error_msg}
        
        # Extract scraped data
        scraped_data = result.get("data", {})
        if not scraped_data:
            return {"success": True, "document": None, "message": "No data found"}
        
        # Get markdown content (main content for embeddings)
        markdown_content = scraped_data.get("markdown", "")
        if not markdown_content:
            return {"success": True, "document": None, "message": "No markdown content found"}
        
        logger.info(f"Sending scraped document to Kafka topic: {kafka_service.topic}")
        
        # Create message structure
        document_id = str(uuid4())
        message = {
            "document_id": document_id,
            "url": url,
            "document_content": markdown_content,
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }
        
        # Send to Kafka
        kafka_service.send_message(document_id, message)
        
        # Ensure message is sent
        remaining = kafka_service.flush(10)
        if remaining > 0:
            logger.warning(f"{remaining} messages were not delivered")
        else:
            logger.info(f"Document successfully sent to Kafka")
        
        # Return success with document info
        return {
            "success": True,
            "document": {
                "document_id": document_id,
                "url": url,
                "word_count": len(markdown_content.split()) if markdown_content else 0,
                "timestamp": message["timestamp"]
            },
            "processed_at": message["timestamp"]
        }
        
    except Exception as e:
        logger.error(f"Error in process_url_and_send_to_kafka: {str(e)}", exc_info=True)
        return {"success": False, "error": str(e)}