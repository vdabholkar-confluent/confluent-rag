# db_utils.py
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
from pymongo import MongoClient, DESCENDING

from config_utils import load_config

# Configure logging
logger = logging.getLogger("db")

class MongoDBVectorSearch:
    """Handles connection and vector similarity search in MongoDB."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize MongoDB connection with config settings."""
        try:
            self.client = MongoClient(
                config['mongodb_uri'],
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=5000
            )
            # Test the connection
            self.client.admin.command('ping')
            
            self.db = self.client[config['mongodb_database']]
            self.collection = self.db[config['mongodb_collection']]
            
            # Create URLs collection if not exists
            if 'processed_urls' not in self.db.list_collection_names():
                self.db.create_collection('processed_urls')
            
            self.urls_collection = self.db['processed_urls']
            
            logger.info(f"Connected to MongoDB collection: {config['mongodb_collection']}")
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {str(e)}")
            raise

    def vector_search(self, query_embedding: List[float], limit: int = 3, query_text: str = "") -> List[Dict[str, Any]]:
        """Search for similar documents based on vector similarity using summary embeddings."""
        try:
            # Create the aggregation pipeline for vector search on summary embeddings
            pipeline = [
                {
                    "$vectorSearch": {
                        "index": "vector_index_cc_docs",
                        "path": "document_summary_embedding",
                        "queryVector": query_embedding,
                        "numCandidates": limit * 100,  # Significantly increased candidate pool
                        "limit": limit,  # Get only top 'limit' results
                        "minScore": 0.65  # Adjusted similarity threshold
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "document_id": 1,
                        "url": 1,
                        "document_content": 1,
                        "document_summary": 1,
                        "timestamp": 1,
                        "score": {"$meta": "vectorSearchScore"}
                    }
                }
            ]
            
            # Try standard ANN search first
            results = list(self.collection.aggregate(pipeline))
            
            # If no good results, try exact search (ENN)
            if not results or (results and results[0].get('score', 0) < 0.7):
                logger.info("Low quality ANN results, trying exact search (ENN)")
                exact_pipeline = [
                    {
                        "$vectorSearch": {
                            "index": "vector_index_cc_docs",
                            "path": "document_summary_embedding",
                            "queryVector": query_embedding,
                            "exact": True,  # Use exact nearest neighbor search
                            "limit": limit  # Maintain the same limit
                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
                            "document_id": 1,
                            "url": 1,
                            "document_content": 1,
                            "document_summary": 1,
                            "timestamp": 1,
                            "score": {"$meta": "vectorSearchScore"}
                        }
                    }
                ]
                
                exact_results = list(self.collection.aggregate(exact_pipeline))
                if exact_results and (not results or exact_results[0].get('score', 0) > results[0].get('score', 0)):
                    logger.info("Using exact search results instead of ANN results")
                    results = exact_results
            
            # If still no results, try with more relaxed parameters
            if not results:
                logger.info("No results found, trying with relaxed parameters")
                fallback_pipeline = [
                    {
                        "$vectorSearch": {
                            "index": "vector_index_cc_docs",
                            "path": "document_summary_embedding",
                            "queryVector": query_embedding,
                            "numCandidates": limit * 200,
                            "limit": limit,  # Still maintain the same limit
                            "minScore": 0.6  # Lower similarity threshold for fallback
                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
                            "document_id": 1,
                            "url": 1,
                            "document_content": 1,
                            "document_summary": 1,
                            "timestamp": 1,
                            "score": {"$meta": "vectorSearchScore"}
                        }
                    }
                ]
                results = list(self.collection.aggregate(fallback_pipeline))
            
            # Log detailed information
            logger.info(f"Retrieved {len(results)} documents from vector search")
            if results:
                logger.info(f"Top result score: {results[0].get('score', 'N/A'):.2f}")
                for i, result in enumerate(results):
                    score = result.get('score', 0)
                    content_snippet = result.get('document_summary', '')[:100] + "..."
                    logger.info(f"Result {i+1}: Score={score:.2f}, Summary={content_snippet}")
            
            # Ensure we're returning only the highest quality results, sorted by score
            sorted_results = sorted(results, key=lambda x: x.get('score', 0), reverse=True)
            
            # Explicitly limit to the top 'limit' results
            return sorted_results[:limit]
        except Exception as e:
            logger.error(f"Vector search failed: {str(e)}")
            return []

    def get_document_by_id(self, document_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a specific document by its ID."""
        try:
            document = self.collection.find_one(
                {"document_id": document_id},
                {"_id": 0}
            )
            if document:
                logger.info(f"Retrieved document {document_id}")
            else:
                logger.warning(f"Document {document_id} not found")
            return document
        except Exception as e:
            logger.error(f"Failed to retrieve document {document_id}: {str(e)}")
            return None

    def search_documents_by_url(self, url: str) -> List[Dict[str, Any]]:
        """Search for documents by URL."""
        try:
            documents = list(self.collection.find(
                {"url": url},
                {"_id": 0}
            ))
            logger.info(f"Found {len(documents)} documents for URL: {url}")
            return documents
        except Exception as e:
            logger.error(f"Failed to search documents by URL {url}: {str(e)}")
            return []

    def get_recent_documents(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recently processed documents."""
        try:
            documents = list(self.collection.find(
                {},
                {"_id": 0}
            ).sort("timestamp", DESCENDING).limit(limit))
            logger.info(f"Retrieved {len(documents)} recent documents")
            return documents
        except Exception as e:
            logger.error(f"Failed to retrieve recent documents: {str(e)}")
            return []
    
# Singleton pattern to ensure one database connection
_mongodb_instance = None

def get_mongodb_instance() -> MongoDBVectorSearch:
    """Get or create MongoDB instance using singleton pattern."""
    global _mongodb_instance
    if _mongodb_instance is None:
        config = load_config()
        _mongodb_instance = MongoDBVectorSearch(config)
    return _mongodb_instance

def store_processed_url(url: str, processing_type: str, document_count: int = 1, **kwargs) -> bool:
    """Store information about a processed URL with flexible parameters."""
    try:
        mongo = get_mongodb_instance()
        
        # Create document for the processed URL
        url_doc = {
            "url": url,
            "processing_type": processing_type,  # Changed from chunker_type to processing_type
            "document_count": document_count,    # Changed from chunk_count to document_count
            "processed_at": datetime.now()
        }
        
        # Add any additional keyword arguments
        url_doc.update(kwargs)
        
        # Insert or update the URL record
        result = mongo.urls_collection.update_one(
            {"url": url},
            {"$set": url_doc},
            upsert=True
        )
        
        logger.info(f"Stored URL information for {url} (type: {processing_type})")
        return True
    except Exception as e:
        logger.error(f"Failed to store URL information: {str(e)}")
        return False

def get_stored_urls(limit: int = 100) -> List[Dict[str, Any]]:
    """Retrieve stored URLs ordered by processing time."""
    try:
        mongo = get_mongodb_instance()
        
        # Get URLs sorted by processing time (newest first)
        urls = list(mongo.urls_collection.find(
            {},
            {"_id": 0}
        ).sort("processed_at", DESCENDING).limit(limit))
        
        logger.info(f"Retrieved {len(urls)} processed URLs")
        return urls
    except Exception as e:
        logger.error(f"Failed to retrieve processed URLs: {str(e)}")
        return []

def get_processing_stats() -> Dict[str, Any]:
    """Get statistics about processed URLs and documents."""
    try:
        mongo = get_mongodb_instance()
        
        # Get URL processing stats
        url_stats = mongo.urls_collection.aggregate([
            {
                "$group": {
                    "_id": "$processing_type",
                    "count": {"$sum": 1},
                    "total_documents": {"$sum": "$document_count"}
                }
            }
        ])
        url_stats_dict = {stat["_id"]: stat for stat in url_stats}
        
        # Get document stats
        total_documents = mongo.collection.count_documents({})
        recent_documents = mongo.collection.count_documents({
            "timestamp": {"$gte": (datetime.now() - datetime.timedelta(days=7)).isoformat()}
        })
        
        stats = {
            "total_urls_processed": mongo.urls_collection.count_documents({}),
            "total_documents": total_documents,
            "recent_documents_7days": recent_documents,
            "processing_types": url_stats_dict,
            "last_updated": datetime.now().isoformat()
        }
        
        logger.info(f"Generated processing statistics: {stats}")
        return stats
    except Exception as e:
        logger.error(f"Failed to get processing statistics: {str(e)}")
        return {}

# Backward compatibility functions
def store_processed_url_legacy(url: str, chunker_type: str, chunk_count: int) -> bool:
    """Legacy function for backward compatibility."""
    return store_processed_url(url, chunker_type, chunk_count)

# New convenience functions for the updated workflow
def store_scraped_url(url: str, document_id: str = None, word_count: int = None) -> bool:
    """Store information about a scraped URL."""
    kwargs = {}
    if document_id:
        kwargs["document_id"] = document_id
    if word_count:
        kwargs["word_count"] = word_count
    
    return store_processed_url(url, "scraper", 1, **kwargs)

def store_summarized_url(url: str, document_id: str = None, summary_length: int = None) -> bool:
    """Store information about a summarized URL."""
    kwargs = {}
    if document_id:
        kwargs["document_id"] = document_id
    if summary_length:
        kwargs["summary_length"] = summary_length
    
    return store_processed_url(url, "summarizer", 1, **kwargs)