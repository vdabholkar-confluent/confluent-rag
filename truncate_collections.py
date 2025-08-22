# truncate_collections.py
from pymongo import MongoClient
from config_utils import load_config
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("truncate")

def truncate_collections():
    """Truncate all documents from MongoDB collections while keeping the collections."""
    try:
        # Load config
        config = load_config()
        
        # Connect to MongoDB
        client = MongoClient(config['mongodb_uri'])
        db = client[config['mongodb_database']]
        
        # Collection names to truncate
        collections_to_truncate = [
            config['mongodb_collection'],  # confluent_docs
            'processed_urls'  # processed URLs collection
        ]
        
        for collection_name in collections_to_truncate:
            if collection_name in db.list_collection_names():
                collection = db[collection_name]
                
                # Count documents before deletion
                count_before = collection.count_documents({})
                
                # Delete all documents
                result = collection.delete_many({})
                
                logger.info(f"Collection '{collection_name}': Deleted {result.deleted_count} documents (was {count_before})")
            else:
                logger.warning(f"Collection '{collection_name}' not found")
        
        # Close connection
        client.close()
        logger.info("All collections truncated successfully")
        
    except Exception as e:
        logger.error(f"Error truncating collections: {str(e)}")

if __name__ == "__main__":
    truncate_collections()