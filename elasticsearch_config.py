from elasticsearch import Elasticsearch
import os
from dotenv import load_dotenv

load_dotenv()

# Create ES client
es = Elasticsearch([{
    'host': os.getenv("ELASTICSEARCH_HOST", "localhost"),
    'port': int(os.getenv("ELASTICSEARCH_PORT", 9200)),
    'scheme': 'http'
}])

INDEX_NAME = "plan_index"

def create_index():
    """Create index with parent-child relationships"""
    mapping = {
        "mappings": {
            "properties": {
                "objectId": {"type": "keyword"},
                "objectType": {"type": "keyword"},
                "_org": {"type": "keyword"},
                "planType": {"type": "keyword"},
                "creationDate": {"type": "keyword"},  # ← Changed from "date" to "keyword"
                "name": {"type": "text"},
                "deductible": {"type": "integer"},
                "copay": {"type": "integer"},
                
                # Parent-Child join field
                "plan_join": {
                    "type": "join",
                    "relations": {
                        "plan": ["planCostShares", "linkedPlanServices"],
                        "linkedPlanServices": ["linkedService", "planserviceCostShares"]
                    }
                }
            }
        }
    }
    
    try:
        if es.indices.exists(index=INDEX_NAME):
            es.indices.delete(index=INDEX_NAME)
            print(f"🗑️  Deleted existing index: {INDEX_NAME}")
    except Exception as e:
        print(f"Note: {e}")
    
    es.indices.create(index=INDEX_NAME, mappings=mapping["mappings"])
    print(f"✅ Created index with parent-child mapping: {INDEX_NAME}")

def index_document(doc_id, doc_body):
    """Index a single document"""
    try:
        es.index(index=INDEX_NAME, id=doc_id, document=doc_body, refresh=True)
        print(f"📝 Indexed: {doc_id}")
        return True
    except Exception as e:
        print(f"❌ Error indexing {doc_id}: {e}")
        return False

def delete_all_plan_documents(plan_id):
    """Delete all documents related to a plan"""
    try:
        # Delete by query - remove plan and all its children
        query = {
            "query": {
                "bool": {
                    "should": [
                        {"term": {"objectId": plan_id}},
                        {"term": {"parent_id": plan_id}}
                    ]
                }
            }
        }
        es.delete_by_query(index=INDEX_NAME, body=query, refresh=True)
        print(f"🗑️  Deleted all documents for plan: {plan_id}")
        return True
    except Exception as e:
        print(f"❌ Error deleting documents: {e}")
        return False
    

def index_document_with_routing(doc_id, doc_body, routing):
    """Index document with routing (required for parent-child)"""
    try:
        es.index(
            index=INDEX_NAME, 
            id=doc_id, 
            document=doc_body, 
            routing=routing,
            refresh=True
        )
        print(f"📝 Indexed: {doc_id} (routing: {routing})")
        return True
    except Exception as e:
        print(f"❌ Error indexing {doc_id}: {e}")
        return False