from elasticsearch import Elasticsearch
import os

es = Elasticsearch(
    [
        {
            "host": os.getenv("ELASTICSEARCH_HOST", "localhost"),
            "port": int(os.getenv("ELASTICSEARCH_PORT", 9200)),
            "scheme": "http",
        }
    ]
)

INDEX_NAME = "plan_index"


def create_index():
    """Create index with parent-child mapping"""
    if es.indices.exists(index=INDEX_NAME):
        print(f"🗑️  Deleting existing index: {INDEX_NAME}")
        es.indices.delete(index=INDEX_NAME)

    mapping = {
        "mappings": {
            "properties": {
                "objectId": {"type": "keyword"},
                "objectType": {"type": "keyword"},
                "_org": {"type": "keyword"},
                "planType": {"type": "keyword"},
                "creationDate": {"type": "keyword"},
                "deductible": {"type": "integer"},
                "copay": {"type": "integer"},
                "name": {"type": "text"},
                "plan_join": {
                    "type": "join",
                    "relations": {
                        "plan": ["planCostShares", "linkedPlanServices"],
                        "linkedPlanServices": [
                            "linkedService",
                            "planserviceCostShares",
                        ],
                    },
                },
            }
        }
    }

    es.indices.create(index=INDEX_NAME, body=mapping)
    print(f"✅ Created index: {INDEX_NAME}")


def index_document_with_routing(doc_id, document, routing):
    """Index a document with routing"""
    try:
        es.index(
            index=INDEX_NAME,
            id=doc_id,
            body=document,
            routing=routing,
            refresh="wait_for",  # ← IMPORTANT: Wait for refresh
        )
        return True
    except Exception as e:
        print(f"❌ Error indexing {doc_id}: {e}")
        raise


def delete_all_plan_documents(plan_id):
    """
    Delete all documents related to a plan (including grandchildren)
    """
    print(f"🔍 Finding all documents for plan: {plan_id}")

    try:
        # Step 1: Find all linkedPlanServices (direct children)
        children_response = es.search(
            index=INDEX_NAME,
            body={
                "query": {"parent_id": {"type": "linkedPlanServices", "id": plan_id}},
                "size": 100,
            },
            routing=plan_id,
        )

        linked_service_ids = [hit["_id"] for hit in children_response["hits"]["hits"]]
        print(f"  Found {len(linked_service_ids)} linkedPlanServices")

        # Step 2: Delete grandchildren (children of each linkedPlanServices)
        grandchild_count = 0
        for linked_id in linked_service_ids:
            # Delete linkedService children
            result = es.delete_by_query(
                index=INDEX_NAME,
                body={
                    "query": {"parent_id": {"type": "linkedService", "id": linked_id}}
                },
                routing=plan_id,
                refresh=True,
            )
            grandchild_count += result["deleted"]

            # Delete planserviceCostShares children
            result = es.delete_by_query(
                index=INDEX_NAME,
                body={
                    "query": {
                        "parent_id": {"type": "planserviceCostShares", "id": linked_id}
                    }
                },
                routing=plan_id,
                refresh=True,
            )
            grandchild_count += result["deleted"]

        print(f"  Deleted {grandchild_count} grandchildren")

        # Step 3: Delete direct children (planCostShares, linkedPlanServices)
        child_result = es.delete_by_query(
            index=INDEX_NAME,
            body={
                "query": {
                    "bool": {
                        "should": [
                            {"parent_id": {"type": "planCostShares", "id": plan_id}},
                            {
                                "parent_id": {
                                    "type": "linkedPlanServices",
                                    "id": plan_id,
                                }
                            },
                        ]
                    }
                }
            },
            routing=plan_id,
            refresh=True,
        )
        print(f"  Deleted {child_result['deleted']} direct children")

        # Step 4: Delete the parent plan itself
        try:
            es.delete(index=INDEX_NAME, id=plan_id, routing=plan_id, refresh="wait_for")
            print(f"  Deleted parent plan: {plan_id}")
        except Exception as e:
            print(f"  Note: Parent plan might not exist: {e}")

        # Step 5: Force final refresh
        es.indices.refresh(index=INDEX_NAME)

        total_deleted = grandchild_count + child_result["deleted"] + 1
        print(f"✅ Total documents deleted: {total_deleted}")

    except Exception as e:
        print(f"❌ Error deleting documents for plan {plan_id}: {e}")
        raise


def index_document(doc_id, document):
    """Backward compatibility - use index_document_with_routing instead"""
    return index_document_with_routing(doc_id, document, routing=doc_id)
