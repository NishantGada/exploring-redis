from elasticsearch import Elasticsearch
import os

es = Elasticsearch(
    [f"http://{os.getenv('ELASTICSEARCH_HOST', 'localhost')}:{os.getenv('ELASTICSEARCH_PORT', 9200)}"]
)

INDEX_NAME = "plan_index"

def query_child_by_copay(min_copay=1):
    """Query: Find planserviceCostShares where copay >= 1"""
    query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {
                            "plan_join": "planserviceCostShares"
                        }
                    },
                    {
                        "range": {
                            "copay": {
                                "gte": min_copay
                            }
                        }
                    }
                ]
            }
        }
    }
    
    result = es.search(index=INDEX_NAME, body=query)
    print(f"\n🔍 Child Query: planserviceCostShares with copay >= {min_copay}")
    print(f"Found: {result['hits']['total']['value']} documents")
    
    for hit in result['hits']['hits']:
        source = hit['_source']
        print(f"  - ID: {source['objectId']}, Copay: {source['copay']}")
    
    return result

def query_children_of_parent(parent_id="12xvxc345ssdsds-508"):
    """Query: Find all children of a plan using has_parent"""
    query = {
        "query": {
            "has_parent": {
                "parent_type": "plan",
                "query": {
                    "term": {
                        "objectId": parent_id
                    }
                }
            }
        }
    }
    
    result = es.search(index=INDEX_NAME, body=query)
    print(f"\n🔍 Parent Query: All children of plan '{parent_id}'")
    print(f"Found: {result['hits']['total']['value']} documents")
    
    for hit in result['hits']['hits']:
        source = hit['_source']
        print(f"  - ID: {source['objectId']}, Type: {source['objectType']}")
    
    return result

def query_by_conditions(copay=175, deductible=10):
    """Query: Find documents matching multiple conditions"""
    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"copay": copay}},
                    {"term": {"deductible": deductible}}
                ]
            }
        }
    }
    
    result = es.search(index=INDEX_NAME, body=query)
    print(f"\n🔍 Conditional Query: copay={copay} AND deductible={deductible}")
    print(f"Found: {result['hits']['total']['value']} documents")
    
    for hit in result['hits']['hits']:
        source = hit['_source']
        print(f"  - ID: {source['objectId']}, Copay: {source.get('copay')}, Deductible: {source.get('deductible')}")
    
    return result

if __name__ == "__main__":
    print("\n" + "="*60)
    print("DEMO 3 - Elasticsearch Parent-Child Queries")
    print("="*60)
    
    query_child_by_copay(1)
    query_by_conditions(175, 10)
    query_children_of_parent("12xvxc345ssdsds-508")
