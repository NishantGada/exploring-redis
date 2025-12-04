GET plan_index
POST plan_index/_refresh

# query to get count of documents in plan_index
GET plan_index/_count


# view all documents for within plan_index
GET plan_index/_search
{
  "query": {
    "match_all": {}
  }
}

# to get all mappings for plan_index
GET plan_index/_mapping


GET plan_index/_search
{
  "size": 0,
  "aggs": {
    "types_breakdown": {
      "terms": {
        "field": "plan_join",
        "size": 20
      }
    }
  }
}

# query to find all parents (root parents)
GET plan_index/_search
{
  "query": {
    "term": {
      "plan_join": "plan"
    }
  }
}


# get all children for a specific parent using "parent_id"
GET plan_index/_search
{
  "query": {
    "parent_id": {
      "type": "planCostShares",        
      "id": "12xvxc345ssdsds-508"
    }
  }
}


# get ALL children of ALL types instead of filtering by types
GET plan_index/_search
{
  "query": {
    "has_parent": {
      "parent_type": "plan",
      "query": {"term": {"objectId": "12xvxc345ssdsds-508"}}
    }
  }
}


# Find parent plan that has this specific child
GET plan_index/_search
{
  "query": {
    "has_child": {
      "type": "linkedPlanServices",
      "query": {
        "term": {
          "objectId": "27283xvx9asdff-504"
        }
      }
    }
  }
}


GET plan_index/_search
{
  "query": {
    "has_child": {
      "type": "planserviceCostShares",
      "query": {
        "range": {
          "copay": {
            "gte": 50
          }
        }
      }
    }
  }
}


# get ALL children of ALL types instead of filtering by types
GET plan_index/_search
{
  "query": {
    "has_parent": {
      "parent_type": "plan",
      "query": {
        "match_all": {}
      }
    }
  }
}



# conditional search
GET /plan_index/_search
{
  "query": {
    "bool": {
      "must": [
        {
          "bool": {
            "must": [
              {
                "match": {
                  "copay": 175
                }
              },
              {
                "match": {
                  "deductible": 10
                }
              }
            ]
          }
        }
      ]
    }
  }
}


















