from flask import Flask, request, jsonify, make_response
from flask_cors import CORS
from jsonschema import validate, ValidationError
import json

from use_case_schema import plan_schema
from redis_config import r
from helper_functions import generate_etag_from_json
from oauth import require_oauth
from rabbitmq_config import publish_to_queue  # NEW

app = Flask(__name__)
CORS(app)


@app.route("/")
def root():
    return jsonify({"message": "Welcome to Demo 3!"})


# NEW HELPER FUNCTION - Store individual objects
def store_individual_objects(plan_data):
    """Store each object with its own Redis key"""
    plan_id = plan_data['objectId']
    
    # Store main plan (keep this for easy GET)
    r.set(plan_id, json.dumps(plan_data))
    
    # Store planCostShares individually
    cost_shares = plan_data['planCostShares']
    r.set(cost_shares['objectId'], json.dumps(cost_shares))
    
    # Store each linkedPlanService and its nested objects
    for service in plan_data.get('linkedPlanServices', []):
        # Store linkedPlanService
        r.set(service['objectId'], json.dumps(service))
        
        # Store linkedService
        r.set(service['linkedService']['objectId'], json.dumps(service['linkedService']))
        
        # Store planserviceCostShares
        r.set(service['planserviceCostShares']['objectId'], json.dumps(service['planserviceCostShares']))
    
    print(f"✅ Stored individual objects for plan: {plan_id}")


# NEW HELPER FUNCTION - Delete individual objects
def delete_individual_objects(plan_data):
    """Delete all individual Redis keys for a plan"""
    plan_id = plan_data['objectId']
    
    # Delete main plan
    r.delete(plan_id)
    
    # Delete planCostShares
    r.delete(plan_data['planCostShares']['objectId'])
    
    # Delete linkedPlanServices and nested objects
    for service in plan_data.get('linkedPlanServices', []):
        r.delete(service['objectId'])
        r.delete(service['linkedService']['objectId'])
        r.delete(service['planserviceCostShares']['objectId'])
    
    print(f"🗑️  Deleted individual objects for plan: {plan_id}")


@app.route("/plans", methods=["POST"])
@require_oauth
def create_plan():
    try:
        payload = request.json
        validate(instance=payload, schema=plan_schema)
    except ValidationError as e:
        return jsonify({"error": f"Invalid payload: {e.message}"}), 400

    object_id = payload["objectId"]

    if r.exists(object_id):
        return jsonify({"error": "Plan already exists"}), 409

    try:
        # Store individual objects in Redis
        store_individual_objects(payload)  # NEW
        
        # Publish to RabbitMQ
        publish_to_queue(payload, "CREATE")  # NEW
        
        etag = generate_etag_from_json(payload)

        resp = make_response(jsonify(payload), 201)
        resp.headers["ETag"] = etag
        return resp
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/plans/<object_id>", methods=["GET"])
@require_oauth
def get_plan(object_id):
    # GET still works easily because we kept the main key
    data = r.get(object_id)
    if not data:
        return jsonify({"error": "Not found"}), 404

    data = json.loads(data)
    etag = generate_etag_from_json(data)

    if_none_match = request.headers.get("If-None-Match")
    if if_none_match == etag:
        return "", 304

    resp = make_response(jsonify(data), 200)
    resp.headers["ETag"] = etag
    return resp


@app.route("/plans/<object_id>", methods=["PATCH"])
@require_oauth
def patch_plan(object_id):
    """Partially update a plan resource."""
    try:
        # Fetch existing data
        existing_data = r.get(object_id)
        if not existing_data:
            return jsonify({"error": "Resource not found"}), 404

        existing_data = json.loads(existing_data)
        current_etag = generate_etag_from_json(existing_data)

        # ETag check
        if_match = request.headers.get("If-Match")
        if if_match and if_match != current_etag:
            return jsonify({"error": "ETag mismatch - resource has been modified"}), 412

        # Merge patch data
        patch_data = request.json
        if not isinstance(patch_data, dict):
            return jsonify({"error": "Invalid request body - must be JSON object"}), 400

        merged_data = {**existing_data, **patch_data}

        # Validate
        try:
            validate(instance=merged_data, schema=plan_schema)
        except ValidationError as e:
            return jsonify({"error": f"Validation failed: {e.message}"}), 400

        # Delete old individual objects
        delete_individual_objects(existing_data)  # NEW
        
        # Store new individual objects
        store_individual_objects(merged_data)  # NEW
        
        # Publish to RabbitMQ
        publish_to_queue(merged_data, "UPDATE")  # NEW
        
        new_etag = generate_etag_from_json(merged_data)

        resp = make_response(jsonify(merged_data), 200)
        resp.headers["ETag"] = new_etag
        return resp

    except Exception as e:
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500


@app.route("/plans/<object_id>", methods=["DELETE"])
@require_oauth
def delete_plan(object_id):
    if not r.exists(object_id):
        return jsonify({"error": "Not found"}), 404

    # Get the plan data before deleting
    plan_data = json.loads(r.get(object_id))
    
    # Delete individual objects
    delete_individual_objects(plan_data)  # NEW
    
    # Publish to RabbitMQ
    publish_to_queue({"objectId": object_id}, "DELETE")  # NEW
    
    return "", 204


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)