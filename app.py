from flask import Flask, request, jsonify, make_response
from flask_cors import CORS
from jsonschema import validate, ValidationError
import json

from use_case_schema import plan_schema
from redis_config import *
from helper_functions import generate_etag_from_json
from oauth import require_oauth

app = Flask(__name__)
CORS(app)


@app.route("/")
def root():
    return jsonify({"message": "Welcome to Demo 2!"})


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
        r.set(object_id, json.dumps(payload))
        etag = generate_etag_from_json(payload)

        resp = make_response(jsonify(payload), 201)
        resp.headers["ETag"] = etag
        return resp
    except Exception as e:
        return jsonify({"error": e}), 500


@app.route("/plans/<object_id>", methods=["GET"])
@require_oauth
def get_plan(object_id):
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
        # 1️/ Fetch existing data from Redis
        existing_data = r.get(object_id)
        if not existing_data:
            return jsonify({"error": "Resource not found"}), 404

        existing_data = json.loads(existing_data)
        current_etag = generate_etag_from_json(existing_data)

        # 2️/ Handle ETag conditional update
        if_match = request.headers.get("If-Match")
        if if_match and if_match != current_etag:
            return jsonify({"error": "ETag mismatch - resource has been modified"}), 412

        # 3️/ Parse and validate incoming patch data
        patch_data = request.json
        if not isinstance(patch_data, dict):
            return jsonify({"error": "Invalid request body - must be JSON object"}), 400

        # 4️/ Merge patch into existing JSON (shallow merge)
        merged_data = {**existing_data, **patch_data}

        # 5️/ Validate merged JSON against schema
        try:
            validate(instance=merged_data, schema=plan_schema)
        except ValidationError as e:
            return jsonify({"error": f"Validation failed: {e.message}"}), 400

        # 6️/ Save updated data and return response with new ETag
        r.set(object_id, json.dumps(merged_data))
        new_etag = generate_etag_from_json(merged_data)

        resp = make_response(jsonify(merged_data), 200)
        resp.headers["ETag"] = new_etag
        return resp

    except Exception as e:
        # 7️/ Catch-all for unexpected issues
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500


@app.route("/plans/<object_id>", methods=["DELETE"])
@require_oauth
def delete_plan(object_id):
    if not r.exists(object_id):
        return jsonify({"error": "Not found"}), 404

    r.delete(object_id)
    return "", 204


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
