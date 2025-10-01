from flask import Flask, request, jsonify, make_response
from flask_cors import CORS
from jsonschema import validate, ValidationError
import json

from use_case_schema import plan_schema
from redis_config import *
from helper_functions import generate_etag_from_json

app = Flask(__name__)
CORS(app)


@app.route("/")
def root():
    return jsonify({"message": "Welcome to Demo 1!"})


@app.route("/plans", methods=["POST"])
def create_plan():
    try:
        payload = request.json
        validate(instance=payload, schema=plan_schema)
    except ValidationError as e:
        return jsonify({"error": f"Invalid payload: {e.message}"}), 400

    object_id = payload["objectId"]

    if r.exists(object_id):
        return jsonify({"error": "Plan already exists"}), 409

    r.set(object_id, json.dumps(payload))
    etag = generate_etag_from_json(payload)

    resp = make_response(jsonify(payload), 201)
    resp.headers["ETag"] = etag
    return resp


@app.route("/plans/<object_id>", methods=["GET"])
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


@app.route("/plans/<object_id>", methods=["DELETE"])
def delete_plan(object_id):
    if not r.exists(object_id):
        return jsonify({"error": "Not found"}), 404

    r.delete(object_id)
    return "", 204


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
