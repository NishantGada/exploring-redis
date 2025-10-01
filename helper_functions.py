import hashlib, json

def generate_etag_from_json(data):
    return hashlib.md5(json.dumps(data, sort_keys=True).encode()).hexdigest()
