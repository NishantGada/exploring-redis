from functools import wraps
from flask import request, jsonify
from jose import jwt, jwk
from jose.utils import base64url_decode
import requests, os, json
from dotenv import load_dotenv
load_dotenv()

# Google OAuth 2.0 configuration
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_OAUTH_CLIENT_ID")
GOOGLE_ISSUER = "https://accounts.google.com"

# Cache Google's public keys (JWKS)
GOOGLE_JWKS_URI = "https://www.googleapis.com/oauth2/v3/certs"
JWKS = requests.get(GOOGLE_JWKS_URI).json()["keys"]


def get_public_key(kid):
    """Fetch the correct public key from Google's JWKS by key ID."""
    for key in JWKS:
        if key["kid"] == kid:
            return jwk.construct(key)
    return None


def require_oauth(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            return jsonify({"error": "Missing or invalid Authorization header"}), 401

        token = auth_header.split("Bearer ")[1]

        try:
            # Decode header to get 'kid'
            headers = jwt.get_unverified_header(token)
            key = get_public_key(headers["kid"])
            if key is None:
                return jsonify({"error": "Unable to find matching public key"}), 403

            # Verify JWT manually
            message, encoded_sig = token.rsplit('.', 1)
            decoded_sig = base64url_decode(encoded_sig.encode('utf-8'))

            if not key.verify(message.encode("utf-8"), decoded_sig):
                return jsonify({"error": "Invalid token signature"}), 403

            # Decode claims without verifying signature again
            payload = jwt.get_unverified_claims(token)

            # Verify audience and issuer manually
            if payload.get("aud") != GOOGLE_CLIENT_ID:
                return jsonify({"error": "Invalid audience"}), 403
            if payload.get("iss") != GOOGLE_ISSUER:
                return jsonify({"error": "Invalid issuer"}), 403

            request.user = payload

        except jwt.ExpiredSignatureError:
            return jsonify({"error": "Token expired"}), 403
        except jwt.JWTClaimsError:
            return jsonify({"error": "Invalid claims"}), 403
        except Exception as e:
            return jsonify({"error": f"Invalid token: {str(e)}"}), 403

        return f(*args, **kwargs)

    return decorated_function
