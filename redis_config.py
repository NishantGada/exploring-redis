import redis, os
from dotenv import load_dotenv
load_dotenv()

r = redis.Redis(host=os.getenv("REDIS_HOST"), port=os.getenv("REDIS_PORT"), db=os.getenv("REDIS_LOGICAL_DATABASE"), decode_responses=True)
