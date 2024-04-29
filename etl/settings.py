import logging
import os
from dotenv import load_dotenv

load_dotenv()

POSTGRES_DB = os.environ.get("POSTGRES_DB")
POSTGRES_USER = os.environ.get("POSTGRES_USER")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
POSTGRES_HOST = os.environ.get("POSTGRES_HOST")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT")

BUTCH_SIZE = int(os.environ.get("BUTCH_SIZE"))

ELASTIC_URL = os.environ.get("ELASTIC_URL")

POSTGRES_DSN = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

CHECK_TABLE = ["film_work", "person", "genre"]

INDEX_NAME = "movies"

logger = logging.getLogger("my_logger")
logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
logger.addHandler(console_handler)
