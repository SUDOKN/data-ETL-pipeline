import os
import logging
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

DOT_ENV_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")

logger.info(f"Loading environment variables from: {DOT_ENV_PATH}")
load_dotenv(dotenv_path=DOT_ENV_PATH)
