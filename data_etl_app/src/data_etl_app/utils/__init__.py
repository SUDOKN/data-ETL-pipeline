import os
from dotenv import load_dotenv

DOT_ENV_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))), ".env"
)
print(f"Loading environment variables from: {DOT_ENV_PATH}")
load_dotenv(dotenv_path=DOT_ENV_PATH)
