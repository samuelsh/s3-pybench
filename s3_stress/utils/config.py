import json
import os

from s3_stress.config.sample_config import sample_config
from s3_stress.utils import server_logger

logger = server_logger.Logger(name=__name__).logger


def ensure_config():
    try:
        with open(os.path.expanduser('~/.s3_config.json'), 'r') as f:
            config = json.load(f)
        return config
    except FileNotFoundError:
        print("Config file is missing. Please create one according to following example:")
        print(json.dumps(sample_config, indent=4))
        return False
