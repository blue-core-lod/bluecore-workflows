import logging
import os
import sys

LOGLEVEL = os.environ.get("LOGLEVEL", "INFO").upper()
logging.basicConfig(level=LOGLEVEL)

logger = logging.getLogger(__name__)
logger.setLevel(LOGLEVEL)
logger.addHandler(logging.StreamHandler(sys.stdout))
