import logging
from pathlib import Path

log_dir = Path(__file__).resolve().parent.parent / "logs"
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(log_dir / "etl.log", mode='w')
    ]
)

logger = logging.getLogger("etl")