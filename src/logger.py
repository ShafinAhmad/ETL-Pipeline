import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(message)s"
)

logger = logging.getLogger("etl")

def log_ingest_start(source: str, rows: int, path: str) -> None:
    logger.info(f"ingest.start source={source} rows={rows} path={path}")

def log_ingest_cleaned(source: str, valid: int, rejected: int) -> None:
    logger.info(f"ingest.cleaned source={source} valid={valid} rejected={rejected}")

def log_ingest_load(source: str, inserted: int, updated: int, duration: float) -> None:
    logger.info(f"ingest.load source={source} inserted={inserted} updated={updated} duration={duration:.1f}s")

def log_ingest_end(source: str, status: str) -> None:
    logger.info(f"ingest.end source={source} status={status}")
