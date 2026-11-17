"""ddreduce: failure-input reduction for relational JSONL data."""

from .engine import Reducer
from .model import Database
from .oracle import OracleConfig
from .schema import load_schema
from .verify import Verifier

__all__ = ["Reducer", "Verifier", "Database", "OracleConfig", "load_schema"]
__version__ = "1.0.0"
