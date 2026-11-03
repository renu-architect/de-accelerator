"""TableStore abstraction over Delta tables (in-memory impl for unit tests, Spark/Delta impl for real use)."""

from de_accelerator.metadata.store.base import TableStore, now_utc
from de_accelerator.metadata.store.memory import InMemoryTableStore

__all__ = ["InMemoryTableStore", "TableStore", "now_utc"]
