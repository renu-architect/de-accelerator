"""Secret resolution. Metadata holds only `kv:<name>` references; values are resolved at runtime, kept in memory, and
registered for redaction. Never persisted in manifests/events/logs."""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any, Protocol

from de_accelerator.core.errors import PlatformAccessError, SourceConfigError
from de_accelerator.core.logging import register_secret

KV_PREFIX = "kv:"


class SecretProvider(Protocol):
    def get(self, name: str) -> str: ...


class DatabricksSecretProvider:
    """Key Vault-backed Databricks secret scope (`dbutils.secrets.get`). `scope` = environment.secret_scope."""

    def __init__(self, scope: str, dbutils: Any | None = None) -> None:
        if not scope:
            raise SourceConfigError("environment.secret_scope is required to resolve Key Vault references")
        self.scope = scope
        self._dbutils = dbutils

    def _db(self) -> Any:
        if self._dbutils is not None:
            return self._dbutils
        try:  # available inside Databricks runtimes
            from pyspark.dbutils import DBUtils
            from pyspark.sql import SparkSession

            return DBUtils(SparkSession.builder.getOrCreate())
        except Exception as exc:  # pragma: no cover - requires Databricks
            raise PlatformAccessError("dbutils is not available: not running on Databricks") from exc

    def get(self, name: str) -> str:
        try:
            value = str(self._db().secrets.get(scope=self.scope, key=name))
        except PlatformAccessError:
            raise
        except Exception as exc:
            raise PlatformAccessError(f"cannot read secret '{name}' from scope '{self.scope}'") from exc
        register_secret(value)
        return value


class EnvSecretProvider:
    """LOCAL DEVELOPMENT ONLY: reads DEACCEL_SECRET_<NAME> (name upper-cased, '-' -> '_'). Refuses to run in PROD."""

    def __init__(self, environment: str, env: Mapping[str, str] | None = None) -> None:
        if environment.upper() == "PROD":
            raise PlatformAccessError("EnvSecretProvider must not be used in PROD; use the Key Vault-backed secret scope")
        self._env = env if env is not None else os.environ

    def get(self, name: str) -> str:
        key = "DEACCEL_SECRET_" + name.upper().replace("-", "_")
        if key not in self._env:
            raise PlatformAccessError(f"secret '{name}' not available (set {key})")
        value = self._env[key]
        register_secret(value)
        return value


class StaticSecretProvider:
    """Tests only."""

    def __init__(self, values: Mapping[str, str]) -> None:
        self._values = dict(values)

    def get(self, name: str) -> str:
        if name not in self._values:
            raise PlatformAccessError(f"secret '{name}' not available")
        register_secret(self._values[name])
        return self._values[name]


def resolve(provider: SecretProvider, value: Any) -> Any:
    """`kv:<name>` -> secret value; anything else is returned unchanged (non-secret settings such as a port)."""
    if isinstance(value, str) and value.startswith(KV_PREFIX):
        return provider.get(value[len(KV_PREFIX):])
    return value


def resolve_connection(provider: SecretProvider, connection: Mapping[str, Any], keys: list[str] | None = None) -> dict[str, Any]:
    return {k: resolve(provider, v) for k, v in connection.items() if keys is None or k in keys}
