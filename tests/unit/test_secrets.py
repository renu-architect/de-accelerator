"""Secret resolution (B5): the PROD guard of EnvSecretProvider, Key Vault reference handling, redaction of resolved values."""

import pytest

from de_accelerator.core.errors import PlatformAccessError, SourceConfigError
from de_accelerator.core.logging import redact
from de_accelerator.core.secrets import (
    DatabricksSecretProvider,
    EnvSecretProvider,
    StaticSecretProvider,
    resolve,
    resolve_connection,
)


@pytest.mark.parametrize("env", ["PROD", "prod", "Prod"])
def test_env_provider_refuses_prod(env):
    with pytest.raises(PlatformAccessError, match="PROD"):
        EnvSecretProvider(env, {"DEACCEL_SECRET_X": "v"})


@pytest.mark.parametrize("env", ["DEV", "QA", "dev"])
def test_env_provider_allowed_outside_prod(env):
    assert EnvSecretProvider(env, {"DEACCEL_SECRET_ERP_PWD": "pw-value-123"}).get("erp-pwd") == "pw-value-123"


def test_env_provider_name_mapping_and_missing_secret():
    p = EnvSecretProvider("DEV", {"DEACCEL_SECRET_MY_DB_USER": "svc-account"})
    assert p.get("my-db-user") == "svc-account" and p.get("MY_DB_USER") == "svc-account"
    with pytest.raises(PlatformAccessError, match="DEACCEL_SECRET_NOPE"):
        p.get("nope")


def test_resolved_values_are_redacted_everywhere_redact_is_applied():
    EnvSecretProvider("DEV", {"DEACCEL_SECRET_TOK": "tok-9f8e7d6c5b"}).get("tok")
    StaticSecretProvider({"k": "static-secret-abc123"}).get("k")
    assert "tok-9f8e7d6c5b" not in redact("auth header tok-9f8e7d6c5b sent")
    assert "static-secret-abc123" not in redact("connection failed for static-secret-abc123")


def test_resolve_only_touches_key_vault_references():
    sec = StaticSecretProvider({"erp-pwd": "s3cret-value"})
    assert resolve(sec, "kv:erp-pwd") == "s3cret-value"
    assert resolve(sec, "1433") == "1433" and resolve(sec, 5) == 5 and resolve(sec, None) is None  # not a reference: unchanged
    with pytest.raises(PlatformAccessError):
        resolve(sec, "kv:missing")


def test_resolve_connection_subset():
    sec = StaticSecretProvider({"h": "host.example", "p": "pw-xyz-1234"})
    conn = {"host": "kv:h", "port": "1433", "password": "kv:p"}
    assert resolve_connection(sec, conn) == {"host": "host.example", "port": "1433", "password": "pw-xyz-1234"}
    assert resolve_connection(sec, conn, keys=["host"]) == {"host": "host.example"}


class FakeSecrets:
    def __init__(self, data, fail=False):
        self.data, self.fail, self.calls = data, fail, []

    def get(self, scope, key):
        self.calls.append((scope, key))
        if self.fail:
            raise RuntimeError("Secret does not exist with scope: kv-x and key: y")
        return self.data[key]


class FakeDbutils:
    def __init__(self, secrets):
        self.secrets = secrets


def test_databricks_provider_reads_from_the_configured_scope_and_redacts():
    fake = FakeSecrets({"erp-pwd": "kv-sourced-value-77"})
    p = DatabricksSecretProvider("kv-acme-dev", FakeDbutils(fake))
    assert p.get("erp-pwd") == "kv-sourced-value-77" and fake.calls == [("kv-acme-dev", "erp-pwd")]
    assert "kv-sourced-value-77" not in redact("x kv-sourced-value-77 y")


def test_databricks_provider_maps_failures_to_platform_errors():
    with pytest.raises(PlatformAccessError, match="erp-pwd"):
        DatabricksSecretProvider("kv-acme-dev", FakeDbutils(FakeSecrets({}, fail=True))).get("erp-pwd")


def test_databricks_provider_requires_a_scope():
    with pytest.raises(SourceConfigError, match="secret_scope"):
        DatabricksSecretProvider("")


def test_databricks_provider_outside_databricks_is_a_clear_error():
    with pytest.raises(PlatformAccessError, match="not running on Databricks"):
        DatabricksSecretProvider("kv-acme-dev").get("x")  # no dbutils injected and pyspark.dbutils unavailable locally
