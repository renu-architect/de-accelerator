import logging

import pytest

from de_accelerator.core import naming, versioning
from de_accelerator.core.bootstrap import load_bootstrap
from de_accelerator.core.errors import AcceleratorError, BootstrapConfigError, ConfigError
from de_accelerator.core.logging import get_logger, redact
from de_accelerator.core.registry import Registry


def test_error_codes_and_context():
    e = ConfigError("bad", key="x")
    assert str(e) == "[ACC-1000] bad" and e.context == {"key": "x"} and isinstance(e, AcceleratorError)
    assert e.error_class == "CONFIG"


def test_catalog_naming():
    assert naming.catalog_name("Acme", "DEV") == "acme_dev"
    with pytest.raises(ValueError):
        naming.catalog_name("Bad Client", "DEV")
    with pytest.raises(ValueError):
        naming.catalog_name("acme", "STAGE")
    assert naming.qualified_table("meta", "cfg_x", "acme_dev") == "acme_dev.meta.cfg_x"
    assert naming.qualified_table("meta", "cfg_x") == "meta.cfg_x"
    with pytest.raises(ValueError):
        naming.qualified_table("nope", "t")
    assert set(naming.SCHEMAS) == {"meta", "audit", "dq", "recon", "gov", "bronze", "silver", "gold"}


@pytest.mark.parametrize("text", ["password=hunter2", "AccountKey=abc123==", "token: abcdef", "pwd=x;"])
def test_redaction(text):
    assert "hunter2" not in redact(text) and "abc123" not in redact(text) and "abcdef" not in redact(text)
    assert "***" in redact(text)


def test_logger_redacts(caplog):
    log = get_logger("t")
    with caplog.at_level(logging.INFO, logger="de_accelerator.t"):
        log.info("connecting with password=%s", "s3cret")
    assert "s3cret" not in caplog.text


def test_registry():
    r: Registry[int] = Registry("thing")
    r.register("a", 1)
    assert r.get("a") == 1 and r.keys() == ["a"]
    with pytest.raises(KeyError):
        r.register("a", 2)
    with pytest.raises(KeyError):
        r.get("zzz")


def test_versioning():
    assert versioning.satisfies("1.2.0", ">=1.0,<2.0")
    assert not versioning.satisfies("2.0.0", ">=1.0,<2.0")
    assert versioning.satisfies("0.1.0.dev0", ">=0.1.0.dev0,<1.0")
    assert not versioning.satisfies("1.0", "garbage")
    assert versioning.valid_specifier(">=1.0,<2.0") and not versioning.valid_specifier("~~1")
    assert versioning.is_semver("1.2.3") and versioning.is_semver("0.1.0.dev0")
    assert not versioning.is_semver("abc")
    assert not versioning.is_semver("1.2")
    assert versioning.framework_version()


def test_bootstrap(tmp_path):
    (tmp_path / "base.yaml").write_text("client_code: acme\nenvironment: DEV\nflavor: LOCAL\n")
    (tmp_path / "qa.yaml").write_text("environment: QA\n")
    cfg = load_bootstrap(tmp_path / "base.yaml")
    assert cfg.catalog == "acme_dev" and cfg.flavor == "LOCAL"
    assert load_bootstrap(tmp_path / "base.yaml", tmp_path / "qa.yaml").catalog == "acme_qa"
    (tmp_path / "bad.yaml").write_text("client_code: acme\nenvironment: DEV\nsurprise: 1\n")
    with pytest.raises(BootstrapConfigError):
        load_bootstrap(tmp_path / "bad.yaml")
    with pytest.raises(BootstrapConfigError):
        load_bootstrap(tmp_path / "missing.yaml")


def test_wire_loggers_are_silenced():
    import logging

    from de_accelerator.core.logging import quiet_wire_loggers

    logging.getLogger("py4j.clientserver").setLevel(logging.DEBUG)
    quiet_wire_loggers()
    assert not logging.getLogger("py4j.clientserver").isEnabledFor(logging.DEBUG)
    assert not logging.getLogger("py4j").isEnabledFor(logging.INFO)
