# DE Accelerator — core repository

Enterprise metadata-driven data engineering accelerator (Azure Databricks / Delta / Unity Catalog). One core, one deployment repo per client.
Status: **M1 (metadata engine) implemented, LOCAL_VERIFIED; Databricks integration pending.** See `CLAUDE.md` and `docs/`.

## Local development
```bash
uv venv --python 3.11 .venv && uv pip install --python .venv/bin/python -e ".[spark,dev]"
.venv/bin/python -m pytest -m "not component"      # fast unit tests (~30 s)
.venv/bin/python -m pytest -m component            # local Spark + Delta tests (several minutes)
scripts/ci_test.sh                                 # lint + mypy + all tests (what CI runs)
```

## Try the metadata CLI on the synthetic example pack
```bash
de-accel validate-config metadata/examples/acme_pack                      # all environments
de-accel build-bundle metadata/examples/acme_pack --out /tmp/bundles
# with a local bootstrap file (client_code, environment, flavor: LOCAL, warehouse_dir):
de-accel --bootstrap bootstrap.yaml migrate
de-accel --bootstrap bootstrap.yaml load-pack /tmp/bundles/bundle_DEV.json --activate --approved-by <name>
de-accel --bootstrap bootstrap.yaml snapshot --execution daily_sales
de-accel --bootstrap bootstrap.yaml rollback --reason "why"
```
All data in the example pack is synthetic. Module spec: `docs/architecture/modules/metadata-engine.md`.
