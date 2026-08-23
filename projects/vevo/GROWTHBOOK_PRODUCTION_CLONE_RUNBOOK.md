# VEVO GrowthBook Production clone runbook

Status: Production foundation and reader evidence verified; reviewed UI clone allowed but not started

This runbook creates a separate Production Athena data source, two fact tables, and eight Starter-compatible metrics in GrowthBook. It never repoints or edits the verified Preview objects. It does not publish GTM, start an experiment, change Meta Ads or BiznisWeb, accept a paid upgrade, or move Production allocation above `0%`.

The current official GrowthBook model is the basis for this procedure: a data source defines the warehouse connection plus identifier and assignment-query configuration; fact tables are SQL `SELECT` statements with `timestamp` and the experiment identifier; metrics are defined on top of fact tables. See [Data Source Configuration](https://docs.growthbook.io/app/datasources), [Metrics and Fact Tables](https://docs.growthbook.io/app/metrics), and [Athena setup](https://docs.growthbook.io/warehouses/athena).

## Hard preconditions

Do not start the GrowthBook UI work until every item below is true in the reviewed `main` manifest:

1. The first natural reconciliation evidence is recorded and hash-verified.
2. The route-disabled Production foundation evidence is recorded and proves:
   - AWS account `919341186960`;
   - region `eu-central-1`;
   - service `vevo-growthbook-collector-production`;
   - runtime path `/app`;
   - exact private task/IP and localhost health/marker evidence;
   - zero public API routes and an empty Production event bucket.
3. The Production reader evidence is recorded and proves exactly one active key on `vevo-growthbook-production-reader`, attached only to `vevo-growthbook-readonly-production`.
4. `growthbook_clone.status` is `reader_verified_ready_for_reviewed_growthbook_clone`, `clone_allowed=true`, and `mutation_status=not_started`.
5. Production registry is empty, Production allocation is `0%`, GTM is `not_published`, and `production_activation_allowed=false`.
6. The GrowthBook workspace is still Starter and `paid_pro_upgrade_authorized=false`.

The CMS credential artifact stays outside Git. Decrypt it only locally, enter the access-key ID and secret only into the authenticated GrowthBook Athena connection, and never print, screenshot, paste into chat, or store either value in the observation. Browser entry of these credentials requires explicit action-time confirmation immediately before transmission to GrowthBook; record that confirmation in the active task, never in the source manifest.

## Frozen source objects

Read these Preview IDs back before and after the Production clone. Any change is a stop condition.

- Data source: `ds_19g6mmt2c4dmn`
- Fact tables:
  - `vevo_device_outcomes_v1`: `ftb_19g6mmt2dhrdi`
  - `vevo_performance_vitals_v1`: `ftb_19g6mmt2e0otd`
- Metrics: the eight IDs under `athena.production.growthbook_clone.source_metric_ids` in `growthbook_workspace.json`
- Preview connection repointing is forbidden.

## Creation and read-back procedure

Perform one object at a time in this exact order. After each save, reload the object and read back its name, ID, and configuration before proceeding.

1. Create a new Athena data source named `VEVO Production Experiment Facts`.
2. Enter only the dedicated Production reader credentials. Configure:
   - database `vevo_growthbook_production`;
   - workgroup `vevo-growthbook-readonly-production`;
   - the exact Production Athena results URL from the verified reader/foundation evidence;
   - the catalog required by the current Athena UI, without granting any broader prefix.
3. Run the GrowthBook connection test. It must pass using the curated-only reader.
4. Add identifier type `device_id`.
5. Add assignment query `VEVO consented devices` from `growthbook_sql/assignment.sql`. Compare the exact UI read-back with the repository file and run the query test. Before traffic it must succeed with exactly `0` rows.
6. Create fact table `VEVO Device Outcomes v1` from `growthbook_sql/device_outcomes.sql` and identifier `device_id`.
7. Create fact table `VEVO Performance Vitals v1` from `growthbook_sql/performance_vitals.sql` and identifier `device_id`.
8. For both fact tables, compare the exact query with the repository file, verify the configured column types, and run the query test. Each must succeed with exactly `0` rows before traffic. If the empty result prevents automatic type inference, set the types manually from the verified Preview contract and read them back.
9. Create only these eight Starter-compatible metrics, matching every field in the corresponding `metrics` row of `growthbook_workspace.json`:
   - `vevo_add_to_cart_24h`
   - `vevo_purchase_conversion_7d`
   - `vevo_revenue_per_exposed_device_7d`
   - `vevo_cm1_per_exposed_device_7d`
   - `vevo_average_order_value_7d`
   - `vevo_cancelled_order_rate_14d`
   - `vevo_refunded_order_rate_14d`
   - `vevo_client_error_device_rate_24h`
10. Do not create the three p75 Quantile metrics. Do not accept a trial or paid upgrade. Their target IDs remain null.
11. Reload all Production objects and the frozen Preview objects. Confirm:
    - all target IDs are new and unique;
    - every Production name, identifier, query, filter, aggregation, direction, number format, currency, and window matches the versioned contract;
    - Preview IDs/configuration are unchanged and the Preview connection was not repointed;
    - no experiment is running in Production;
    - GTM remains unpublished and Production allocation remains `0%`.

If any step fails, stop. Do not mark the clone complete, do not start A/A, and do not automatically delete or repoint anything. Record the exact non-secret partial object IDs and failure boundary in `PROJECT_STATE.md` through a reviewed PR before deciding a recovery action.

## Build the canonical observation

The builder fills affirmative read-back assertions. Run it only after every check above was genuinely observed. Replace every placeholder with the exact GrowthBook object ID read from the authenticated UI.

```text
python scripts/record_growthbook_production_clone_evidence.py build --workspace projects/vevo/growthbook_workspace.json --observed-at-utc <YYYY-MM-DDTHH:MM:SSZ> --data-source-id <ds_target> --fact-table-id vevo_device_outcomes_v1=<ftb_target> --fact-table-id vevo_performance_vitals_v1=<ftb_target> --metric-id vevo_add_to_cart_24h=<fact__target> --metric-id vevo_purchase_conversion_7d=<fact__target> --metric-id vevo_revenue_per_exposed_device_7d=<fact__target> --metric-id vevo_cm1_per_exposed_device_7d=<fact__target> --metric-id vevo_average_order_value_7d=<fact__target> --metric-id vevo_cancelled_order_rate_14d=<fact__target> --metric-id vevo_refunded_order_rate_14d=<fact__target> --metric-id vevo_client_error_device_rate_24h=<fact__target> --output projects/vevo/vevo-growthbook-production-clone-observation.json
```

Review the entire canonical JSON. It may contain only the exact reader run/commit/artifact hash, GrowthBook object IDs, fixed configuration hashes, zero-row outcomes, and explicit safety booleans. It must contain no credential, query result row, event/device identity, order/customer data, or paid-Pro target ID.

Compute its SHA-256 independently:

```text
python -c "import hashlib,pathlib; p=pathlib.Path('projects/vevo/vevo-growthbook-production-clone-observation.json'); print(hashlib.sha256(p.read_bytes()).hexdigest())"
```

## Record completion through Git

Only after independent review, record the exact canonical file and hash:

```text
python scripts/record_growthbook_production_clone_evidence.py record --observation projects/vevo/vevo-growthbook-production-clone-observation.json --observation-sha256 <sha256> --workspace projects/vevo/growthbook_workspace.json --output projects/vevo/growthbook_workspace.json
python scripts/validate_growthbook_workspace.py
python -m unittest tests.test_growthbook_production_clone_evidence_recorder tests.test_growthbook_workspace
python scripts/security_ci.py
git diff --check
```

The recorder may change only the clone status, target IDs, observation/hash, and the next gate. A valid completion sets `status=verified_complete`, closes `clone_allowed`, records `mutation_status=created_and_query_verified`, and keeps Production allocation `0%`, GTM unpublished, paid upgrade false, and Production A/A unstarted. Merge that evidence through a reviewed PR before preparing any Production A/A activation.
