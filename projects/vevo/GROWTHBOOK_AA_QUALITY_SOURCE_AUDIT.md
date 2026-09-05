# A/A quality-source audit — 2026-09-05

Status: `QUALITY_SOURCE_WINDOW_BINDING_BLOCKED`. This is a source-contract
finding, not a measured A/A failure or permission to restart the experiment.

## Offline source binding migrated — automated acquisition remains closed

`open-automated` now rejects the legacy rolling-quality interface. The new
`scripts/growthbook_aa_source_binding.py` verifies independently downloaded
source **and** health ZIPs against their successful exact-main GitHub runs and
sole-artifact metadata, including ownership, ZIP digests and reviewer-supplied
canonical JSON hashes. It verifies the source is a first-attempt manual run,
health completed before capture, the latest local reconciliation at capture
time, and freshness then (not at a later offline recording time).

Expected inputs are the five named source-commit Git blobs: snapshot, workspace,
activation, acceptance and reconciliation deployment evidence. The read-only
Git loader requires the exact full commit to be an ancestor of the current
checkout, disables replacement objects, and does not fetch, switch branches or
fall back to the working tree. The source's snapshot hash therefore refers to
the original pre-transition bytes. Neither the capture's own claims nor the
later source-opened manifest can supply the independent expected values.

The offline recorder verifies both archives before making any write. The first
transition requires current snapshot content to equal the source-commit
snapshot, then records a strict source provenance binding and upgrades only
the snapshot schema to 3. It preserves the fixed interval, qualifying count,
checkpoint history and manual component. Replaying the same open transition
is idempotent; a different source or an unrelated changed snapshot is rejected.
Legacy schema 2 can remain closed but cannot open a producer using a rolling
report. No checked-in live manifest has been migrated or opened yet.

Automated evidence schema 2 now requires `quality_source_sha256`. The component
recorder compares it with the recorded source JSON hash, and the offline
builder/assembler reject a missing hash or old automated schema. The protected
snapshot workflow requires source-bound manifest schema 3. The final aggregate
snapshot schema and statistical acceptance criteria remain unchanged.

The revised offline command takes the following independently prepared inputs
(all example paths are temporary sanitized inputs, not a new source artifact):

```text
python scripts/record_growthbook_aa_evidence_gates.py open-automated
  --source-zip <source.zip> --source-run <source-run.json>
  --source-artifacts <source-artifacts.json>
  --health-zip <health.zip> --health-run <health-run.json>
  --health-artifacts <health-artifacts.json>
  --expected-workflow-run-id <source-run-id>
  --expected-main-commit <source-main-sha>
  --expected-evidence-sha256 <source-json-sha256>
  --expected-health-run-id <health-run-id>
  --expected-health-sha256 <health-json-sha256>
```

Do not execute this command or acquire a source yet: the managed acquisition
and automated consumer support gate remains false. Once the consumer migration
is reviewed, independently obtain these six inputs through the GitHub boundary,
use the recorder on a new branch, inspect the diff, test, commit/push/PR, and
delete only the exact temporary inputs after verification. Do not substitute
JSON/run/hash metadata fabricated from the capture itself.

Verification: 15 new source-binding regressions and the expanded 214-test
source/reporting/A/A lifecycle suite pass. Coverage includes both independent
archives, Git object selection, source-commit byte binding, temporal provenance,
invalid ZIP entries, immutable window/state, source substitution, schema
transitions and CLI idempotence. Required security-baseline CI includes these
tests plus the completion-recorder and snapshot-workflow regressions. Only
metadata of an existing GitHub infra-health run was inspected to confirm API
ownership fields; no live source or AWS/BiznisWeb call occurred.

Next replace the automated workflow's rolling S3 quality read with independent
source/health archive verification and original-commit Git input loading, emit
the verified source hash in its schema 2 observation, and correct its current
activated collector identity. Also correct the Meta cohort audit: its existing
`eligible_facts` CTE reads rolling curated facts, which are not proof of the
frozen cohort/context. Keep source/automated AWS access closed until these
remaining consumer paths and tests are reviewed. Do not change the window,
thresholds or count, or allow later outcome-driven source selection.

## Managed acquisition prepared — consumer migration must precede live capture

The manual source workflow is now implemented at
`.github/workflows/collect-vevo-growthbook-production-aa-quality-source.yml`.
Its CLI and offline capture validator are
`scripts/collect_growthbook_aa_quality_source.py` and
`scripts/validate_growthbook_aa_quality_capture.py`. The stages below describe
the earlier calculation/adapter work; this section is the current handoff.

**The workflow cannot yet acquire AWS credentials or read source data.**
`EXACT_WINDOW_SOURCE_CAPTURE_SUPPORTED` in the existing evidence recorder is
deliberately false. Both the recorder and automated consumer must require the
new capture before a separate reviewed migration can enable this pre-AWS gate.
Do not flip the constant merely to test the producer or discover live input.

Prepared acquisition boundaries:

- Exact clean main, GitHub-hosted manual run, explicit confirmation and first
  run attempt. Independently verify a successful current-main infra-health run,
  sole artifact, GitHub ZIP digest and canonical JSON hash; require the latest
  local 03:45 reconciliation and an observation no older than six hours.
- Refuse another active/successful source capture and any failed prior run with
  an artifact. Consume or recover retained evidence; never select a source by
  recapturing until quality passes.
- Derive context from the UTC day of the reviewed empty, route-disabled
  Production foundation. Bind full source-commit snapshot bytes and the final
  qualifying checkpoint. Validate the current activated collector revision,
  Fargate task/private network, service and `/app` identity, immutable reconciler
  task/image and prior localhost gate, unchanged source schedule and exact
  raw/curated/query/multipart retention policy before source/token reads.
- Use the tested complete retained S3 partitions and verify accepted-write
  parity with fully paginated collector receipts whose log retention covers the
  context. Recheck source/runtime control identity at the end of capture.
- Resolve only the existing VEVO API token inherited by the immutable
  reconciler and original reporting task definitions from the same AWS account
  and region. The transport pins the existing checked-in VEVO endpoint, disables
  environment proxy/netrc inheritance, rejects redirects and HTTP/GraphQL or
  malformed/duplicate/nonfinite/oversized responses, and paces two exact-ID
  passes. It does not query customer/contact/address fields or call mutations.
- The optional `order_facts_only` constructor skips reporting/ad/weather clients,
  cache/export directories and reporting environment mutation, while keeping
  the shared financial and lifecycle calculations. It is a construction mode,
  not a general capability sandbox; the source uses only the fact conversion.
- Retain only `vevo-growthbook-aa-quality-source.json` in the single artifact
  `vevo-growthbook-aa-quality-source`, for 90 days. Raw events, API responses,
  AWS metadata and secrets stay in runner memory; diagnostics are suppressed.
  No ordinary publisher, deployment, Preview wake or experiment mutation runs.

The official [API best practices](https://www.biznisweb.sk/a/1382/best-practices)
recommend the public domain to avoid redirects. This prepared transport keeps
the repository's existing `https://vevo.flox.sk/api/graphql` identity and refuses
redirects rather than forwarding a token to an unreviewed destination. Live
endpoint behavior and inherited credential access are not yet verified; any
necessary endpoint/access change needs its own reviewed exact scope.

The capture validator binds all independently expected provenance and coverage
proofs, but explicitly preserves the adapters' false claims for forensic
historical retention and atomic historical order snapshots. Retention policy,
stable inventory and count parity must not be described as proof against every
possible historical manual deletion/substitution.

Verification: 20 new synthetic managed-source tests plus the prior 162 tests
pass. The full capture test uses real calculation/adapters with injected AWS
and API transports, verifies no ordinary publication or output directory, and
checks serialized privacy. Default exporter construction and financial/lifecycle
parity with the PII-free order projection are covered. The suite is included in
required security-baseline CI. No live source/API/AWS read or dispatch occurred.

Next migrate the offline recorder, snapshot schema/validator and automated
consumer to this exact canonical capture and independent successful run/main/
ZIP/JSON provenance. Retrieve the pre-transition snapshot/foundation at the
source run's commit. Fix the automated workflow's old foundation collector
revision/image assumption using the reviewed activated collector. Review that
migration before allowing the source gate; no fixed window/count, later
checkpoint or lifecycle gate may be changed to accommodate source data.

## Repair progress — exact-window calculation implemented, runtime still closed

The first implementation stage adds optional `ExperimentReceiptWindow` handling
to the existing deterministic fact builder and the pure
`scripts/build_growthbook_aa_quality_source.py` module. Ordinary callers omit
the option and keep their existing reporting behavior. A windowed bundle is
explicitly rejected by the ordinary curated publisher before its first write.

The new calculation keeps prior receipts as assignment/ambiguity context, uses
`received_at` with inclusive-from/exclusive-through bounds, and includes only
devices whose first valid contextual exposure falls in the resolved interval.
Event/duplicate/orphan counts cover the receipt interval; context-only devices
cannot re-enter the cohort. Cross-cohort transaction ambiguity remains visible.
Receipts at/after through cannot change assignment eligibility, outcomes or
performance. This is the bounded A/A audit calculation, not a change to the
ordinary 7-day purchase or 24-hour health/cart metric definitions or the future
CTA follow-up policy.

The pure source builder emits only an aggregate envelope binding exact receipt,
cohort and context bounds; independent snapshot/checkpoint hashes; main-run
provenance; generation time; and whole-extract input digests. Its validator
rejects missing/mismatched windows despite equal counts, unrelated provenance,
noncanonical bytes, altered SHA-256, population mismatch, and unsafe fields.
Input digests retain duplicates and are order-independent; no event, device or
order identity appears in the output. The new generation uses whole-second UTC
schema timestamps, without rewriting any legacy S3 object.

**This is not yet a live quality-source capture or a completed repair.** At
the calculator stage the workflow path was reserved only; the prepared, still
closed workflow is described above. There is no runtime deployment, source
artifact, consumer gate opening or producer dispatch, nor evidence of a run.
The current source recorder and automated workflow still use the legacy
quality-object interface and remain blocked by this audit.

Next implement the managed, main-only source producer and migrate the source
recorder/automated consumer to this exact envelope. The producer's pre-AWS gate
must independently validate the resolved snapshot and checkpoint, derive the
complete context floor from approved activation/runtime history, freeze the
source generation and input coverage, and supply the expected hashes/run/main
metadata. Never trust those expected values only because the artifact says so.
Do not infer source completeness from input digests or the pure calculation;
the protected source adapter must separately prove complete coverage. A full
snapshot-file hash must be bound to the source run's pre-transition commit,
not recomputed against the later source-opened manifest. Prepare any required
one-shot runtime/credential access as a separately reviewed gate before live
collection; do not overwrite ordinary curated facts or wake Preview.

Verification: 133 focused tests cover the new source/boundaries plus existing
reporting, scheduled reconciliation, checkpoint, evidence, assembly and
workspace behavior. The entire suite is now included in the existing required
`security-baseline` PR job. Window/workspace validators, security and diff checks pass.
An additional 100 synthetic differential cases matched all three ordinary
outputs against the pre-change committed core byte-for-byte after canonical
JSON serialization, including duplicates, contamination, orphans and ambiguity.
Only synthetic input was used; real cohort eligibility still requires source
production and must exactly equal the already resolved checkpoint, or stop.

## Input-adapter repair stage — retained coverage, no live collection

`reporting_core/experiment_quality_source_io.py` now provides two separately
testable read-only input adapters with injected clients. It does not construct
an AWS/API client, acquire credentials, log inputs, write files or publish
facts. The prepared caller described above remains blocked before credentials.

- Raw input: enumerate every page of each exact UTC receipt partition, including
  empty days; validate bounded object identities/sizes/ETags/timestamps; bind
  each GET with `IfMatch`; verify the full body and receipt/date parity; run the
  existing strict event validation; and re-enumerate the complete inventory.
  Any addition/deletion/replacement, repeated object, invalid pagination, schema
  error or size/row overflow fails closed. Retained input includes the later
  edge of the last UTC partition for validation; the existing windowed builder
  excludes it from the cohort and quality calculation.
- Orders: query only the exact supplied validated completion-receipt IDs using
  one fixed `getOrder` operation, selecting existing reporting money, item,
  status and payment-metadata fields, not customer/contact/address data. Every
  ID must have an explicit order or explicit null answer. Errors, missing fields
  and wrong IDs are not converted into not-found answers. A second identical
  pass detects observed source drift; there is no retry-until-quality-passes.
  The shared authoritative-order conversion remains the intended downstream
  calculator; no substitute lifecycle or financial values are generated.

The API operation follows the existing checked-in Order schema and official
[BiznisWeb API calling documentation](https://www.biznisweb.sk/a/1268/volanie-api).
The future managed transport must independently verify the VEVO endpoint/token,
apply the existing request pacing, and reject all HTTP/GraphQL error responses.
It must not use the broad cached order-list/export workflow to prove coverage.

Only whole-input/inventory/query hashes and explicit coverage assertions are
exportable proof; internal rows are excluded from result `repr` and may never
be logged, downloaded to the PC or uploaded as artifacts. SDK/API errors are
replaced by fixed messages without chained sensitive exceptions. The input
byte limits are separate from row limits, and source bodies are closed on
successful and failed reads.

**These adapters do not prove complete historical source retention, a correct
context floor, an atomic historical BiznisWeb snapshot, runtime identity or
successful-main provenance.** The proof explicitly leaves these claims false
or outside its scope. Equality of two inventories only establishes a stable
read of the retained objects, not that earlier deletion never occurred. The
protected producer must derive the context floor and prove the approved
storage/retention boundary independently, select only pre-through completion
receipts, bind the actual capture interval and input proofs to the source
envelope, and run under the managed credential boundary. Do not treat an
adapter's returned proof as permission to open a producer.

Verification: 29 new synthetic tests plus the previous 133 regressions pass,
and the new suite is included in the required `security-baseline` CI job. Tests
cover complete and cyclic pagination, conditional reads, input changes,
partition edges, malformed/PII input, explicit missing orders, mutation of
reused client response objects, bounds and sanitized errors/proofs. No live
source, workflow dispatch, infrastructure action or shop request occurred.

Next integrate these adapters into the separately reviewed main-only source
workflow and migrate the offline recorder/automated consumer to the canonical
source envelope. The existing `QUALITY_SOURCE_WINDOW_BINDING_BLOCKED` state,
frozen resolved window and all closed gates remain unchanged.

## Verified boundary

The reviewed snapshot resolves the existing window to
`[2026-08-25T22:00:00Z, 2026-09-03T22:00:00Z)`, at checkpoint 3 with 1,058
cumulative eligible devices. Do not change that window, its count, checkpoint
history, or stopping rule to make a later quality report pass.

Both evidence producers and the snapshot build remain closed. No automated or
manual evidence workflow has a recorded run. No live quality report, arm,
outcome, or performance value was inspected during this audit.

## Reproduced defects

1. `run_scheduled_growthbook_reconciliation.py` derives complete **UTC** date
   partitions, with `rolling_partition_days=40` in the Production settings.
   `reconcile_growthbook_facts.py` passes all loaded events to
   `build_experiment_facts`; it does not apply the resolved A/A receipt-time
   interval. The September 4 generation therefore loads through the end of
   September 3 UTC (`2026-09-04T00:00:00Z`), two hours later than the resolved
   local-day through-boundary,
   and its rolling start also predates the A/A start.
2. `reporting_core/experiments.py` creates each quality report over all supplied
   events for the experiment. Its quality JSON contains a generation timestamp
   but no source `from_utc`/`through_utc` or immutable window provenance.
3. `validate_quality_report` checks generation at/after the through-boundary,
   canonical field identities and equality of the eligible-device total. It
   does not prove the same input window. A matching total is not proof of the
   same devices, events, order joins or performance observations.
4. The protected automated workflow mixes the bound reporting-quality object
   with separately bounded receipt/Athena audits. Its S3 read is intentionally
   inaccessible until a quality key/hash has already been reviewed. Repository
   search found no existing managed workflow that independently exports and
   proves the initial exact-window quality source. Do not use local AWS
   credentials or open the automated producer just to discover a source.

The source-window defect is reproducible entirely offline with the existing
synthetic test fixture. This command currently succeeds even though the report
has no source-window provenance and its generation was moved a month later:

```text
python -c "from scripts.record_growthbook_aa_evidence_gates import validate_quality_report; from tests.test_growthbook_aa_evidence_gate_recorder import quality_report; q=quality_report(); q['facts_generated_at']='2026-10-01T01:50:00Z'; validate_quality_report(q,quality_report_key='experiment-events/curated/quality/experiment_id=vevo-sk-aa-001/facts_generated_at=20261001T015000Z.json',resolved_through_utc='2026-09-01T22:00:00Z',resolved_eligible_devices=1000); print('SYNTHETIC_ONLY: unbound_later_generation_accepted=true')"
```

The live source contents have not been inspected, so this finding does **not**
claim that a particular stored report has a wrong count or that data were lost.
It proves that the present source contract cannot establish the required
window identity. Do not select a generation by trying reports until counts or
quality values happen to pass, or relabel a rolling report as exact-window.

## Test lifecycle repair completed

The next-stage tests also inherited the now-resolved checked-in snapshot while
expecting an unstarted window. Five recorder scenarios failed before testing
any transition; two workflow tests asserted stale initial dates/status.
`tests/growthbook_aa_fixtures.py` now constructs a validated isolated initial
scenario. Recorder and workflow tests use it while the production manifest
continues to be validated independently. No production gate was loosened.

The combined checkpoint, source-recorder, automated/manual evidence, snapshot
assembler and workspace suite passes 86 tests. Passing these tests does not
resolve the source-contract defect above.

## Required next implementation

Before running either quality producer or claiming A/A PASS:

1. Add regression coverage that rejects missing/mismatched source-window
   provenance even when totals match. Keep tests proving prior checkpoints,
   canonical hashes and independently supplied successful main-run provenance.
2. Implement a narrowly scoped repository-owned exact-window quality-source
   path through the existing managed GitHub AWS boundary. Preserve the existing
   metric, eligibility, contamination and order-join semantics; explicitly bind
   input/cohort interval and immutable source generation. Verify pre-window and
   post-through records cannot leak into the required metrics. A count mismatch
   must fail closed rather than alter the resolved stopping record.
3. Export only canonical sanitized aggregate quality plus independently
   verifiable source/run/window/hash provenance. Raw input and customer/order/
   device identities must remain inside the approved AWS/runner boundary and
   never be included in logs or retained artifacts. Do not change ordinary
   reporting outputs, schedules or Preview to create this source.
4. Make the offline source recorder and automated workflow require that exact
   provenance, through a reviewed PR with tests and CI before any dispatch.
   If source production needs a new deployment, task execution or write scope,
   prepare a separate explicit versioned gate and pass the established live
   instance/IP/service/path and localhost-marker checks first; do not improvise
   a broad reconciler run or overwrite curated facts.
5. Independently verify the source artifact, bind it through the versioned
   recorder, then complete genuine browser QA and both protected components.
   Only the assembled protected snapshot can establish PASS and allow the
   separate reviewed stop/readback sequence.

Preview stays asleep. A/A stop, paid Pro, CTA start, GTM, Meta Ads, BiznisWeb and
commerce changes remain out of scope for this repair until their own gates open.
