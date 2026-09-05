# A/A quality-source audit — 2026-09-05

Status: `SOURCE_CAPTURE_TIMEOUT_RETAINED_RAW`. The
source-contract repair and its implementation gate are reviewed, but the first
managed source failed without an artifact and its diagnostic acquisition timed
out without an artifact. Source coverage and A/A PASS remain
unproven. This is not permission to restart an experiment or alter its window.

### Diagnostic acquisition is terminal, not a live wait

Run `33964597883` on main `2e04784765a74e71ba5b7a21ab075cebd91102e4`
ended `cancelled`; job `101302371923` completed at `2026-09-05T12:40:21Z`
after starting at `11:55:06Z`. GitHub check annotations independently confirm
the job exceeded its maximum execution time. Only that sanitized boolean was
emitted, not raw annotations. The source's fixed markers show runtime preflight
at `11:55:49Z`, then `retained-raw-source` at `11:55:53Z`, with no later source
phase before cancellation. Artifact upload was skipped, cleanup succeeded and
independent artifact metadata confirms zero artifacts. No additional acquisition
was dispatched and there is no successful capture to record.

The retained-source phase includes initial inventory, conditional object reads,
strict receipt validation and final inventory. Current markers do not identify
which substep was active at timeout. Static sequential GETs are an implementation
fact, not proof of the live root cause. Do not infer a source-quality failure,
weaken coverage, raise runtime/input limits or repeat acquisition without a
reviewed, evidence-supported diagnostic/correction. The source did not reach
receipt parity, managed token/order API acquisition or the quality calculation.
The earlier source failure below remains a separate unresolved failure, not
proof that both attempts have the same cause.

Keep the frozen window, source binding and every downstream producer gate
unchanged. Review the exact retained-source/transport path offline before any
next managed acquisition; recovery checks and fresh independently verified
same-main health remain mandatory for a future reviewed run.

### First actual source failure

Run `33961911689` on exact main `1deb4d9ae9997a03bf21370695e2d47a7378b9a4`
passed local/provenance gates and failed in `source-capture` with exit 2 after
42m32s, before the 45-minute job timeout. Upload was skipped, cleanup passed,
and independent GitHub metadata confirms zero artifacts. The failure message
does not identify the source operation or root cause; do not infer one from
duration or claim that A/A quality itself failed.

The diagnostic correction adds only fixed operation markers and allowlisted
failure codes, never exception text, SDK payloads, inputs, counts or identities.
It preserves SDK-output suppression, source contracts, timeout, query/pacing,
acceptance rules and canonical output. Synthetic tests exercise the actual CLI
failure path and all acquisition phases. PR #522 merged after all four CI checks
and 231 tests passed. The diagnostic acquisition `33964597883` used main
`2e04784765a74e71ba5b7a21ab075cebd91102e4`, after independently verified
same-main health `33964475551`. Its pre-AWS gate passed, but it subsequently
timed out as recorded above. No source quality or A/A PASS is established.

For a future evidence-supported correction, inspect all source history again
after review/CI/merge. Any retained artifact must be recovered, not recaptured.
A future acquisition would require fresh independently verified health on its
own exact main and all unchanged managed gates; this failure record does not
authorize one. Do not rerun an old attempt, change the source/window or retry
based on quality/outcomes. Diagnosis must identify the failure safely, not keep
collecting until a report passes.

### Live health format compatibility

The first fresh exact-main managed health run `33961275554` succeeded. Its
independent source-reader verification stopped before source dispatch because
the existing health producer uses its indented `canonical_evidence_bytes`, not
the compact `canonical_source_bytes` used by quality captures. The readers now
select the exact serializer by producer, preserving original artifact bytes and
independent ZIP/JSON hashes. Reformatting a downloaded artifact is forbidden.
Synthetic health fixtures use the real producer serializer, and a new regression
rejects the wrong format even with updated valid hashes (228 focused tests).

The candidate readers verified the actual original health ZIP/JSON, complete
run/artifact ownership, offline health evidence and latest reconciliation; no
local download file or raw payload was retained. This proves the serializer
correction, not source quality. After the correction merges, obtain fresh health
on that new exact main before the first source dispatch; the previous main's
health must not be relabelled. No source capture has been dispatched yet.

## Current operational gate

`EXACT_WINDOW_SOURCE_CAPTURE_SUPPORTED` is now true after PRs #515–518 and
the separately reviewed support-gate change. Earlier sections below retain the
repair history; their closed-support statements are historical. The live
snapshot is still schema 2 with source and both evidence components unrecorded,
and all automated/manual producer, snapshot, stop, paid and CTA gates closed.

On clean exact main, first verify no source capture already exists. Verify fresh
same-main infra health through the managed monitor, including its successful
run, sole ZIP/GitHub digest, canonical JSON hash and offline validation. Only
then manually dispatch the one managed quality source with that independent
health run/hash. This read-only capture does not modify ordinary facts or shop
data. Source coverage/token/endpoint failures remain closed, not invitations to
choose a different generation or change the sample boundary. A successful
capture must be independently recorded with both source and health provenance
through a new PR before automated collection or an A/A PASS can follow.

### Pre-capture SDK metadata correction

Final readiness review found per-request Botocore `ResponseMetadata` in the
two Scheduler responses included in the source control digest. Exclude only
that top-level transport envelope: actual schedule, target, role and input
settings remain hash-bound, as do task/definition/bucket/retention identities.
A new regression proves changing request IDs/headers does not cause false
control drift, while genuine configuration changes still do. The focused suite
now contains 227 tests. No live source was acquired, and the shared source-support
flag remains false pending its separately reviewed opening.

## Automated acquisition migrated — support-gate review is next

The protected automated workflow now consumes the recorded source through
`scripts/consume_growthbook_aa_quality_source.py`. Before AWS credentials, it
independently downloads both source and health run/artifact metadata and ZIPs,
loads the original source-commit Git inputs, verifies all provenance again and
requires the result to equal the reviewed schema-3 binding and fixed checkpoint
history. Hosted exact-main/manual/first-attempt checks and prior-run/artifact
history prevent local execution, duplicate collection and result-driven retries.

After the activated collector identity and Glue checks, a bounded conditional
S3 read must reproduce the source's entire raw-input digest. Only assignment
and Meta dimensions are rebuilt using the shared exact-window calculator:
prior context excludes returning pre-window devices, first-exposure dimensions
remain fixed, in-window contamination remains visible, and post-through events
cannot alter the cohort. This step reads no orders/API/token and cannot replace
captured authoritative quality or outcomes with its order-free calculation.

The Athena audit no longer reads rolling curated device facts. It audits all
raw rows in the fixed receipt window and must match captured raw/unique counts;
privacy, consent and receipt checks remain. The actual observation assembler
requires the unchanged captured quality and emits schema 2 with its recorded
`quality_source_sha256`. The activated revision/image replaces the obsolete
foundation comparison, accepting only stable CREATE/UPDATE-complete stacks.
Raw AWS CLI errors and runtime identifiers are not printed; runner payloads
are deleted before uploading the single canonical automated component.

Verification: 12 new executable regressions and the expanded 226-test focused
suite pass. Coverage includes the actual pre-AWS CLI and actual inline
observation assembly, independently verified archives/original Git inputs,
substitution with equal totals, frozen context/contamination, preserved order
quality, conditional retained reads, runtime drift and duplicate-run refusal.
All tests use synthetic sources and are required in security-baseline CI.

**Live acquisition is still closed.** This migration does not flip
`EXACT_WINDOW_SOURCE_CAPTURE_SUPPORTED` or open any checked-in live manifest.
After review/CI/merge, review that support constant alone in a separate PR;
then require fresh exact-main infra health and acquire/record the one source.
Retained coverage, inherited token access and pinned endpoint behavior must be
proved live or remain blocked. No extra checkpoint, sample/window change,
Preview wake, A/A stop or paid/CTA/commerce transition is permitted here.

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
