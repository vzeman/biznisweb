# A/A quality-source audit — 2026-09-05

Status: `FRESH_INFRA_HEALTH_MARKER_SUMMARY_UNVERIFIED`. Receipt diagnostics are
reviewed, but the fresh same-main infrastructure monitor failed before producing
health evidence. No fifth source acquisition was dispatched. The prior
`SOURCE_CAPTURE_RECEIPT_PARITY_UNCLASSIFIED_FAILURE` remains unresolved; all four
source attempts are terminal without artifacts. Complete source coverage and
A/A PASS remain unproven. This is
not permission to restart an experiment or alter its window. Separately, browser
QA is fail-closed on `GTM_LIVE_VERSION_DRIFT` and the newly verified static
`CLARITY_DIAGNOSTIC_FREE_TEXT_PRIVACY_RISK`; see the browser precheck.

### Fresh health readback failed; do not dispatch source or reuse old health

PR #532 merged at `2026-09-05T15:17:52Z` as
`81a1fa21b60284946fc6a2042c8c00fbb47e2836`. All four exact-head checks passed
for reviewed head `4e00d170d030e81798e544c9bcfd0c027757afb7`, independently
bound to CI `33974202527` and observability `33974202552`. Security job
`101327918081` actually ran the full source/lifecycle/receipt step from
`15:15:34Z` to `15:15:37Z`; its filtered log confirms 252 tests OK. Clean exact
main and workspace/window/activation validators were rechecked before health.
All four source runs were independently rechecked terminal and artifact-free.

Fresh health run `33974425550`, job `101328515277`, was dispatched on that exact
main at `2026-09-05T15:19:03Z` and completed `failure` at `15:19:32Z`. Local and
credential gates, stack/schedule/alarm/DLQ checks and natural-task selection
returned successfully. The failed marker/parity step emitted only:
`scheduled reconciliation marker/summary drift:marker-lines=0:summary-lines=0:raw-messages-emitted=false`.
These are marker/summary-line counts, not reporting rows or experimental
population/outcomes. They do **not** prove the entire stream was empty, that
the scheduled run failed, or that the runtime changed. Artifact build, upload
and explicit raw-response cleanup were skipped; independent metadata confirms
zero artifacts. No raw response file was downloaded locally, no old health
artifact was substituted, and no fifth source run was started.

Static review identifies a completeness gap: the marker step calls
`aws logs get-log-events --start-from-head` once and never verifies a terminal
`nextForwardToken`. The [official GetLogEvents contract](https://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_GetLogEvents.html)
permits partial or empty pages before the forward token stabilizes, and warns
that unbounded forward reads can fail to terminate. The local SDK's read-only
paginator model contains no GetLogEvents paginator; this model inspection
created no client and is not a claim about the runner's particular CLI build.
The workflow currently has no explicit bounded token-completion proof regardless
of CLI behavior. That is a verified code gap, **not proof of the failed run's
raw page contents or root cause**; those payloads were not inspected.

The cleanup step also lacks `always()` and was independently observed skipped
after the failure. Runner disposal must not be represented as verified explicit
cleanup. A source attempt cannot proceed until a reviewed correction establishes
fresh complete health; do not simply retry the failed monitor or bypass marker
validation with the earlier successful artifact from a different main.

Next prepare a narrowly scoped, offline-tested monitor correction: read only
the already-selected exact reconciliation stream, bound the time/page/event/byte
range, follow forward tokens through empty pages until a validated terminal
token, reject token cycles/malformed/overflow responses, then retain the same
exact single marker/summary and generated/published parity checks. Add failure-
path cleanup with independently validated exact runner-temp/run-ID targets; no
unresolved broad deletion and no raw-data upload. Keep runtime selection,
identity, localhost/deploy binding, schedules, alarm/DLQ/source controls, health
schema, source gates and A/A window unchanged. Review/test/CI/PR the correction
before any new managed health check. This is read-only monitoring repair, not
authority for an infrastructure deployment, data query or live UI change.

### Bounded reads completed; receipt-phase failure requires sanitized diagnosis

Run `33972852946`, job `101324325818`, on original main
`b467f416ddfbc36bcd9fe18cdf55b2377814c110` completed `failure` at
`2026-09-05T14:58:41Z` (run metadata updated `14:58:42Z`). The acquisition
exited 2 at `14:58:38Z`, not at the 45-minute timeout. Upload was skipped,
cleanup succeeded and an independent artifact listing confirms zero artifacts.

The fixed sanitized phase log proves conditional reads ran from
`14:48:27.6553165Z` to `14:55:00.0216782Z` (6m32.366s, compared with the prior
43m02.684s observation). Strict raw-event validation then returned; the final
inventory began at `14:55:00.3388080Z` and returned before receipt parity began
at `14:55:08.0574219Z`. Receipt parity stopped at `14:58:38.0464885Z` with
`stage=source-capture:phase=receipt-parity:code=unclassified-error:raw=false`.
No reporting import, token/order API read, complete source, parity success or
quality evaluation follows from those markers. This establishes the observed
read-time improvement, not a controlled performance benchmark or the cause of
the receipt failure. It does not prove a mismatch, missing events, invalid JSON,
an AWS access error or the cause of earlier acquisitions.

Offline review found a diagnostic gap: receipt parity wraps log-group retention,
paginated log reads, strict receipt validation and count comparison. A local
`ReceiptSummaryError` was not classified by the managed safe-error handler and
therefore looked the same as an unknown SDK/Python error. This is a possible
explanation of the category, **not proof that this exception occurred live**.

The prepared diagnostic change adds four fixed substeps, silent by default and
emitted once per phase, never per page/event. SDK operation exceptions become
fixed retention-read or page-read categories with their context suppressed.
Local receipt validation attaches an allowlisted fixed reason without changing
its original conditions, detailed local messages, calculations or canonical
output. The managed handler accepts only the exact local exception type and an
allowlisted string code; it never formats the exception, event position or SDK
payload. Malformed JSON can additionally be classified as a bounded sequence of
two or more **exact canonical** collector markers in one message, but it still
fails: there is no splitting, repair or acceptance of that framing. That shape
category cannot establish a producer or transport root cause. Prefixes, suffixes,
other formats/fields and exceeded diagnostic bounds remain generic failures.

Seven new synthetic tests cover every local category, forged/non-string codes,
hostile exception formatting/subclasses, actual SDK/validation/comparison
substeps, identical silent/progress pagination/proof, the real CLI's suppressed
receipt error path, and exact concatenated-marker diagnosis without recovery.
The receipt-summary tests are now included in CI and the existing pre-AWS source
test gate. The complete suite is 252 tests. All source schemas, counts, request
bounds/filter/pagination, raw workers, workflow timeouts, API pacing, window,
retention, acceptance and live manifests remain unchanged.

Require exact-head review, full tests/validators and CI before merge. Only after
fresh same-new-main managed health and independent run/artifact recovery checks
may one diagnostic acquisition be dispatched through the existing source gate.
Do not rerun an old attempt, infer a receipt cause from duration, relax a receipt
check, rebuild from an unverified partial capture or retry until PASS. No new
acquisition was dispatched during this offline diagnostic implementation.

### Historical start of the now-terminal bounded-read acquisition

PR #530 merged at `2026-09-05T14:44:20Z` as
`b467f416ddfbc36bcd9fe18cdf55b2377814c110`. Its reviewed head
`2698fca84f6c89631ba674bd278b27d1c45507f8` passed all four checks;
independent run metadata for CI `33972563940` and observability `33972563959`
matches that head. Security job `101323551794` actually executed the full
source/lifecycle step from `14:42:26Z` to `14:42:29Z`; the filtered log confirms
240 tests OK. Exact clean main was then synchronized and the workspace/window/
activation validators passed again. All three prior source runs were separately
rechecked as terminal without artifacts; no active or successful source existed.

Fresh same-main managed health run `33972739143` succeeded. Its sole artifact
`9971410127` has independently matched GitHub ZIP SHA-256
`9c9a4f33e0bb6255c4df5047edc119f525216ecd2a93563980898e37b32bcc3b`
and original canonical JSON SHA-256
`c3b230c075d1cb0e0d039e4b9d02e9b9537abce58ca640323e4df8656468f1d8`.
Run/repository/original-main ownership, single JSON, canonical bytes and hashes
were verified in memory, then an independent second download passed the offline
health/deployment/freshness/latest-reconciliation checks. Observation
`2026-09-05T14:45:47Z` proves `natural_reconciliation_verified` for September 5
03:45 Bratislava. No local file or raw AWS payload was retained.

After another exact-main and prior-artifact recovery check, dispatched exactly
one source run `33972852946`, attempt 1, created `2026-09-05T14:47:29Z` on that
original main. Job `101324325818` started `14:47:35Z`; the independent GitHub
readback shows `in_progress`, with all pre-AWS/credential steps passed and
canonical acquisition running. Upload and cleanup are not complete at this
observation. Do not duplicate or rerun it, infer speedup from startup, or treat
the running job as a complete source. A later documentation merge does not
replace the acquisition's original main or health identities.

Next wait for this exact run's terminal result. On success independently verify
its sole original source and health archives, GitHub ZIP digests, canonical JSON
hashes and the five named inputs from its original Git commit before any offline
recorder transition. On failure inspect only fixed sanitized phase/code and
artifact metadata, preserve any artifact for recovery and keep all existing
boundaries closed. No downstream gate or live experiment control was changed.

### Bounded conditional-read correction prepared for review

The managed source opts into eight conditional-read workers; the shared adapter
defaults to its existing serial behavior. The limit is an exact integer in
`1..8`, checked before any progress callback or I/O. A rolling queue holds at
most that many submitted, not-yet-reduced futures, rather than eagerly queuing
every retained object. Only the coordinator submits work and appends rows in
sorted inventory-key order; no worker mutates the inventory or result list.
Every original IfMatch, metadata, bounded-body, JSON and receipt-partition check
remains in the one-object reader. On failure the coordinator cancels queued
futures and drains started workers before the existing sanitized exception can
return. Every successfully acquired valid body closes in its `finally` block.
Strict all-row validation and the second sequential inventory occur only after
the executor has shut down. Empty input and the default serial path create no
pool. The source schema, canonical proof, byte/object limits, calculations,
transport retries/timeouts, order API pacing, workflow timeout, window, manifests
and all acceptance/producer gates are unchanged.

The managed S3 client is created/cached on the coordinator from its explicit
`boto3.Session` before worker submission. Workers receive that existing client,
not the Session/factory, and perform only read-only GETs without mutating client
metadata or installing Botocore event hooks. This follows the documented
[Boto3 client thread-safety conditions](https://docs.aws.amazon.com/boto3/latest/guide/clients.html).
The implementation deliberately avoids Python 3.11's eager `Executor.map` and
uses explicit pending-future cancellation plus the context manager's waiting
shutdown, as specified by the [Python 3.11 executor contract](https://docs.python.org/3.11/library/concurrent.futures.html).
There is no local AWS client construction or networked source test.

Five new deterministic tests cover real overlapping reads with forced
out-of-order completion and exact serial rows/proof equivalence; bounded future
submission; queued cancellation and draining an already-started body; submission
failure cleanup; pre-I/O worker limits; and unchanged metadata/JSON/partition/PII/
inventory rejection at every admitted concurrent limit. Existing managed tests
now exercise the real CLI client cache/worker path under raw-output suppression
and the real full synthetic capture's exact eight-worker opt-in. Tests verify
body closure and worker termination before downstream validation/return.
The three synchronization-sensitive cases also passed 25 repetitions each.

These are offline correctness checks, not a measured live speedup or proof that
the remaining acquisition will finish. After exact-head review, the full
240-test suite/validators and CI must pass before merge. Only then may the
coordinator recover-check every source run/artifact, verify fresh same-new-main
managed health independently, and dispatch one new attempt through the existing
source workflow. Do not rerun an old attempt or relax coverage if a later phase
fails. No source was captured and no downstream gate was opened by this change.

### Third acquisition terminal; raw-read elapsed time independently localized

Run `33968053395`, job `101311556789`, on original main
`afea00d095c0a06e46433991f9ae8b0593a01bfa` completed `cancelled` at
`2026-09-05T13:54:18Z`. Independently read GitHub check annotations prove the
maximum execution time was exceeded. Upload was skipped, cleanup succeeded and
independent run-artifact metadata proves zero artifacts. No live wait remains.

Only fixed sanitized log markers and cancellation metadata were inspected:

| Phase | UTC start on 2026-09-05 |
| --- | --- |
| runtime-preflight | 13:09:48.4797236 |
| retained-raw-source / raw-inventory-before | 13:09:52.3929228 / 13:09:52.3929962 |
| raw-conditional-reads | 13:09:59.5133737 |
| raw-event-validation | 13:53:02.1974561 |
| raw-inventory-after | 13:53:02.5227258 |
| receipt-parity | 13:53:10.5479016 |
| Runner cancellation | 13:54:15.2247884 |

The conditional-read phase consumed about 43m03s. The versioned adapter performs
these reads sequentially. The later markers prove strict event validation and
stable before/after inventory checks returned successfully, but neither receipt
parity nor the remaining source calculations completed. Do not equate a timeout
with quality FAIL, claim a missing/slow particular object, read raw logs or
attribute the first two failures to this exact cause.

Next review a bounded, deterministic conditional-read optimization through Git
and tests. Preserve the original request/IfMatch and response/body/receipt
checks, all object/byte bounds, sorted output, dual inventories, failure cleanup
and privacy suppression. Prove the managed client can be used safely; do not
submit an unbounded future per object or relax validation/timeouts. A future
acquisition still requires a separate reviewed implementation, no retained or
active source run, fresh independently verified same-main managed health and
the unchanged source gate. Do not rerun the old workflow attempt or retry until
PASS. No complete source or downstream gate was recorded by this investigation.

### Historical dispatch provenance of the now-terminal third acquisition

PR #527 merged as `afea00d095c0a06e46433991f9ae8b0593a01bfa` after the four
exact-head CI checks and 235 regressions passed. Both previous source runs were
independently rechecked as terminal with zero artifacts before any new dispatch.
Fresh same-main health run `33967911035` succeeded and was independently verified
twice, including sole artifact `9970017379`, GitHub ZIP digest
`ac6021d06d99e031b8fa4ab29048d804ecac50a9a2152b3965fdb370d9a76a44`,
single canonical JSON and JSON hash
`a72582eb192e15bb3924d7826f59e0f1a1ed4c20f0a3ebd743b55a7da1e43610`.
The original observation `2026-09-05T13:06:24Z` passed offline identity/deployment,
freshness and latest 03:45 Bratislava reconciliation validation. No local file
or raw AWS payload was retained.

Source run `33968053395`, job `101311556789`, was created at
`2026-09-05T13:08:58Z` on that exact original main using that health run/hash.
Pre-AWS validation and credential steps passed; acquisition began at
`13:09:45Z` and was `in_progress` at the metadata check before `13:19:06Z`.
Its subsequent terminal finding is recorded above. A later documentation merge
must not replace the source's original commit identity.

This run is no longer pending and produced no artifact to record. Preserve its
terminal provenance. Never infer failure from a polling timeout, change the
frozen source/acceptance rules, or recapture an existing artifact.

### Substep diagnostics prepared; no performance or acceptance change

Offline inspection localized the missing diagnostic boundary: the single raw
phase wraps initial inventory, conditional GETs, strict event validation and
final inventory. The adapter now has an optional silent-by-default callback
with those four exact constant names, admitted by the existing managed source
diagnostic allowlist. Each marker is emitted before its operation. It contains
no input-derived text, values, counts, identities or per-object progress.

Four new synthetic tests prove byte-equivalent rows/proofs and identical I/O
requests with/without diagnostics, every actual failing substep, fail-closed
invalid bounds/callbacks, and the real CLI/adapter error path with raw SDK output
suppressed. The full 235-test suite and validators pass. No parallelism, source
limit, transport/pacing/retry, job timeout, calculation or live manifest changed.
This is not a performance fix, live substep diagnosis or A/A PASS.

After separate exact-head review/CI/merge, one managed diagnostic acquisition
may identify the remaining substep only after clean-main synchronization,
independent recovery checks for every existing run/artifact, and fresh
independently verified same-main health. No acquisition was dispatched by this
implementation. Do not rerun an old attempt or use this instrumentation to
change a source/window/acceptance condition or retry until quality passes.

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
