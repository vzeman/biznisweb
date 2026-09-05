# A/A quality-source audit — 2026-09-05

Status: `QUALITY_SOURCE_WINDOW_BINDING_BLOCKED`. This is a source-contract
finding, not a measured A/A failure or permission to restart the experiment.

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

**This is not yet a live quality-source capture or a completed repair.** There
is no new source workflow, runtime deployment, source artifact, consumer gate
opening or producer dispatch. The reserved workflow path in the new envelope
is a contract for the next implementation, not evidence that a run exists.
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
workspace behavior. Window/workspace validators, security and diff checks pass.
An additional 100 synthetic differential cases matched all three ordinary
outputs against the pre-change committed core byte-for-byte after canonical
JSON serialization, including duplicates, contamination, orphans and ambiguity.
Only synthetic input was used; real cohort eligibility still requires source
production and must exactly equal the already resolved checkpoint, or stop.

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
