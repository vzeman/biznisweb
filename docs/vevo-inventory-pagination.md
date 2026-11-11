# VEVO order pagination resilience

## Observed provider behavior

On 2026-11-11 the scheduled VEVO report failed while enumerating orders. Repeated native API reads at offset10440 / limit30 returned19 ordered rows, `totalRecords=10460`, `hasNextPage=false`, `nextCursor=null`. The opposite direction returned the same terminal count; the ASC tail matched the reverse DESC head suffix. This is an inconsistent reported count. The evidence does not identify a hidden or deleted order.

## Runtime behavior

Reporting opts into two bounded rechecks of a validly shaped page with contradictory numeric metadata. There are at most six rechecks per scan. Failed responses are discarded. The original maximum-ID anchor, continuity proof, page budget and deadline are retained. Invalid identities, malformed pages, business-validation failures and unreliable cursors stay fatal.

A stable terminal anomaly is accepted only when all of these conditions hold:

- Full unfiltered reporting scan, short nonempty ASC page, and exactly one excess declared record.
- Explicit false/null continuation metadata, exact prior `(id, order_num)` overlap and original maximum anchor.
- A fresh ASC read returns identical ordered identities and terminal metadata.
- A fresh complete DESC head has valid metadata, unchanged total and anchor, and an exactly matching reversed suffix.
- A fresh ASC read at the observed end with the normal page size is empty with explicit false/null continuation and unchanged total.

Every proof read uses normal caller row validation and the same scan budget. Any failed proof stops the calculation. Source metadata is preserved; a structured numeric warning records the verified anomaly. Invoice/cancellation callers do not enable either new option. No amount, order eligibility, refund, currency, advertising or publication-quality policy changes.

## Independent diagnosis

From a committed checkout with the existing AWS profile:

```text
python -B scripts/diagnose_reporting_inventory_pages.py --project vevo --profile codex --output data/vevo-pagination-diagnostic/bidirectional-identities.private.json
```

The script refuses concurrent report tasks and uncommitted script bytes. It performs only allowlisted HTTPS read queries, uses bounded time/pages and existing API pacing, compares complete ordered identity sequences in both directions and writes private evidence only inside ignored `data/`. Console output contains counts/hashes and numeric anomalies. It never authorizes publication or changes provider state. Use a new output path per run; do not overwrite evidence.

If diagnostics differ or a new anomaly appears, preserve the existing strict gate. Do not treat stable repeated bad metadata as proof of complete data. Establish fresh runtime identity, inspect the safe diagnostics and use a reviewed repair before another publication. Existing `vevo-reporting-run-failed` monitoring remains the failure signal.

## Release verification

Use the normal exact-image VEVO controller after PR checks/build, with email and invoice writes disabled. Verify its candidate and live localhost markers, all four period artifacts, final enabled scheduler, released lease and terminal tasks. Verify authenticated report/API behavior before the browser. A UI image change is unnecessary for an ingestion-only fix. Do not blindly rerun an uncertain release.
