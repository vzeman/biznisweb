# Project status identities

The automation uses a reviewed project/ID contract when VEVO presents renamed
statuses. It does not rename shop settings or translate arbitrary labels. ROY
retains its own catalogue and receives no VEVO aliases.

| VEVO ID | Stable internal name | Observed current label |
| --- | --- | --- |
| 4 | Odoslaná | Shipped |
| 17 | Storno | Cancelled |
| 31 | Platba online - zaplatené | Payment online - paid |
| 33 | Platba online - platnosť vypršala | Payment online - expired |
| 34 | Platba online - platba zamietnutá | Payment online - expired |

IDs 33 and 34 retain distinct timeout and rejected-payment meanings despite the
same visible English label. Historical native evidence binds the GoPay events
TIMEOUTED and CANCELED to these IDs; it does not make a gateway timeout evidence
of an unpaid bank transfer. ID 1 is not aliased because its earlier VEVO label
was not proven. Other valid catalogue rows retain their current names.

The source-controlled contract in `order_status_identity.py` records five private
evidence hashes. All objects are under
`data/vevo/order-automation/audits/2026-09-13/`:

- Current native catalogue: `current-native-status-catalogue-20260913.json`,
  SHA `879d05d452b7a29842fca578278b841b63f9340a4ffc8d8bcbac195fcaf0952b`.
- Historical shipped status: `status-review-user-restored-readback-20260913.json`,
  SHA `c4621d2d3792a53320706f2bcfee8429b5d443e69a3eabd250ad14f7029c1aea`.
- Historical Storno status: `native-creditnote-invoice-key-contract-20260913.json`,
  SHA `523a2c2d5c7677f4e0889cdffed116e863865e814090c79207c7b904171dc70e`.
- Historical event/ID bindings and transliterated labels:
  `gopay-native-status-guard-inspection-20260913.json`,
  SHA `4c28ff3ace67ae583249ef85a5f01aeef9c5128608b79a6d8e67ea70d00499bc`.
- Historical exact timeout label: `status-review-two-case-api-20260913.json`,
  SHA `f7b1b0ddd42a7fd038088e94e235fcc2e812affb6283a1362d95ad6b4f9dc5cc`.

The historical labels for 31 and 34 were transliterated in the sanitized proof.
Accent comparison is permitted only inside the exact reviewed project/ID binding;
it is not an English dictionary or a match on the words “paid” or “cancelled”.

Runtime factories bind the actual API client, project and transport endpoint.
The active same-shop API catalogue is checked once for a scan, then freshly before
a mutation intent. Missing/duplicate reviewed IDs, an unapproved observed label,
or a duplicate canonical role stop the affected operation. A failed refresh
invalidates the previous catalogue. Inventory rows referring to unrelated inactive
IDs remain in the complete scan as unbound evidence and cannot qualify by name;
fresh candidate details still require a valid catalogue identity.

Canonical copies retain `raw_status` and `status_identity_contract`. New status
intents retain canonical identities plus `source_raw_status`, `target_raw_status`
and the contract version; available independent readback retains
`verified_raw_status`. New invoice preparation/finalization/email intents retain
their status-binding evidence. Sealed confirmation documents and earlier consumed
intents are not rewritten. Manual settlement remains independent of gateway and
native financial/return evidence takes precedence as described in the existing
settlement runbook.

Status changes still make one mutation request and independently read the result.
Wrong order/ID, unexpected label, catalogue drift, partial data and uncertain
outcomes remain blocked; an ambiguous request is never replayed. The invoice,
unpaid cancellation, creditnote and fixed closure paths use the shared binding,
as do their fresh verification helpers. Reads use existing bounded backoff and
lease callbacks, without an additional catalogue request for each inventory row.

This source change does not update protected reporting images. The separate
standalone guard migration preserves ROY reporting task 71 and VEVO reporting
task 33 image pins. Their older name-based revenue/segmentation filters therefore
still require the separately reviewed reporting correction; disabling the older
inline guard alone cannot fix those filters. No protected report, gateway setting,
payment mapping or provider record is changed by this code preparation.
