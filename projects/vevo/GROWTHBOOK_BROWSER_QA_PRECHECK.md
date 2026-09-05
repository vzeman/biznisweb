# Production A/A browser visual precheck

Observed: 2026-09-05, completed by `2026-09-05T12:08:13Z`.
Scope: read-only public storefront navigation in the connected Chrome browser.

This is **partial supporting observation**, not the canonical manual-QA
observation, not a passed manual evidence component, and not A/A PASS. It must
not open any producer or substitute for the remaining versioned QA checks.

## Directly observed

- The public [homepage](https://www.vevo.sk/) rendered its navigation and product
  links. Navigating its Ylang Absolute link opened the intended public
  [product detail](https://www.vevo.sk/p-1531/parfum-do-prania-vevo-no-07-ylang-absolute).
- At the browser's original desktop viewport, the detail showed the selected
  500 ml option, product image, EUR 25.90 price and the orange-gradient
  `Pridať do košíka` button. The button label was unchanged from the expected
  baseline. This visual observation does not inspect the runtime feature class
  or prove an experiment's allocation/configuration.
- With a temporary `390 x 844` viewport override, the title and product image
  rendered in the responsive layout. After scrolling, the selected volume,
  price, quantity control and orange-gradient button were visible and legible.
  This is desktop Chrome responsive-layout inspection, not a physical mobile
  device or mobile-browser compatibility test.

## Boundaries preserved

- No add-to-cart, quantity, variant, checkout, order, consent or subscription
  control was clicked. The pre-existing user cart was left untouched.
- No GrowthBook, GTM, Meta Ads or BiznisWeb administration page was opened or
  changed. No experiment results, event/device identity, storage value or
  network payload was inspected.
- The viewport override was reset to default and the agent-created temporary
  QA tab was closed. User tabs were not closed; no screenshot file or local
  application process was created.

## Still required / not established

- Actual Tag Assistant connection and the exact Production GrowthBook/GTM
  configuration readback.
- Consent acceptance, rejection, withdrawal and regrant event behavior.
- Browser-console/runtime checks and duplicate GA4/Meta purchase-event checks.
- Authorized add-to-cart/checkout behavior and rollback verification.
- Full desktop/mobile QA under the versioned manual-observation contract.
- Successful frozen-source and automated evidence, both independently recorded
  components, and the protected A/A snapshot/evaluator PASS.

Do not turn these missing checks into asserted booleans. At this visual precheck
the source run was `33964597883`; it subsequently timed out without an artifact.
The current source acquisition and its original-main identity are recorded in
`GROWTHBOOK_AA_QUALITY_SOURCE_AUDIT.md`; do not duplicate it.

## Authenticated GTM readback — 2026-09-05

Observed by `2026-09-05T13:19:06Z`, through read-only navigation from the GTM
homepage to the exact account `6254499282` and container `198135331`
(`GTM-5ZB5LFGB`). No current Tag Assistant session was connected.

The fully loaded [Versions page](https://tagmanager.google.com/#/versions/accounts/6254499282/containers/198135331/versions)
explicitly reports **version 19 as published**. The current activation manifest
and stop/readback runbook still require version 15. This is
`GTM_LIVE_VERSION_DRIFT`, not a successful version-15 readback.

The displayed publication history (UI time; no UTC conversion asserted) is:

| Version | Displayed title | Published |
| --- | --- | --- |
| 19 | VEVO validation signal only 2026-09-04 | 2026-09-04 14:29:07 |
| 18 | VEVO guest checkout late-render fix 2026-09-04 | 2026-09-04 14:18:19 |
| 17 | VEVO guest checkout and validation diagnostics 2026-09-04 | 2026-09-04 14:14:15 |
| 16 | VEVO UX checkout funnel and JS diagnostics 2026-09-04 | 2026-09-04 13:59:24 |
| 15 | VEVO GrowthBook Production loader – zero-allocation | 2026-08-24 16:34:29 |

Version 19's detail identifies one modified tag in its immediate change list:
`VEVO UX - Checkout validation signal`. Its description claims the unreliable
guest-checkbox mutation was removed and validation measurement retained. That
is the publisher's description, **not independently verified code behavior or
a privacy/commerce guarantee**. The earlier intermediate versions were not
opened and their code was not reviewed.

The version's full tag list still contains the four Production GrowthBook tags:
Loader, Consent Bridge, Add to Cart Bridge and Purchase Bridge. Their displayed
modification dates are August 24; the loader's displayed trigger is
`Initialization - All Pages`. The list also contains two VEVO UX checkout/Clarity
diagnostic tags. Names/dates/triggers alone do not prove source bytes, tag
sequencing, consent metadata, runtime behavior, absence of duplicate events or
canonical QA. No tag source, secret, event payload or customer/order data was
opened or exported.

The workspace picker listed Default Workspace with zero changes and VEVO
GrowthBook Preview with five changes; the previously recorded Production A/A
workspace was not listed. This observation does not authorize discarding any
draft or recreating a workspace.

All four listed September 4 publications postdate the resolved through-boundary
`2026-09-03T22:00:00Z`. That fact does not automatically invalidate historical
data and does not establish current QA or absence of impact. Preserve the fixed
window and existing evidence. Do not change a recorded version number to make
the mismatch disappear, restore a historical container over unrelated work,
publish a draft, stop A/A or open paid/CTA gates.

Next: reconcile ownership of the September 4 changes and perform a read-only
version-15 versus version-19 code/trigger/consent/sequencing review. Any resulting
compatibility contract requires its own reviewed Git transition; the browser
observation cannot modify historical activation proof or substitute for full QA.

## Completed read-only rendered-code comparison and diagnostic review

Observed on 2026-09-05, completed before `14:02:13Z`. The exact GTM account,
container and historical/current version panels above were used. The comparison
opened only read-only tag views; no section edit mode, save, publish, rollback,
preview, consent, checkout or commerce action was used.

### Method and GrowthBook findings

The editor virtualizes source lines. Only its actual rendered DOM was read:
each line's displayed number and text were collected while scrolling the code
pane through the browser UI. Repeated overlap had to be identical, every line
from 1 to the final line had to be present, and the editor's bottom had to be
reached. No hidden application/editor state, API request, storage or clipboard
was used. Every corresponding displayed line matched between versions 15 and
19. Hashes below cover the UTF-8 encoding of those displayed lines joined by LF.

| Production tag | Complete displayed lines per version | Equal rendered-text SHA-256 in v15 and v19 |
| --- | --- | --- |
| Loader | 1089 | `20a349438e14d6bdf7027e11546c88e9d9842ef7f9caf2337081dc8590fab4a5` |
| Consent Bridge | 6 | `9c08c87cccb2c9eb00e60d49733a58d56fec922a0246e9dc29fbd58c6c4f295e` |
| Add to Cart Bridge | 6 | `d4658136f4c57dde511961cdc9c319ac1a288e6097745c2d9da29a7ce9b09280` |
| Purchase Bridge | 6 | `e4a042330aae612af3e682752a354a142441b9a8113a090121b3faa30247acf2` |

Both versions display no additional consent requirement for all four tags, the
loader on `Initialization - All Pages`, and that loader as each bridge's setup
tag. Bridge trigger names match: `CE - cookie_consent`,
`CE - add_to_cart - VEVO GrowthBook Production`, and `CE - Purchase`.

These hashes are **not original source/export bytes or a canonical GTM artifact
binding**. DOM rendering can represent whitespace differently. This comparison
does not establish all hidden flags (including the setup-failure option), full
trigger definitions, all unrelated tags, live downloaded container bytes,
Tag Assistant delivery, absence of duplicate purchases, consent behavior,
performance or complete manual QA. Do not replace the original activation hash
or version with these supporting observations.

### Ownership and independent review of the two additional tags

The existing task `Analyzuj Clarity pre vevo.sk`
(`01a06c07-010f-7872-af60-630484c8c2a5`) records publication of versions 16–19
and removal of an unreliable checkout-checkbox workaround before version 19.
That record identifies the related work; it is not new authority or independent
proof of the publisher's assertions. No message was sent to that task and no
customer recording, shared recording link or customer-level observation is
retained in this audit.

Current version 19's two additional tags were read completely through the same
rendered-DOM method and reviewed statically without executing their source:

- `VEVO UX - Checkout validation signal`: 55 displayed lines, rendered-text
  SHA-256 `5a1fbac00df7d1f66df0d7f94d6d405c6e881c4bb584ac94aa03627603ef381d`.
  It checks for error-class form controls on `cart_form`/`save`, sends fixed
  Clarity stage/event names once when available, observes subtree changes and
  polls at most 40 times. It does not read the invalid control's value or assign
  a form value/checked state. Its MutationObserver remains attached until the
  page lifecycle ends; current performance has not been measured.
- `VEVO UX - Clarity checkout funnel + JS diagnostics`: 334 displayed lines,
  rendered-text SHA-256
  `212f775d9f749b004272460cb4a1b98f32081c9d1f92923204a14b8139153dcf`.
  It contains three concatenated script blocks sharing the same initialization
  guard. In the normal sequential path the first sets that guard and later
  blocks return; this is redundant code, not by itself proof of triple events.
  The active code sends fixed funnel stages, tests the submitter/active-element
  label (without forwarding that label), and captures runtime/resource/promise
  errors under `/e/orders`. It limits distinct errors to five per page and sends
  error type, message, source, line, column and user agent as Clarity custom tags.

Neither reviewed tag contains a direct form-value/checkbox assignment, form
submission or direct GrowthBook call. Both display `All Pages` as their GTM
trigger and enforce path filtering in code. The observation does not establish
consent-aware delivery by the existing Clarity integration or the absence of
other script effects.

### Verified static privacy weakness — not a verified live leak

`CLARITY_DIAGNOSTIC_FREE_TEXT_PRIVACY_RISK`: the error sanitizer removes only
email-like substrings and digit sequences of at least seven digits, then clips
to 255 characters. The URL sanitizer drops query and fragment but retains the
path. Arbitrary runtime/promise error text can therefore retain names, addresses,
short order-like values or other identifiers; resource paths can retain embedded
identifiers. No claim is made that any actual customer value reached Clarity.
Do not inspect customer sessions to try to prove or dismiss this static risk.

The observed transformations were reproduced with purely synthetic input; this
is a standalone diagnostic replica, not a passed production QA component:

```javascript
const cleanMessage = value => String(value || 'unknown')
  .replace(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi, '[email]')
  .replace(/\b\d{7,}\b/g, '[number]').slice(0, 255);
const cleanUrl = value => String(value || 'unknown')
  .split('#')[0].split('?')[0].slice(0, 255);
const sample = 'Name SYNTHETIC_PERSON; address SYNTHETIC_STREET 12; order 1234';
console.assert(cleanMessage(sample) === sample); // free text survives
console.assert(cleanMessage('demo@example.invalid 12345678') === '[email] [number]');
console.assert(cleanUrl('https://example.invalid/SYNTHETIC_PERSON/order-1234?secret=discard#fragment')
  === 'https://example.invalid/SYNTHETIC_PERSON/order-1234');
```

All assertions were independently checked as true without contacting the shop,
Clarity or AWS. No source export, screenshot or credentials file was created.
Ephemeral browser code copies were cleared and its JavaScript session reset.

Next: keep canonical manual-QA/stop/paid/CTA gates closed. Reconcile version
compatibility without rewriting history, and separately review a narrowly scoped
correction of the owning UX tag to fixed diagnostic categories rather than
arbitrary free text/paths, with consent and performance verification. Do not
publish or roll back GTM under this read-only investigation.
