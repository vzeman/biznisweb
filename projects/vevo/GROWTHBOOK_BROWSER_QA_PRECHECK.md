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
