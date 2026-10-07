# ROY reporting project state

Date: 2026-10-07
Repo: `vzeman/biznisweb`
Branch: `codex/roy-reporting-cost-discount-audit-20261007`

What changed:

- The owner explicitly requested corrections to ROY reporting for foreign tip/insurance costs, order discounts and analogous calculation defects, followed by production deployment.
- Started independent primary-source catalogue/payment/order checks, shared monetary-calculation review and current-runtime release mapping. This is reporting work; storefront prices, visibility, orders, payments and financial documents must not be mutated.
- Client-specific ROY policies stay in this project. Shared calculations and release safety belong to reusable core code, with an explicit project identity.

What is verified:

- Clean branch from main `50bc9670a8d4ee537eb9aeeb61113a6180c7d692`; repository, status, fetch and pull verified before work.
- Fresh read-only Meta and Google country-spend probes use the ROY accounts, verify EUR currency and reconcile exactly to each provider's daily totals over the checked monthly window. Private evidence stays under ignored `data/roy-reporting-audit-20261007/` until encrypted archival; no commercial values or customer data belong in public Git.
- Existing ROY fixed, packaging, shipping and acquisition-cost assumptions differ from VEVO and must not be copied from it. Current service identities and payment IDs require direct ROY catalogue evidence before configuration changes.

Known issues:

- ROY currently lacks the new explicit service-cost and header-discount policies. Its financial payment-ID allowlist differs from its invoice allowlist; determine actual classification impact from current source evidence before changing it.
- The existing VEVO-only release controller cannot be used unchanged for ROY. Verify the actual ROY task definition, immutable image, sink, secret references and host path; a candidate must pass exact task/IP/service/path and localhost marker gates before promotion.
- VEVO remains paused with its retained uncertain lease. Preserve it and every other runtime throughout ROY deployment. Wait for the settings-triggered monthly accounting workflow to settle before recording the protected-schedule baseline.

Next exact step:

Finish the source catalogue, order/discount and runtime readbacks; implement only evidence-backed ROY settings and necessary shared-calculation corrections with synthetic regressions. Review the project-scoped release path, merge after required CI, build an immutable image, then run the full-history isolated probe and report-only regeneration through 2026-10-06. Disable email, invoices and creditnote mutation for that regeneration. Independently reconcile published outputs and record the exact deployed runtime and cleanup. No persistent local runtime has been started.

## Calculation and release implementation ready for CI

Date: 2026-10-07
Repo: `vzeman/biznisweb`
Branch: `codex/roy-reporting-cost-discount-audit-20261007`
What changed: ROY now explicitly enables source-total order-discount reconciliation and measured country advertising allocation. Its verified tip/insurance identities override every acquisition-cost rule to zero across countries. Financial payment IDs include current source catalogue IDs while retaining verified historical IDs. Native money precision comes from the source currency catalogue. The shared reconciler proves raw/display consistency and native line/unit VAT rounding without widening its grand-total tolerance; only zero-tax/free net-denominated totals are accepted. Existing bundle component accounting now also determines parent CSV cost, with the former parent lookup preserved as a reference. No acquisition-price list, fixed/packaging/shipping model, storefront or invoice policy was changed.

What is verified: Fresh primary coverage spans the complete production history through 2026-10-06. Every financially included order passes strict monetary reconciliation and CSV/component cost equality. The existing excluded internal-license metadata exception remains explicit; missing price elements were not fabricated and no exception was broadened. All observed services match the two verified identities. Local combined CI-equivalent regression passes 1,348 tests across 76 modules, including synthetic currency/discount/bundle failures and project release lifecycle/CAS/race boundaries. Critical Ruff and whitespace checks pass. The legacy payment-cache unit fixture explicitly disables unrelated monetary checks; production reconciliation remains enabled.

Runtime boundary: Read-only production discovery confirmed account `919341186960`, region `eu-central-1`, cluster `vevo-reporting-cluster`, service `roy-daily-report-email`, active definition `roy-reporting-daily:77`, immutable image `sha256:638e628781783c2b4897f93c97eaec8a2c1db31f96fc18b03a30155e77b7283f`, sink `daily-reports/roy-sk` and image working path `/app`. No active report task existed at discovery, so task/IP are established again on the candidate. Preflight SHA-256 is `0dbf5d503874cad6f4bda28864ad52fd9d88071adeecb603087c8d495d58f294`. New release policy is project-scoped; ROY owns a separate CAS lease and isolated probe role. It preserves the exact retained VEVO lease/body/ETag, disabled schedule and published outputs. An active peer always blocks. Every host marker and full/7d/30d/90d artifact must pass before promotion, with fresh report-only regeneration and no one-off email.

Known issues / limits: No nonzero historical service cost was observed in this ROY source; the new explicit policy prevents future mapped/fallback cost overrides. Future custom service translations not present in source cannot be inferred and require verified identity mapping. Existing estimated product costs remain estimates, not supplier-invoice validation. A first combined test run exposed a stale legacy-only cache fixture after enabling ROY reconciliation; its isolation was repaired and the complete rerun passed. No production publication is claimed yet. Do not change VEVO's retained paused state.

Next exact step: Finish independent review and private encrypted evidence archival, commit/push, merge after required CI, then wait for both the exact image build and automatic monthly accounting workflow. Freeze that clean main commit throughout the ROY release. Repeat task/IP/service/path and localhost gates before UI verification; archive terminal readbacks and update this product handoff. No persistent local server, worker, watcher or tunnel has been started.
