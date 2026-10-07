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
