# VEVO GrowthBook storefront integration

Status: versioned Preview implementation only. Nothing in this directory has been published to GTM, BiznisWeb, GrowthBook, or AWS.

## Safety properties

- The client is inert on non-Slovak or non-`www.vevo.sk` pages, before analytical consent, with malformed config, when the SDK/payload fails, and for Production. `PRODUCTION_ACTIVATION` remains `false` in source.
- Analytical consent is read from BiznisWeb's public runtime bitmask: `FloxSettings.options.consent & FloxSettings.options.ANALYTIC`. The client also reacts to the existing `cookie_consent` data-layer event through the small bridge tag.
- No identifier, SDK, feature fetch, exposure, Meta dimension, or experiment storage is created before analytical consent. Withdrawal restores the CTA control class and deletes only `vevo_exp_*` / `vevo_gb_*` storage owned by this integration.
- The GrowthBook SDK is pinned to `@growthbook/growthbook@1.7.0` and loaded from the documented jsDelivr bundle with SHA-384 SRI. The manual bundle is used instead of `auto.min.js`, preventing GrowthBook's automatic GA4/GTM exposure forwarding and keeping the PII-free collector as the one exposure source.
- Core Web Vitals use Google's official `web-vitals@6.0.1` standard IIFE build, pinned with SHA-384 SRI and loaded only after consent and a valid assignment. Only the final numeric LCP/INP/CLS value is allowlisted; attribution targets, URLs, resource names, and DOM details are never collected.
- Visual Editor mutations, custom JavaScript injection, and URL redirects are disabled. GrowthBook supplies assignments; version-controlled code applies the one approved CTA class.
- The client never intercepts submit/cart behavior. BiznisWeb itself emits `dataLayer.event = add_to_cart` only after `items_added` exists; the bridge records the product ID after that success event and ignores BiznisWeb ecommerce value, price, name, and customer data.
- Purchase recording reads only a digits-only `transaction_id` from an existing `purchase` data-layer event and joins it to a prior anonymous assignment. It never evaluates a new experiment on the confirmation page and never forwards browser revenue.
- Meta dimensions are strict allowlists from `utm_source`, `utm_medium`, `utm_id`, `meta_adset_id`, `utm_content`, and `meta_placement`. Raw `fbclid`, `_fbp`, `_fbc`, names, query strings, and URLs are not stored or sent.
- Network requests use `credentials: omit`, `referrerPolicy: no-referrer`, a small exact JSON body, bounded retries with the same idempotent event ID, and the dedicated collector only.

## Frozen GrowthBook objects

Create these exact Preview feature/experiment contracts after the account exists:

| Feature key | Experiment key | Variation keys/values | Evaluation surface |
| --- | --- | --- | --- |
| `vevo-sk-aa-assignment` | `vevo-sk-aa-001` | `control`, `variant` | rendered Slovak home/product/category pages; no DOM difference |
| `vevo-sk-product-cta-color` | `vevo-sk-product-cta-color-001` | `control`, `brand_contrast` | only when `#product-detail .s1-detailCart .s1-submitCart` exists |

Variation metadata keys must match those strings exactly. CI now rejects a collector/reporting mismatch, and the browser refuses an unknown tracking key instead of applying the variant.

The frozen CTA hypothesis is: replacing only the current orange/red/pink background with VEVO's existing brand-gold gradient (`#c9a962` → `#b8956f`) and dark brand text (`#0f172a`) improves device-level add-to-cart rate without changing label, size, layout, selector, price, or cart behavior.

## Reproducible GTM Preview artifact

1. Keep `config.preview.example.json` versioned with its placeholder. Pass the actual Preview `sdk-...` client key and the host-verified Preview collector endpoint only through the task-scoped `VEVO_GROWTHBOOK_PREVIEW_CLIENT_KEY` and `VEVO_GROWTHBOOK_PREVIEW_COLLECTOR_URL` environment variables; do not create a local config copy. The builder validates both overrides and still requires Preview dev mode. The client key is public configuration, but it must not become an unreviewed local or committed source of truth.
2. Build the Custom HTML artifact outside the repository and remove it immediately after the exact content is pasted into the isolated GTM workspace:

```text
python scripts/build_vevo_growthbook_gtm_tag.py --config storefront/vevo-growthbook/config.preview.example.json --output <temporary-generated-preview-tag.html>
```

3. Record the printed SHA-256 marker. Paste the generated file unchanged into a GTM Custom HTML tag named `VEVO GrowthBook v1 - Loader - Preview`.
4. In GTM Preview only, run the loader at Initialization on all pages. Do not publish it. Add tag sequencing so it precedes the three bridge tags.
5. Configure these exact Custom Event bridges:
   - `cookie_consent` → `gtm-consent-bridge.html`;
   - `add_to_cart` → `gtm-add-to-cart-bridge.html`;
   - `purchase` → `gtm-purchase-bridge.html`.
6. During Preview, `window.VevoGrowthBook.getState()` exposes only status, page type, client version, and assigned variation keys; it does not expose the device ID or any order/customer value.

## Preview acceptance before any publish

- Reject/no-choice: control DOM, zero `vevo_exp_*` storage, zero GrowthBook/collector requests.
- Accept Analytical: one stable anonymous device ID, exact allowed GrowthBook payload, exact exposure events, and the same variation after reload.
- Withdraw Analytical: control restored immediately, owned storage removed, no later event delivery.
- Both A/A arms are visually identical. Both CTA arms preserve the exact button text, box geometry, product/price/cart behavior, and checkout.
- BiznisWeb `add_to_cart` produces at most one binary 24-hour fact per exposed device; repeated raw events do not inflate the metric.
- A purchase event sends only the exact transaction ID for prior assignments and joins the BiznisWeb order in reporting.
- SDK/payload, configuration, storage, or selector failure leaves control. Collector/network failure never blocks cart or checkout, suppresses downstream same-page cart/health facts whose exposure was not accepted, and fails the Preview/A/A rollout gate.
- Mobile/desktop screenshots, console/network checks, Core Web Vitals, client-error events, duplicate GA4/Meta purchase checks, and the rollback procedure all pass.

No production activation or GTM publish is allowed until the repository hard-gates, consent classification, retention decision, GrowthBook workspace, AWS Preview identity, on-host/architecture verification policy, and signed Preview checklist are complete.
