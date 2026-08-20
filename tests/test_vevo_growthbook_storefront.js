"use strict";

const assert = require("node:assert/strict");
const test = require("node:test");
const createClient = require("../storefront/vevo-growthbook/vevo-growthbook.js");

class MemoryStorage {
  constructor(seed = {}) {
    this.values = new Map(Object.entries(seed));
  }
  get length() { return this.values.size; }
  key(index) { return Array.from(this.values.keys())[index] ?? null; }
  getItem(key) { return this.values.has(key) ? this.values.get(key) : null; }
  setItem(key, value) { this.values.set(String(key), String(value)); }
  removeItem(key) { this.values.delete(String(key)); }
}

class FakeEventTarget {
  constructor() { this.listeners = new Map(); }
  addEventListener(name, handler) {
    if (!this.listeners.has(name)) this.listeners.set(name, new Set());
    this.listeners.get(name).add(handler);
  }
  removeEventListener(name, handler) {
    if (this.listeners.has(name)) this.listeners.get(name).delete(handler);
  }
  dispatch(name) {
    for (const handler of this.listeners.get(name) || []) handler({ type: name });
  }
}

class FakeClassList {
  constructor() { this.values = new Set(); }
  add(value) { this.values.add(value); }
  remove(value) { this.values.delete(value); }
  contains(value) { return this.values.has(value); }
}

function createElement(tagName) {
  const target = new FakeEventTarget();
  target.tagName = tagName.toUpperCase();
  target.id = "";
  target.textContent = "";
  target.parentNode = null;
  target.classList = new FakeClassList();
  target.getAttribute = (name) => target.attributes?.[name] ?? null;
  target.setAttribute = (name, value) => {
    target.attributes = target.attributes || {};
    target.attributes[name] = String(value);
  };
  return target;
}

function makeRoot(options = {}) {
  const product = createElement("div");
  product.setAttribute("data-product-id", options.productId || "1531");
  const button = createElement("button");
  const head = createElement("head");
  head.children = [];
  head.appendChild = (element) => {
    element.parentNode = head;
    head.children.push(element);
    return element;
  };
  head.removeChild = (element) => {
    head.children = head.children.filter((candidate) => candidate !== element);
    element.parentNode = null;
  };
  const document = new FakeEventTarget();
  document.readyState = "complete";
  document.visibilityState = "visible";
  document.head = head;
  document.documentElement = {
    getAttribute: (name) => name === "data-lang-code" ? (options.lang || "sk") : null,
  };
  document.createElement = createElement;
  document.getElementById = (id) => head.children.find((element) => element.id === id) || null;
  document.querySelector = (selector) => {
    if (selector === "#product-detail[data-product-id]") return options.page === "product" ? product : null;
    if (selector === "#product-detail .s1-detailCart .s1-submitCart") return options.page === "product" ? button : null;
    return null;
  };

  let uuidCounter = 1;
  const fetches = [];
  const root = new FakeEventTarget();
  root.document = document;
  root.location = {
    protocol: "https:",
    hostname: options.hostname || "www.vevo.sk",
    pathname: options.path || (options.page === "product" ? "/p-1531/example" : "/"),
    search: options.search || "",
  };
  root.URL = URL;
  root.URLSearchParams = URLSearchParams;
  root.Uint8Array = Uint8Array;
  root.crypto = {
    randomUUID() {
      const suffix = uuidCounter.toString(16).padStart(12, "0");
      uuidCounter += 1;
      return `00000000-0000-4000-8000-${suffix}`;
    },
  };
  root.localStorage = options.localStorage || new MemoryStorage();
  root.sessionStorage = options.sessionStorage || new MemoryStorage();
  root.FloxSettings = { options: { consent: options.consent ? 2 : 0, ANALYTIC: 2 } };
  root.dataLayer = options.dataLayer || [];
  root.AbortController = AbortController;
  root.fetch = async (url, init) => {
    fetches.push({ url, init, body: JSON.parse(init.body) });
    return options.collectorFailure
      ? { ok: false, status: 503 }
      : { ok: true, status: 202 };
  };
  let timerId = 0;
  const pendingTimers = new Map();
  root.setTimeout = (handler, delay) => {
    timerId += 1;
    if (delay <= 500) {
      const native = setTimeout(handler, 0);
      pendingTimers.set(timerId, native);
    }
    return timerId;
  };
  root.clearTimeout = (id) => {
    if (pendingTimers.has(id)) clearTimeout(pendingTimers.get(id));
    pendingTimers.delete(id);
  };
  root.setInterval = () => ++timerId;
  root.clearInterval = () => {};
  root.PerformanceObserver = undefined;

  const variations = options.variations || {
    "vevo-sk-aa-001": "control",
    "vevo-sk-product-cta-color-001": "brand_contrast",
  };
  let sdkInstances = 0;
  class FakeGrowthBook {
    constructor(settings) {
      this.settings = settings;
      this.destroyed = false;
      sdkInstances += 1;
    }
    async init() { return options.sdkFailure ? { success: false } : { success: true }; }
    getFeatureValue(featureKey, fallback) {
      const experimentId = featureKey === "vevo-sk-aa-assignment"
        ? "vevo-sk-aa-001"
        : "vevo-sk-product-cta-color-001";
      const variation = variations[experimentId] || fallback;
      const trackingKey = options.invalidTrackingKey && experimentId === "vevo-sk-product-cta-color-001"
        ? "1"
        : variation;
      this.settings.trackingCallback(
        { key: experimentId },
        { key: trackingKey, value: variation },
      );
      return variation;
    }
    destroy() { this.destroyed = true; }
  }
  class FakeStickyService { constructor(settings) { this.settings = settings; } }
  root.growthbook = {
    GrowthBook: FakeGrowthBook,
    LocalStorageStickyBucketService: FakeStickyService,
    configureCache(settings) { root.cacheSettings = settings; },
    setPolyfills(settings) { root.sdkPolyfills = settings; },
  };
  if (!options.foreignGrowthBook) {
    const sdkScript = createElement("script");
    sdkScript.id = "vevo-growthbook-sdk-v1";
    sdkScript.src = "https://cdn.jsdelivr.net/npm/@growthbook/growthbook@1.7.0/dist/bundles/index.min.js";
    sdkScript.integrity = "sha384-LE9sSbxrM6BIe5z0T5qNuBymAEx7Iwp14FYi2TtCWSalftZaK5cG7ckbe3hNSRPK";
    head.appendChild(sdkScript);
  }
  const vitalHandlers = {};
  if (options.withWebVitals) {
    root.webVitals = {
      onCLS(handler) { vitalHandlers.CLS = handler; },
      onINP(handler) { vitalHandlers.INP = handler; },
      onLCP(handler) { vitalHandlers.LCP = handler; },
    };
    const vitalsScript = createElement("script");
    vitalsScript.id = "vevo-web-vitals-v1";
    vitalsScript.src = "https://cdn.jsdelivr.net/npm/web-vitals@6.0.1/dist/web-vitals.iife.js";
    vitalsScript.integrity = "sha384-xduvx5szsAXW0V0fxOYjfsvz/Zl93SEZcLM+BK+7y6Spco3N+8g8NjbtUIAWCCAQ";
    head.appendChild(vitalsScript);
  }

  return {
    root,
    document,
    product,
    button,
    fetches,
    sdkInstances: () => sdkInstances,
    vitalHandlers,
  };
}

function config(overrides = {}) {
  return {
    schemaVersion: 1,
    environment: "preview",
    clientKey: "sdk-abcdefgh12345678",
    apiHost: "https://cdn.growthbook.io",
    collectorUrl: "https://abc123.execute-api.eu-central-1.amazonaws.com/v1/events",
    allowedHost: "www.vevo.sk",
    gtmContainerId: "GTM-5ZB5LFGB",
    enableDevMode: true,
    ...overrides,
  };
}

test("stays control and stores nothing before analytical consent", async () => {
  const fixture = makeRoot({ page: "product", consent: false });
  const client = createClient(fixture.root, config());
  await client.ready();

  assert.equal(client.getState().reason, "analytics_consent_absent");
  assert.equal(fixture.sdkInstances(), 0);
  assert.equal(fixture.fetches.length, 0);
  assert.equal(fixture.root.localStorage.length, 0);
  assert.equal(fixture.button.classList.contains("vevo-gb-cta-brand-contrast"), false);
  client.destroy();
});

test("assigns only after consent, captures safe Meta IDs, and records successful cart event", async () => {
  const fixture = makeRoot({
    page: "product",
    consent: true,
    search: "?utm_source=meta&utm_medium=paid_social&utm_id=123&meta_adset_id=456&utm_content=789&meta_placement=instagram_feed&fbclid=forbidden",
  });
  const client = createClient(fixture.root, config());
  await client.ready();

  assert.equal(client.getState().status, "active");
  assert.deepEqual(client.getState().variations, {
    "vevo-sk-aa-001": "control",
    "vevo-sk-product-cta-color-001": "brand_contrast",
  });
  assert.equal(fixture.button.classList.contains("vevo-gb-cta-brand-contrast"), true);
  assert.equal(fixture.fetches.filter((row) => row.body.event_name === "experiment_exposure").length, 2);
  for (const row of fixture.fetches) {
    assert.equal(row.body.meta_campaign_id, "123");
    assert.equal(row.body.meta_adset_id, "456");
    assert.equal(row.body.meta_ad_id, "789");
    assert.equal(row.body.meta_placement, "instagram_feed");
    assert.equal(JSON.stringify(row.body).includes("fbclid"), false);
    assert.equal(row.init.credentials, "omit");
  }

  await client.recordAddToCart();
  const cartRows = fixture.fetches.filter((row) => row.body.event_name === "add_to_cart");
  assert.equal(cartRows.length, 2);
  assert.ok(cartRows.every((row) => row.body.product_id === "1531"));
  assert.ok(cartRows.every((row) => !("value" in row.body) && !("price" in row.body)));
  assert.equal(fixture.root.cacheSettings.cacheKey, "vevo_gb_features_v1");
  assert.equal(typeof fixture.root.sdkPolyfills.fetch, "function");
  const beforeDestroy = fixture.fetches.length;
  client.destroy();
  assert.equal(await client.notifyConsentChanged(), false);
  assert.equal(await client.recordAddToCart(), false);
  assert.equal(fixture.fetches.length, beforeDestroy);
});

test("withdrawal restores control and removes only owned experiment storage", async () => {
  const localStorage = new MemoryStorage({ unrelated: "preserve" });
  const fixture = makeRoot({ page: "product", consent: true, localStorage });
  const client = createClient(fixture.root, config());
  await client.ready();
  assert.equal(fixture.button.classList.contains("vevo-gb-cta-brand-contrast"), true);

  fixture.root.FloxSettings.options.consent = 0;
  await client.notifyConsentChanged();

  assert.equal(client.getState().reason, "analytics_consent_absent");
  assert.equal(fixture.button.classList.contains("vevo-gb-cta-brand-contrast"), false);
  assert.equal(localStorage.getItem("vevo_exp_device_v1"), null);
  assert.equal(localStorage.getItem("vevo_exp_assignments_v1"), null);
  assert.equal(localStorage.getItem("unrelated"), "preserve");
  const before = fixture.fetches.length;
  assert.equal(await client.recordAddToCart(), false);
  assert.equal(fixture.fetches.length, before);
  client.destroy();
});

test("checkout sends exact transaction id for prior assignments without new SDK evaluation", async () => {
  const device = "11111111-1111-4111-8111-111111111111";
  const firstExposureAt = new Date(Date.now() - 60_000).toISOString();
  const localStorage = new MemoryStorage({
    vevo_exp_device_v1: device,
    vevo_exp_assignments_v1: JSON.stringify({
      "vevo-sk-aa-001": { variationId: "variant", firstExposureAt },
      "vevo-sk-product-cta-color-001": { variationId: "brand_contrast", firstExposureAt },
    }),
  });
  const fixture = makeRoot({
    page: "checkout",
    path: "/e/order-complete",
    consent: true,
    localStorage,
    dataLayer: [{ event: "purchase", ecommerce: { transaction_id: "2602000001", value: 999, email: "forbidden@example.com" } }],
  });
  const client = createClient(fixture.root, config());
  await client.ready();

  const purchases = fixture.fetches.filter((row) => row.body.event_name === "order_completed");
  assert.equal(fixture.sdkInstances(), 0);
  assert.equal(purchases.length, 2);
  assert.ok(purchases.every((row) => row.body.transaction_id === "2602000001"));
  assert.ok(purchases.every((row) => row.body.page_type === "checkout_success"));
  assert.ok(purchases.every((row) => !("value" in row.body) && !("email" in row.body)));
  assert.equal(await client.recordOrderCompleted("different"), false);
  client.destroy();
});

test("invalid variation metadata and SDK failures both preserve control", async () => {
  const invalidKeyFixture = makeRoot({ page: "product", consent: true, invalidTrackingKey: true });
  const invalidKeyClient = createClient(invalidKeyFixture.root, config());
  await invalidKeyClient.ready();
  assert.equal(invalidKeyFixture.button.classList.contains("vevo-gb-cta-brand-contrast"), false);
  assert.equal(
    invalidKeyFixture.fetches.some((row) => row.body.experiment_id === "vevo-sk-product-cta-color-001"),
    false,
  );
  invalidKeyClient.destroy();

  const failureFixture = makeRoot({ page: "product", consent: true, sdkFailure: true });
  const failureClient = createClient(failureFixture.root, config());
  await failureClient.ready();
  assert.equal(failureClient.getState().reason, "sdk_payload_unavailable");
  assert.equal(failureFixture.fetches.length, 0);
  assert.equal(failureFixture.button.classList.contains("vevo-gb-cta-brand-contrast"), false);
  failureClient.destroy();

  const foreignFixture = makeRoot({ page: "product", consent: true, foreignGrowthBook: true });
  const foreignClient = createClient(foreignFixture.root, config());
  await foreignClient.ready();
  assert.equal(foreignClient.getState().reason, "sdk_load_unavailable");
  assert.equal(foreignFixture.fetches.length, 0);
  assert.equal(foreignFixture.button.classList.contains("vevo-gb-cta-brand-contrast"), false);
  foreignClient.destroy();

  const collectorFixture = makeRoot({ page: "product", consent: true, collectorFailure: true });
  const collectorClient = createClient(collectorFixture.root, config());
  await collectorClient.ready();
  assert.equal(await collectorClient.recordAddToCart(), false);
  assert.equal(
    collectorFixture.fetches.some((row) => row.body.event_name === "add_to_cart"),
    false,
  );
  collectorClient.destroy();
});

test("production config is hard-disabled and malformed session dimensions are nulled", async () => {
  const productionFixture = makeRoot({ page: "product", consent: true });
  const productionClient = createClient(productionFixture.root, config({ environment: "production", enableDevMode: false }));
  await productionClient.ready();
  assert.equal(productionClient.getState().reason, "production_not_activated");
  assert.equal(productionFixture.fetches.length, 0);
  productionClient.destroy();

  const sessionStorage = new MemoryStorage({
    vevo_exp_meta_v1: JSON.stringify({
      utm_source: "forbidden@example.com",
      utm_medium: "paid_social",
      meta_campaign_id: "not-digits",
      meta_adset_id: "456",
      meta_ad_id: "789",
      meta_placement: "unknown-placement",
    }),
  });
  const sanitizedFixture = makeRoot({ page: "product", consent: true, sessionStorage });
  const sanitizedClient = createClient(sanitizedFixture.root, config());
  await sanitizedClient.ready();
  const exposure = sanitizedFixture.fetches.find((row) => row.body.event_name === "experiment_exposure").body;
  assert.equal(exposure.utm_source, null);
  assert.equal(exposure.utm_medium, "paid_social");
  assert.equal(exposure.meta_campaign_id, null);
  assert.equal(exposure.meta_adset_id, "456");
  assert.equal(exposure.meta_ad_id, "789");
  assert.equal(exposure.meta_placement, null);
  sanitizedClient.destroy();

  const portFixture = makeRoot({ page: "product", consent: true });
  const portClient = createClient(portFixture.root, config({
    collectorUrl: "https://abc123.execute-api.eu-central-1.amazonaws.com:444/v1/events",
  }));
  await portClient.ready();
  assert.equal(portClient.getState().reason, "invalid_config");
  assert.equal(portFixture.fetches.length, 0);
  portClient.destroy();
});

test("uses the pinned official Web Vitals build and emits numeric metrics only", async () => {
  const fixture = makeRoot({ page: "product", consent: true, withWebVitals: true });
  const client = createClient(fixture.root, config());
  await client.ready();
  await Promise.resolve();

  fixture.vitalHandlers.LCP({ name: "LCP", value: 1234.6, attribution: { target: "#private" } });
  fixture.vitalHandlers.INP({ name: "INP", value: 199.5, entries: [{ name: "private" }] });
  fixture.vitalHandlers.CLS({ name: "CLS", value: 0.1234, id: "private" });
  await Promise.resolve();
  await Promise.resolve();

  const rows = fixture.fetches.filter((row) => row.body.event_name === "performance_vital");
  assert.equal(rows.length, 6);
  for (const row of rows) {
    assert.deepEqual(
      Object.keys(row.body).sort(),
      [
        "consent_state", "device_id", "event_id", "event_name", "experiment_id",
        "meta_ad_id", "meta_adset_id", "meta_campaign_id", "meta_placement", "occurred_at",
        "page_load_id", "page_path", "page_type", "schema_version", "utm_medium", "utm_source",
        "variation_id", "vital_name", "vital_value",
      ].sort(),
    );
  }
  assert.deepEqual(
    Array.from(new Set(rows.map((row) => `${row.body.vital_name}:${row.body.vital_value}`))).sort(),
    ["cls_milli:123", "inp_ms:200", "lcp_ms:1235"],
  );
  assert.equal(JSON.stringify(rows).includes("#private"), false);
  client.destroy();
});

test("loads Web Vitals from the exact SRI-pinned URL only after consent and assignment", async () => {
  const denied = makeRoot({ page: "product", consent: false });
  const deniedClient = createClient(denied.root, config());
  await deniedClient.ready();
  assert.equal(denied.document.head.children.some((row) => row.id === "vevo-web-vitals-v1"), false);
  deniedClient.destroy();

  const granted = makeRoot({ page: "product", consent: true });
  const grantedClient = createClient(granted.root, config());
  await grantedClient.ready();
  await Promise.resolve();
  const script = granted.document.head.children.find((row) => row.id === "vevo-web-vitals-v1");
  assert.ok(script);
  assert.equal(script.src, "https://cdn.jsdelivr.net/npm/web-vitals@6.0.1/dist/web-vitals.iife.js");
  assert.equal(script.integrity, "sha384-xduvx5szsAXW0V0fxOYjfsvz/Zl93SEZcLM+BK+7y6Spco3N+8g8NjbtUIAWCCAQ");
  assert.equal(script.crossOrigin, "anonymous");
  assert.equal(script.referrerPolicy, "no-referrer");
  grantedClient.destroy();
});
