/* VEVO GrowthBook storefront client v1. Preview-only until reviewed activation. */
(function (factory) {
  "use strict";
  if (typeof module === "object" && module.exports) {
    module.exports = factory;
    return;
  }
  if (typeof window !== "undefined") {
    if (
      window.VevoGrowthBook &&
      typeof window.VevoGrowthBook.getState === "function" &&
      window.VevoGrowthBook.getState().clientVersion === "vevo-growthbook-storefront-v1"
    ) {
      return;
    }
    window.VevoGrowthBook = factory(window, window.VEVO_GROWTHBOOK_CONFIG);
  }
})(function createVevoGrowthBook(root, suppliedConfig) {
  "use strict";

  var CLIENT_VERSION = "vevo-growthbook-storefront-v1";
  var PRODUCTION_ACTIVATION = false;
  var SDK_URL = "https://cdn.jsdelivr.net/npm/@growthbook/growthbook@1.7.0/dist/bundles/index.min.js";
  var SDK_INTEGRITY = "sha384-LE9sSbxrM6BIe5z0T5qNuBymAEx7Iwp14FYi2TtCWSalftZaK5cG7ckbe3hNSRPK";
  var SDK_SCRIPT_ID = "vevo-growthbook-sdk-v1";
  var WEB_VITALS_URL = "https://cdn.jsdelivr.net/npm/web-vitals@6.0.1/dist/web-vitals.iife.js";
  var WEB_VITALS_INTEGRITY = "sha384-xduvx5szsAXW0V0fxOYjfsvz/Zl93SEZcLM+BK+7y6Spco3N+8g8NjbtUIAWCCAQ";
  var WEB_VITALS_SCRIPT_ID = "vevo-web-vitals-v1";
  var STYLE_ID = "vevo-growthbook-style-v1";
  var CTA_CLASS = "vevo-gb-cta-brand-contrast";
  var STORAGE = {
    device: "vevo_exp_device_v1",
    assignments: "vevo_exp_assignments_v1",
    meta: "vevo_exp_meta_v1",
    featureCache: "vevo_gb_features_v1",
    stickyPrefix: "vevo_gb_sticky_v1__"
  };
  var EXPERIMENTS = {
    "vevo-sk-aa-001": {
      featureKey: "vevo-sk-aa-assignment",
      variations: ["control", "variant"],
      exposurePages: ["home", "product", "category"],
      healthPages: ["home", "product", "category", "checkout_success"]
    },
    "vevo-sk-product-cta-color-001": {
      featureKey: "vevo-sk-product-cta-color",
      variations: ["control", "brand_contrast"],
      exposurePages: ["product"],
      healthPages: ["product", "checkout_success"]
    }
  };
  var META_SOURCES = ["meta", "facebook", "instagram"];
  var META_MEDIUMS = ["paid_social", "social", "cpc"];
  var META_PLACEMENTS = [
    "audience_network",
    "facebook_feed",
    "facebook_marketplace",
    "facebook_reels",
    "facebook_stories",
    "facebook_video_feeds",
    "instagram_explore",
    "instagram_feed",
    "instagram_profile_feed",
    "instagram_reels",
    "instagram_stories",
    "messenger_inbox",
    "threads_feed"
  ];
  var CONFIG_FIELDS = [
    "schemaVersion",
    "environment",
    "clientKey",
    "apiHost",
    "collectorUrl",
    "allowedHost",
    "gtmContainerId",
    "enableDevMode"
  ];

  var state = {
    status: "inert",
    reason: "not_started",
    consent: false,
    active: false,
    pageType: null,
    variations: {},
    clientVersion: CLIENT_VERSION
  };
  var config = null;
  var sdk = null;
  var deviceId = null;
  var activationGeneration = 0;
  var activationPromise = Promise.resolve(false);
  var consentTimer = null;
  var consentCheckTimer = null;
  var listeners = [];
  var runtimeListeners = [];
  var currentPageAssignments = {};
  var sentOrderEvents = {};
  var sentHealthEvents = {};
  var exposureDeliveries = {};
  var pageLoadId = null;
  var destroyed = false;

  function ownKeys(value) {
    return value && typeof value === "object" && !Array.isArray(value)
      ? Object.keys(value).sort()
      : [];
  }

  function sameArray(left, right) {
    if (left.length !== right.length) return false;
    for (var index = 0; index < left.length; index += 1) {
      if (left[index] !== right[index]) return false;
    }
    return true;
  }

  function validateConfig(candidate) {
    if (!candidate || typeof candidate !== "object" || Array.isArray(candidate)) return null;
    if (!sameArray(ownKeys(candidate), CONFIG_FIELDS.slice().sort())) return null;
    if (candidate.schemaVersion !== 1) return null;
    if (candidate.environment !== "preview" && candidate.environment !== "production") return null;
    if (candidate.environment === "production" && !PRODUCTION_ACTIVATION) return null;
    if (typeof candidate.clientKey !== "string" || !/^sdk-[A-Za-z0-9_-]{8,120}$/.test(candidate.clientKey)) return null;
    if (candidate.apiHost !== "https://cdn.growthbook.io") return null;
    if (candidate.allowedHost !== "www.vevo.sk") return null;
    if (candidate.gtmContainerId !== "GTM-5ZB5LFGB") return null;
    if (typeof candidate.enableDevMode !== "boolean") return null;
    if (candidate.environment === "production" && candidate.enableDevMode) return null;
    try {
      var collector = new root.URL(candidate.collectorUrl);
      if (
        collector.protocol !== "https:" ||
        collector.username ||
        collector.password ||
        collector.port ||
        collector.search ||
        collector.hash ||
        collector.pathname !== "/v1/events"
      ) {
        return null;
      }
      if (
        collector.hostname !== "events-preview.vevo.sk" &&
        !/^[a-z0-9]+\.execute-api\.eu-central-1\.amazonaws\.com$/.test(collector.hostname)
      ) {
        return null;
      }
    } catch (_error) {
      return null;
    }
    return Object.freeze({
      schemaVersion: 1,
      environment: candidate.environment,
      clientKey: candidate.clientKey,
      apiHost: candidate.apiHost,
      collectorUrl: candidate.collectorUrl,
      allowedHost: candidate.allowedHost,
      gtmContainerId: candidate.gtmContainerId,
      enableDevMode: candidate.enableDevMode
    });
  }

  function storageGet(storage, key) {
    try {
      return storage && storage.getItem(key);
    } catch (_error) {
      return null;
    }
  }

  function storageSet(storage, key, value) {
    try {
      if (!storage) return false;
      storage.setItem(key, value);
      return storage.getItem(key) === value;
    } catch (_error) {
      return false;
    }
  }

  function storageRemove(storage, key) {
    try {
      if (storage) storage.removeItem(key);
    } catch (_error) {
      // Fail closed without surfacing storage details.
    }
  }

  function storageKeys(storage) {
    var result = [];
    try {
      if (!storage) return result;
      for (var index = 0; index < storage.length; index += 1) {
        var key = storage.key(index);
        if (typeof key === "string") result.push(key);
      }
    } catch (_error) {
      return [];
    }
    return result;
  }

  function clearOwnedStorage() {
    storageRemove(root.localStorage, STORAGE.device);
    storageRemove(root.localStorage, STORAGE.assignments);
    storageRemove(root.localStorage, STORAGE.featureCache);
    storageRemove(root.sessionStorage, STORAGE.meta);
    storageKeys(root.localStorage).forEach(function (key) {
      if (key.indexOf(STORAGE.stickyPrefix) === 0) storageRemove(root.localStorage, key);
    });
  }

  function uuid4() {
    try {
      if (root.crypto && typeof root.crypto.randomUUID === "function") {
        var nativeUuid = root.crypto.randomUUID().toLowerCase();
        if (/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(nativeUuid)) {
          return nativeUuid;
        }
      }
      if (!root.crypto || typeof root.crypto.getRandomValues !== "function") return null;
      var bytes = new Uint8Array(16);
      root.crypto.getRandomValues(bytes);
      bytes[6] = (bytes[6] & 15) | 64;
      bytes[8] = (bytes[8] & 63) | 128;
      var hex = Array.prototype.map.call(bytes, function (value) {
        return value.toString(16).padStart(2, "0");
      }).join("");
      return [hex.slice(0, 8), hex.slice(8, 12), hex.slice(12, 16), hex.slice(16, 20), hex.slice(20)].join("-");
    } catch (_error) {
      return null;
    }
  }

  function getOrCreateDeviceId() {
    var existing = storageGet(root.localStorage, STORAGE.device);
    if (existing && /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(existing)) {
      return existing;
    }
    var generated = uuid4();
    if (!generated || !storageSet(root.localStorage, STORAGE.device, generated)) return null;
    return generated;
  }

  function hasAnalyticsConsent() {
    try {
      var options = root.FloxSettings && root.FloxSettings.options;
      return !!(
        options &&
        Number.isInteger(options.consent) &&
        Number.isInteger(options.ANALYTIC) &&
        options.ANALYTIC > 0 &&
        (options.consent & options.ANALYTIC)
      );
    } catch (_error) {
      return false;
    }
  }

  function validPath() {
    var path = root.location && root.location.pathname;
    if (typeof path !== "string" || path.length > 200) return null;
    if (!/^\/[A-Za-z0-9/_~!$&'()*+,;=:@.%+-]{0,199}$/.test(path)) return null;
    if (path.indexOf("//") !== -1 || (path + "/").indexOf("/../") !== -1) return null;
    return path;
  }

  function purchaseTransactionFromDataLayer() {
    var layer = root.dataLayer;
    if (!Array.isArray(layer)) return null;
    for (var index = layer.length - 1; index >= 0; index -= 1) {
      var entry = layer[index];
      if (!entry || typeof entry !== "object" || entry.event !== "purchase") continue;
      var ecommerce = entry.ecommerce && typeof entry.ecommerce === "object" ? entry.ecommerce : {};
      var candidate = ecommerce.transaction_id || entry.transactionId || entry.transaction_id;
      candidate = candidate === undefined || candidate === null ? "" : String(candidate).trim();
      if (/^[0-9]{1,20}$/.test(candidate)) return candidate;
    }
    return null;
  }

  function detectPageType() {
    if (purchaseTransactionFromDataLayer()) return "checkout_success";
    try {
      if (root.document.querySelector("#product-detail[data-product-id]")) return "product";
    } catch (_error) {
      return null;
    }
    var path = validPath();
    if (path === "/") return "home";
    if (path && /^\/c(?:\/|$)/.test(path)) return "category";
    return null;
  }

  function isSlovakStorefront() {
    try {
      return !!(
        config &&
        root.location &&
        root.location.protocol === "https:" &&
        root.location.hostname === config.allowedHost &&
        root.document &&
        root.document.documentElement &&
        root.document.documentElement.getAttribute("data-lang-code") === "sk"
      );
    } catch (_error) {
      return false;
    }
  }

  function domReady() {
    if (!root.document || root.document.readyState !== "loading") return Promise.resolve();
    return new Promise(function (resolve) {
      var done = function () {
        root.document.removeEventListener("DOMContentLoaded", done);
        resolve();
      };
      root.document.addEventListener("DOMContentLoaded", done);
    });
  }

  function normalizeAllowed(value, allowed) {
    if (typeof value !== "string") return null;
    var normalized = value.trim().toLowerCase();
    return allowed.indexOf(normalized) !== -1 ? normalized : null;
  }

  function normalizeMetaId(value) {
    if (typeof value !== "string") return null;
    var normalized = value.trim();
    return /^[0-9]{1,30}$/.test(normalized) ? normalized : null;
  }

  function readMetaDimensions() {
    var empty = {
      utm_source: null,
      utm_medium: null,
      meta_campaign_id: null,
      meta_adset_id: null,
      meta_ad_id: null,
      meta_placement: null
    };
    var fromSession = storageGet(root.sessionStorage, STORAGE.meta);
    if (fromSession) {
      try {
        var parsed = JSON.parse(fromSession);
        if (parsed && sameArray(ownKeys(parsed), ownKeys(empty))) {
          empty = {
            utm_source: normalizeAllowed(parsed.utm_source, META_SOURCES),
            utm_medium: normalizeAllowed(parsed.utm_medium, META_MEDIUMS),
            meta_campaign_id: normalizeMetaId(parsed.meta_campaign_id),
            meta_adset_id: normalizeMetaId(parsed.meta_adset_id),
            meta_ad_id: normalizeMetaId(parsed.meta_ad_id),
            meta_placement: normalizeAllowed(parsed.meta_placement, META_PLACEMENTS)
          };
        }
      } catch (_error) {
        storageRemove(root.sessionStorage, STORAGE.meta);
      }
    }
    try {
      var params = new root.URLSearchParams(root.location.search || "");
      var current = {
        utm_source: normalizeAllowed(params.get("utm_source"), META_SOURCES),
        utm_medium: normalizeAllowed(params.get("utm_medium"), META_MEDIUMS),
        meta_campaign_id: normalizeMetaId(params.get("utm_id")),
        meta_adset_id: normalizeMetaId(params.get("meta_adset_id")),
        meta_ad_id: normalizeMetaId(params.get("utm_content")),
        meta_placement: normalizeAllowed(params.get("meta_placement"), META_PLACEMENTS)
      };
      if (current.utm_source || current.meta_campaign_id || current.meta_ad_id) {
        empty = current;
        storageSet(root.sessionStorage, STORAGE.meta, JSON.stringify(empty));
      }
    } catch (_error) {
      // Keep the validated session value or all-null dimensions.
    }
    return empty;
  }

  function baseEvent(eventName, experimentId, variationId, pageType) {
    if (!state.consent || !deviceId || !validPath()) return null;
    var eventId = uuid4();
    if (!eventId) return null;
    var meta = readMetaDimensions();
    return {
      schema_version: 1,
      event_id: eventId,
      event_name: eventName,
      occurred_at: new Date().toISOString(),
      device_id: deviceId,
      page_path: validPath(),
      page_type: pageType,
      consent_state: "analytics_granted",
      experiment_id: experimentId,
      variation_id: variationId,
      utm_source: meta.utm_source,
      utm_medium: meta.utm_medium,
      meta_campaign_id: meta.meta_campaign_id,
      meta_adset_id: meta.meta_adset_id,
      meta_ad_id: meta.meta_ad_id,
      meta_placement: meta.meta_placement
    };
  }

  function delay(milliseconds) {
    return new Promise(function (resolve) {
      root.setTimeout(resolve, milliseconds);
    });
  }

  function postEvent(payload, attempt) {
    if (destroyed || !payload || !state.consent || !config || typeof root.fetch !== "function") return Promise.resolve(false);
    var controller = typeof root.AbortController === "function" ? new root.AbortController() : null;
    var timeout = controller ? root.setTimeout(function () { controller.abort(); }, 3000) : null;
    return root.fetch(config.collectorUrl, {
      method: "POST",
      mode: "cors",
      credentials: "omit",
      cache: "no-store",
      redirect: "error",
      referrerPolicy: "no-referrer",
      keepalive: true,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
      signal: controller ? controller.signal : undefined
    }).then(function (response) {
      if (timeout) root.clearTimeout(timeout);
      if (response && response.ok) return true;
      var retryable = response && (response.status === 409 || response.status === 429 || response.status >= 500);
      if (retryable && attempt < 2 && state.consent && !destroyed) {
        return delay(100 * Math.pow(2, attempt)).then(function () { return postEvent(payload, attempt + 1); });
      }
      return false;
    }).catch(function () {
      if (timeout) root.clearTimeout(timeout);
      if (attempt < 2 && state.consent && !destroyed) {
        return delay(100 * Math.pow(2, attempt)).then(function () { return postEvent(payload, attempt + 1); });
      }
      return false;
    });
  }

  function sendEvent(payload) {
    return postEvent(payload, 0);
  }

  function readAssignments() {
    var raw = storageGet(root.localStorage, STORAGE.assignments);
    if (!raw) return {};
    try {
      var parsed = JSON.parse(raw);
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return {};
      var safe = {};
      Object.keys(EXPERIMENTS).forEach(function (experimentId) {
        var row = parsed[experimentId];
        if (!row || typeof row !== "object") return;
        if (EXPERIMENTS[experimentId].variations.indexOf(row.variationId) === -1) return;
        if (typeof row.firstExposureAt !== "string" || !Number.isFinite(Date.parse(row.firstExposureAt))) return;
        safe[experimentId] = {
          variationId: row.variationId,
          firstExposureAt: row.firstExposureAt
        };
      });
      return safe;
    } catch (_error) {
      return {};
    }
  }

  function rememberAssignment(experimentId, variationId) {
    var assignments = readAssignments();
    if (!assignments[experimentId]) {
      assignments[experimentId] = { variationId: variationId, firstExposureAt: new Date().toISOString() };
    }
    storageSet(root.localStorage, STORAGE.assignments, JSON.stringify(assignments));
    return assignments[experimentId];
  }

  function validVariation(experimentId, variationId) {
    var definition = EXPERIMENTS[experimentId];
    return !!(definition && definition.variations.indexOf(variationId) !== -1);
  }

  function trackingCallback(experiment, result) {
    var experimentId = experiment && experiment.key;
    // GrowthBook's result.key is the variation tracking key and defaults to
    // the numeric index ("0"/"1"). Our data contract is defined by the
    // string feature value, so prefer result.value and fail closed below.
    var variationId = result && typeof result.value === "string"
      ? result.value
      : result && (result.key || result.variationId);
    var definition = EXPERIMENTS[experimentId];
    if (
      !definition ||
      !validVariation(experimentId, variationId) ||
      definition.exposurePages.indexOf(state.pageType) === -1 ||
      !state.consent
    ) {
      return Promise.resolve(false);
    }
    rememberAssignment(experimentId, variationId);
    currentPageAssignments[experimentId] = variationId;
    state.variations[experimentId] = variationId;
    var payload = baseEvent("experiment_exposure", experimentId, variationId, state.pageType);
    var delivery = sendEvent(payload);
    exposureDeliveries[experimentId] = delivery;
    return delivery;
  }

  function sendAfterCurrentPageExposure(experimentId, payload) {
    var delivery = exposureDeliveries[experimentId];
    if (!delivery || !payload) return Promise.resolve(false);
    return delivery.then(function (accepted) {
      if (!accepted || destroyed || !state.active || !state.consent) return false;
      return sendEvent(payload);
    });
  }

  function ensureStyle() {
    if (!root.document || root.document.getElementById(STYLE_ID)) return;
    var style = root.document.createElement("style");
    style.id = STYLE_ID;
    style.textContent =
      "#product-detail .s1-detailCart .s1-submitCart." + CTA_CLASS + "," +
      "#product-detail .s1-detailCart .s1-submitCart." + CTA_CLASS + ":hover{" +
      "background-color:#c9a962!important;" +
      "background-image:linear-gradient(135deg,#c9a962 0%,#b8956f 100%)!important;" +
      "color:#0f172a!important;" +
      "}";
    root.document.head.appendChild(style);
  }

  function ctaButton() {
    try {
      return root.document.querySelector("#product-detail .s1-detailCart .s1-submitCart");
    } catch (_error) {
      return null;
    }
  }

  function applyCtaVariation() {
    var button = ctaButton();
    if (!button || !button.classList) return;
    var variation = currentPageAssignments["vevo-sk-product-cta-color-001"];
    if (variation === "brand_contrast" && state.consent) {
      ensureStyle();
      button.classList.add(CTA_CLASS);
    } else {
      button.classList.remove(CTA_CLASS);
    }
  }

  function restoreControl() {
    var button = ctaButton();
    if (button && button.classList) button.classList.remove(CTA_CLASS);
    try {
      var style = root.document && root.document.getElementById(STYLE_ID);
      if (style && style.parentNode) style.parentNode.removeChild(style);
    } catch (_error) {
      // Control is already the DOM fallback when removal is unavailable.
    }
  }

  function loadExternalLibrary(scriptId, url, integrity, readLibrary) {
    var loaded = readLibrary();
    var present = root.document && root.document.getElementById(scriptId);
    var exactScript = function (script) {
      return !!(script && script.src === url && script.integrity === integrity);
    };
    if (loaded) return Promise.resolve(exactScript(present) ? loaded : null);
    if (present && !exactScript(present)) return Promise.resolve(null);
    return new Promise(function (resolve) {
      if (!root.document || !root.document.head) return resolve(null);
      var finished = false;
      var timeout = null;
      var finish = function (value) {
        if (finished) return;
        finished = true;
        if (timeout) root.clearTimeout(timeout);
        resolve(value);
      };
      var script = present;
      var isNew = !script;
      if (isNew) {
        script = root.document.createElement("script");
        script.id = scriptId;
        script.src = url;
        script.async = true;
        script.integrity = integrity;
        script.crossOrigin = "anonymous";
        script.referrerPolicy = "no-referrer";
      }
      script.addEventListener("load", function () { finish(readLibrary()); }, { once: true });
      script.addEventListener("error", function () { finish(null); }, { once: true });
      timeout = root.setTimeout(function () { finish(null); }, 5000);
      if (isNew) root.document.head.appendChild(script);
    });
  }

  function loadSdk() {
    return loadExternalLibrary(SDK_SCRIPT_ID, SDK_URL, SDK_INTEGRITY, function () {
      return root.growthbook && typeof root.growthbook.GrowthBook === "function"
        ? root.growthbook
        : null;
    });
  }

  function loadWebVitals() {
    return loadExternalLibrary(WEB_VITALS_SCRIPT_ID, WEB_VITALS_URL, WEB_VITALS_INTEGRITY, function () {
      return root.webVitals &&
        typeof root.webVitals.onCLS === "function" &&
        typeof root.webVitals.onINP === "function" &&
        typeof root.webVitals.onLCP === "function"
        ? root.webVitals
        : null;
    });
  }

  function productId() {
    try {
      var detail = root.document.querySelector("#product-detail[data-product-id]");
      var value = detail && detail.getAttribute("data-product-id");
      return typeof value === "string" && /^[A-Za-z0-9._:-]{1,64}$/.test(value) ? value : null;
    } catch (_error) {
      return null;
    }
  }

  function recordAddToCart() {
    if (destroyed) return Promise.resolve(false);
    if (state.status === "loading") {
      return activationPromise.then(function () { return recordAddToCart(); });
    }
    if (!state.active || !state.consent || state.pageType !== "product") return Promise.resolve(false);
    var id = productId();
    if (!id) return Promise.resolve(false);
    var sends = [];
    Object.keys(currentPageAssignments).forEach(function (experimentId) {
      var variationId = currentPageAssignments[experimentId];
      var definition = EXPERIMENTS[experimentId];
      if (!definition || definition.exposurePages.indexOf("product") === -1) return;
      var payload = baseEvent("add_to_cart", experimentId, variationId, "product");
      if (!payload) return;
      payload.product_id = id;
      sends.push(sendAfterCurrentPageExposure(experimentId, payload));
    });
    return Promise.all(sends).then(function (values) { return values.some(Boolean); });
  }

  function recordOrderCompleted(expectedTransactionId) {
    if (destroyed || !hasAnalyticsConsent()) return Promise.resolve(false);
    state.consent = true;
    deviceId = deviceId || getOrCreateDeviceId();
    if (!deviceId) return Promise.resolve(false);
    var transactionId = purchaseTransactionFromDataLayer();
    if (!transactionId) return Promise.resolve(false);
    if (expectedTransactionId !== undefined && String(expectedTransactionId).trim() !== transactionId) {
      return Promise.resolve(false);
    }
    var assignments = readAssignments();
    var now = Date.now();
    var sends = [];
    Object.keys(assignments).forEach(function (experimentId) {
      var assignment = assignments[experimentId];
      var definition = EXPERIMENTS[experimentId];
      if (!definition || definition.healthPages.indexOf("checkout_success") === -1) return;
      var age = now - Date.parse(assignment.firstExposureAt);
      if (age < 0 || age > 7 * 24 * 60 * 60 * 1000) return;
      var unique = experimentId + "|" + transactionId;
      if (sentOrderEvents[unique]) return;
      sentOrderEvents[unique] = true;
      var payload = baseEvent("order_completed", experimentId, assignment.variationId, "checkout_success");
      if (!payload) return;
      payload.transaction_id = transactionId;
      sends.push(sendEvent(payload));
    });
    return Promise.all(sends).then(function (values) { return values.some(Boolean); });
  }

  function sendHealth(eventName, specific) {
    var sends = [];
    Object.keys(currentPageAssignments).forEach(function (experimentId) {
      var definition = EXPERIMENTS[experimentId];
      if (!definition || definition.healthPages.indexOf(state.pageType) === -1) return;
      var unique = experimentId + "|" + eventName + "|" + (specific.vital_name || specific.error_kind || "");
      if (sentHealthEvents[unique]) return;
      sentHealthEvents[unique] = true;
      var payload = baseEvent(eventName, experimentId, currentPageAssignments[experimentId], state.pageType);
      if (!payload) return;
      Object.keys(specific).forEach(function (key) { payload[key] = specific[key]; });
      sends.push(
        state.pageType === "checkout_success"
          ? sendEvent(payload)
          : sendAfterCurrentPageExposure(experimentId, payload)
      );
    });
    return Promise.all(sends);
  }

  function startHealthObservers() {
    if (!Object.keys(currentPageAssignments).length || !state.consent) return;
    pageLoadId = pageLoadId || uuid4();
    if (!pageLoadId) return;
    var measurementGeneration = activationGeneration;
    var measurementPageLoadId = pageLoadId;
    loadWebVitals().then(function (library) {
      if (
        !library ||
        !state.consent ||
        measurementGeneration !== activationGeneration ||
        measurementPageLoadId !== pageLoadId
      ) {
        return;
      }
      var report = function (metric) {
        if (
          !metric ||
          !Number.isFinite(metric.value) ||
          !state.consent ||
          measurementGeneration !== activationGeneration ||
          measurementPageLoadId !== pageLoadId
        ) {
          return;
        }
        var names = { LCP: "lcp_ms", INP: "inp_ms", CLS: "cls_milli" };
        var vitalName = names[metric.name];
        if (!vitalName) return;
        var multiplier = metric.name === "CLS" ? 1000 : 1;
        var value = Math.max(0, Math.round(metric.value * multiplier));
        var maximum = metric.name === "CLS" ? 100000 : 60000;
        if (value > maximum) return;
        sendHealth("performance_vital", {
          page_load_id: measurementPageLoadId,
          vital_name: vitalName,
          vital_value: value
        });
      };
      try { library.onLCP(report); } catch (_error) { /* unsupported vital stays absent */ }
      try { library.onINP(report); } catch (_error) { /* unsupported vital stays absent */ }
      try { library.onCLS(report); } catch (_error) { /* unsupported vital stays absent */ }
    });

    addRuntimeListener(root, "error", function () {
      sendHealth("client_error_observed", { page_load_id: pageLoadId, error_kind: "runtime_error" });
    });
    addRuntimeListener(root, "unhandledrejection", function () {
      sendHealth("client_error_observed", { page_load_id: pageLoadId, error_kind: "unhandled_rejection" });
    });
  }

  function addListener(target, eventName, handler, options) {
    if (!target || typeof target.addEventListener !== "function") return;
    target.addEventListener(eventName, handler, options);
    listeners.push([target, eventName, handler, options]);
  }

  function addRuntimeListener(target, eventName, handler, options) {
    if (!target || typeof target.addEventListener !== "function") return;
    target.addEventListener(eventName, handler, options);
    runtimeListeners.push([target, eventName, handler, options]);
  }

  function stopRuntimeOnly() {
    activationGeneration += 1;
    if (sdk && typeof sdk.destroy === "function") {
      try { sdk.destroy({ destroyAllStreams: true }); } catch (_error) { /* control fallback */ }
    }
    sdk = null;
    runtimeListeners.forEach(function (item) {
      try { item[0].removeEventListener(item[1], item[2], item[3]); } catch (_error) { /* page is unloading */ }
    });
    runtimeListeners = [];
    currentPageAssignments = {};
    exposureDeliveries = {};
    state.variations = {};
    state.active = false;
    restoreControl();
  }

  function withdraw() {
    stopRuntimeOnly();
    clearOwnedStorage();
    deviceId = null;
    sentOrderEvents = {};
    sentHealthEvents = {};
    pageLoadId = null;
    state.status = "control";
    state.reason = "analytics_consent_absent";
  }

  function activate() {
    var generation = ++activationGeneration;
    activationPromise = domReady().then(function () {
      if (generation !== activationGeneration || !hasAnalyticsConsent()) return false;
      state.pageType = detectPageType();
      if (!state.pageType) {
        state.status = "control";
        state.reason = "ineligible_page";
        return false;
      }
      deviceId = getOrCreateDeviceId();
      if (!deviceId) {
        state.status = "control";
        state.reason = "anonymous_storage_unavailable";
        return false;
      }
      readMetaDimensions();
      if (state.pageType === "checkout_success") {
        var checkoutAssignments = readAssignments();
        Object.keys(checkoutAssignments).forEach(function (experimentId) {
          var assignmentAge = Date.now() - Date.parse(checkoutAssignments[experimentId].firstExposureAt);
          if (assignmentAge >= 0 && assignmentAge <= 7 * 24 * 60 * 60 * 1000) {
            currentPageAssignments[experimentId] = checkoutAssignments[experimentId].variationId;
            state.variations[experimentId] = checkoutAssignments[experimentId].variationId;
          }
        });
        state.active = true;
        state.status = "active";
        state.reason = "checkout_reconciliation_only";
        startHealthObservers();
        return recordOrderCompleted().then(function () { return true; });
      }
      return loadSdk().then(function (library) {
        if (generation !== activationGeneration || !state.consent) return false;
        if (!library) {
          stopRuntimeOnly();
          state.status = "control";
          state.reason = "sdk_load_unavailable";
          return false;
        }
        if (typeof library.setPolyfills === "function") {
          library.setPolyfills({
            fetch: function (url, options) {
              return root.fetch(url, Object.assign({}, options || {}, {
                credentials: "omit",
                referrerPolicy: "no-referrer"
              }));
            }
          });
        }
        if (typeof library.configureCache === "function") {
          var previewWithoutFeatureCache = config.environment === "preview";
          library.configureCache({
            cacheKey: STORAGE.featureCache,
            backgroundSync: false,
            maxAge: previewWithoutFeatureCache ? 0 : 4 * 60 * 60 * 1000,
            disableCache: previewWithoutFeatureCache
          });
        }
        var sticky = typeof library.LocalStorageStickyBucketService === "function"
          ? new library.LocalStorageStickyBucketService({ prefix: STORAGE.stickyPrefix, localStorage: root.localStorage })
          : undefined;
        sdk = new library.GrowthBook({
          apiHost: config.apiHost,
          clientKey: config.clientKey,
          attributes: {
            id: deviceId,
            host: config.allowedHost,
            path: validPath(),
            pageType: state.pageType,
            langCode: "sk"
          },
          stickyBucketService: sticky,
          trackingCallback: trackingCallback,
          disableVisualExperiments: true,
          disableJsInjection: true,
          disableUrlRedirectExperiments: true,
          enableDevMode: config.environment === "preview" && config.enableDevMode
        });
        return sdk.init({ timeout: 1500, streaming: false }).then(function (result) {
          if (generation !== activationGeneration || !state.consent || !result || !result.success) {
            stopRuntimeOnly();
            state.status = "control";
            state.reason = "sdk_payload_unavailable";
            return false;
          }
          Object.keys(EXPERIMENTS).forEach(function (experimentId) {
            var definition = EXPERIMENTS[experimentId];
            if (definition.exposurePages.indexOf(state.pageType) === -1) return;
            if (experimentId === "vevo-sk-product-cta-color-001" && !ctaButton()) return;
            sdk.getFeatureValue(definition.featureKey, "control");
          });
          applyCtaVariation();
          state.active = Object.keys(currentPageAssignments).length > 0;
          state.status = state.active ? "active" : "control";
          state.reason = state.active ? "assigned" : "no_active_experiment";
          startHealthObservers();
          return state.active;
        });
      });
    }).catch(function () {
      stopRuntimeOnly();
      state.status = "control";
      state.reason = "activation_error";
      return false;
    });
    return activationPromise;
  }

  function syncConsent() {
    if (destroyed) return Promise.resolve(false);
    var granted = hasAnalyticsConsent();
    var wasGranted = state.consent;
    state.consent = granted;
    if (!granted) {
      if (wasGranted || state.reason !== "analytics_consent_absent") withdraw();
      state.consent = false;
      return Promise.resolve(false);
    }
    if (state.active || state.status === "loading") return activationPromise;
    if (wasGranted && state.status === "control") return activationPromise;
    state.status = "loading";
    state.reason = "activating_after_consent";
    return activate();
  }

  function scheduleConsentCheck() {
    if (consentCheckTimer) root.clearTimeout(consentCheckTimer);
    consentCheckTimer = root.setTimeout(function () {
      consentCheckTimer = null;
      syncConsent();
    }, 0);
  }

  function start() {
    if (destroyed) return Promise.resolve(false);
    config = validateConfig(suppliedConfig);
    if (!config) {
      state.status = "control";
      state.reason = suppliedConfig && suppliedConfig.environment === "production"
        ? "production_not_activated"
        : "invalid_config";
      return Promise.resolve(false);
    }
    if (!isSlovakStorefront()) {
      state.status = "control";
      state.reason = "wrong_storefront";
      return Promise.resolve(false);
    }
    addListener(root.document, "click", scheduleConsentCheck, true);
    addListener(root.document, "change", scheduleConsentCheck, true);
    addListener(root, "storage", scheduleConsentCheck);
    consentTimer = root.setInterval(syncConsent, 5000);
    return syncConsent();
  }

  function destroy() {
    destroyed = true;
    stopRuntimeOnly();
    if (consentTimer) root.clearInterval(consentTimer);
    if (consentCheckTimer) root.clearTimeout(consentCheckTimer);
    consentTimer = null;
    consentCheckTimer = null;
    listeners.forEach(function (item) {
      try { item[0].removeEventListener(item[1], item[2], item[3]); } catch (_error) { /* page is unloading */ }
    });
    listeners = [];
    state.consent = false;
    state.status = "control";
    state.reason = "destroyed";
    return true;
  }

  function getState() {
    return {
      status: state.status,
      reason: state.reason,
      consent: state.consent,
      active: state.active,
      pageType: state.pageType,
      variations: Object.assign({}, state.variations),
      clientVersion: state.clientVersion
    };
  }

  var api = {
    ready: function () { return activationPromise; },
    notifyConsentChanged: syncConsent,
    recordAddToCart: recordAddToCart,
    recordOrderCompleted: recordOrderCompleted,
    getState: getState,
    destroy: destroy
  };
  start();
  return api;
});
