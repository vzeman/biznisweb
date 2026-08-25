WITH eligible_ranked AS (
  SELECT
    device_id,
    variation_id,
    first_exposure_at,
    add_to_cart_24h,
    purchase_converted,
    joined_order_count,
    net_revenue_eur,
    cm1_eur,
    cancelled_order_count,
    refunded_order_count,
    immature_order_count,
    client_error_observed,
    unmatched_transaction_count,
    ambiguous_transaction_count,
    ROW_NUMBER() OVER (
      ORDER BY from_iso8601_timestamp(first_exposure_at), device_id
    ) AS sample_ordinal
  FROM experiment_device_facts
  WHERE experiment_id = 'vevo-sk-product-cta-color-001'
    AND metric_contract_version = 'vevo_cm1_v1_2026-08-20'
    AND eligible = 1
    AND contaminated = 0
    AND variation_id IN ('control', 'brand_contrast')
    AND from_iso8601_timestamp(first_exposure_at) >= from_iso8601_timestamp('__CTA_STARTED_AT_UTC__')
    AND from_iso8601_timestamp(first_exposure_at) < from_iso8601_timestamp('__CTA_ENDED_AT_UTC__')
),
decision_cohort AS (
  SELECT *
  FROM eligible_ranked
  WHERE sample_ordinal <= __TARGET_TOTAL_SAMPLE__
),
variation_outcomes AS (
  SELECT
    variation_id,
    COUNT(*) AS eligible_devices,
    SUM(add_to_cart_24h) AS add_to_cart_devices,
    SUM(purchase_converted) AS purchase_devices,
    SUM(joined_order_count) AS joined_order_count,
    SUM(net_revenue_eur) AS net_revenue_sum_eur,
    SUM(net_revenue_eur * net_revenue_eur) AS net_revenue_sum_squares_eur2,
    SUM(cm1_eur) AS cm1_sum_eur,
    SUM(cm1_eur * cm1_eur) AS cm1_sum_squares_eur2,
    SUM(cancelled_order_count) AS cancelled_order_count,
    SUM(refunded_order_count) AS refunded_order_count,
    SUM(immature_order_count) AS immature_order_count,
    SUM(client_error_observed) AS client_error_devices,
    SUM(unmatched_transaction_count) AS unmatched_transaction_count,
    SUM(ambiguous_transaction_count) AS ambiguous_transaction_count
  FROM decision_cohort
  GROUP BY variation_id
),
performance AS (
  SELECT
    cohort.variation_id,
    COUNT(DISTINCT performance.page_load_id) AS measured_page_loads,
    approx_percentile(
      CASE WHEN performance.vital_name = 'lcp_ms' THEN performance.vital_value END,
      0.75
    ) AS lcp_p75_ms,
    approx_percentile(
      CASE WHEN performance.vital_name = 'inp_ms' THEN performance.vital_value END,
      0.75
    ) AS inp_p75_ms,
    approx_percentile(
      CASE WHEN performance.vital_name = 'cls_milli' THEN performance.vital_value END,
      0.75
    ) AS cls_p75_milli
  FROM decision_cohort AS cohort
  INNER JOIN experiment_performance_facts AS performance
    ON performance.device_id = cohort.device_id
    AND performance.variation_id = cohort.variation_id
  WHERE performance.experiment_id = 'vevo-sk-product-cta-color-001'
    AND performance.metric_contract_version = 'vevo_cm1_v1_2026-08-20'
    AND performance.eligible = 1
  GROUP BY cohort.variation_id
),
raw_window AS (
  SELECT *
  FROM experiment_events_raw
  WHERE event_date BETWEEN '__CTA_START_DATE__' AND '__FOLLOWUP_LAST_DATE__'
    AND experiment_id = 'vevo-sk-product-cta-color-001'
    AND from_iso8601_timestamp(received_at) >= from_iso8601_timestamp('__CTA_STARTED_AT_UTC__')
    AND from_iso8601_timestamp(received_at) < from_iso8601_timestamp('__FOLLOWUP_THROUGH_UTC__')
),
privacy_sample AS (
  SELECT *
  FROM raw_window
  ORDER BY received_at, event_id
  LIMIT 100
),
quality AS (
  SELECT
    (SELECT COUNT(*) FROM decision_cohort) AS reporting_device_count,
    (
      SELECT COUNT(DISTINCT device_id)
      FROM experiment_device_facts
      WHERE experiment_id = 'vevo-sk-product-cta-color-001'
        AND metric_contract_version = 'vevo_cm1_v1_2026-08-20'
        AND eligible = 1
        AND contaminated = 0
        AND variation_id IN ('control', 'brand_contrast')
        AND from_iso8601_timestamp(first_exposure_at) >= from_iso8601_timestamp('__CTA_STARTED_AT_UTC__')
        AND from_iso8601_timestamp(first_exposure_at) < from_iso8601_timestamp('__CTA_ENDED_AT_UTC__')
    ) AS eligible_devices_seen_before_stop,
    (SELECT COUNT(*) FROM raw_window) AS raw_event_count,
    (SELECT COUNT(DISTINCT event_id) FROM raw_window) AS unique_event_count,
    (
      SELECT COUNT(DISTINCT device_id)
      FROM experiment_device_facts
      WHERE experiment_id = 'vevo-sk-product-cta-color-001'
        AND metric_contract_version = 'vevo_cm1_v1_2026-08-20'
        AND contaminated = 1
        AND from_iso8601_timestamp(first_exposure_at) >= from_iso8601_timestamp('__CTA_STARTED_AT_UTC__')
        AND from_iso8601_timestamp(first_exposure_at) < from_iso8601_timestamp('__CTA_ENDED_AT_UTC__')
    ) AS contaminated_device_count,
    (
      SELECT COUNT_IF(
        regexp_like(
          lower(concat_ws('|', coalesce(page_path, ''), coalesce(utm_source, ''), coalesce(utm_medium, ''), coalesce(product_id, ''), coalesce(error_kind, ''))),
          '[a-z0-9._%+-]+@[a-z0-9.-]+[.][a-z]{2,}|(?:[+]421|00421)[ 0-9-]{8,16}'
        )
      )
      FROM privacy_sample
    ) AS pii_finding_count,
    (
      SELECT COUNT_IF(regexp_like(coalesce(page_path, ''), '(?i)^(?:https?://)|[?#]'))
      FROM privacy_sample
    ) AS full_url_stored_count,
    (
      SELECT COUNT_IF(
        regexp_like(
          lower(concat_ws('|', coalesce(page_path, ''), coalesce(utm_source, ''), coalesce(utm_medium, ''))),
          'fbclid|_fbp|_fbc'
        )
      )
      FROM privacy_sample
    ) AS click_identifier_stored_count,
    (
      SELECT COUNT_IF(event_name = 'experiment_exposure' AND consent_state <> 'analytics_granted')
      FROM raw_window
    ) AS non_analytical_consent_exposure_count
)
SELECT
  outcomes.variation_id,
  outcomes.eligible_devices,
  outcomes.add_to_cart_devices,
  outcomes.purchase_devices,
  outcomes.joined_order_count,
  outcomes.net_revenue_sum_eur,
  outcomes.net_revenue_sum_squares_eur2,
  outcomes.cm1_sum_eur,
  outcomes.cm1_sum_squares_eur2,
  outcomes.cancelled_order_count,
  outcomes.refunded_order_count,
  outcomes.immature_order_count,
  outcomes.client_error_devices,
  outcomes.unmatched_transaction_count,
  outcomes.ambiguous_transaction_count,
  COALESCE(performance.measured_page_loads, 0) AS measured_page_loads,
  performance.lcp_p75_ms,
  performance.inp_p75_ms,
  performance.cls_p75_milli,
  quality.reporting_device_count,
  quality.eligible_devices_seen_before_stop,
  quality.raw_event_count,
  quality.unique_event_count,
  quality.contaminated_device_count,
  quality.pii_finding_count,
  quality.full_url_stored_count,
  quality.click_identifier_stored_count,
  quality.non_analytical_consent_exposure_count
FROM variation_outcomes AS outcomes
LEFT JOIN performance
  ON performance.variation_id = outcomes.variation_id
CROSS JOIN quality
ORDER BY outcomes.variation_id
