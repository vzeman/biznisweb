WITH eligible_aa_devices AS (
  SELECT DISTINCT device_id, variation_id
  FROM experiment_device_facts
  WHERE experiment_id = 'vevo-sk-aa-001'
    AND metric_contract_version = 'vevo_cm1_v1_2026-08-20'
    AND eligible = 1
    AND contaminated = 0
    AND from_iso8601_timestamp(first_exposure_at) >= from_iso8601_timestamp('__AA_FROM_UTC__')
    AND from_iso8601_timestamp(first_exposure_at) < from_iso8601_timestamp('__AA_THROUGH_UTC__')
),
first_product_exposure AS (
  SELECT
    raw.device_id,
    eligible.variation_id,
    MIN(from_iso8601_timestamp(raw.received_at)) AS first_product_exposure_at
  FROM experiment_events_raw AS raw
  INNER JOIN eligible_aa_devices AS eligible
    ON raw.device_id = eligible.device_id
    AND raw.variation_id = eligible.variation_id
  WHERE raw.event_date BETWEEN '__AA_FROM_DATE__' AND '__AA_LAST_EXPOSURE_DATE__'
    AND raw.experiment_id = 'vevo-sk-aa-001'
    AND raw.event_name = 'experiment_exposure'
    AND raw.page_type = 'product'
    AND raw.consent_state = 'analytics_granted'
    AND raw.risk_result = 'accepted'
    AND from_iso8601_timestamp(raw.received_at) >= from_iso8601_timestamp('__AA_FROM_UTC__')
    AND from_iso8601_timestamp(raw.received_at) < from_iso8601_timestamp('__AA_THROUGH_UTC__')
  GROUP BY raw.device_id, eligible.variation_id
),
converted_devices AS (
  SELECT DISTINCT exposure.device_id
  FROM first_product_exposure AS exposure
  INNER JOIN experiment_events_raw AS cart
    ON cart.device_id = exposure.device_id
    AND cart.variation_id = exposure.variation_id
  WHERE cart.event_date BETWEEN '__AA_FROM_DATE__' AND '__FOLLOWUP_LAST_DATE__'
    AND cart.experiment_id = 'vevo-sk-aa-001'
    AND cart.event_name = 'add_to_cart'
    AND cart.page_type = 'product'
    AND cart.consent_state = 'analytics_granted'
    AND cart.risk_result = 'accepted'
    AND from_iso8601_timestamp(cart.received_at) >= exposure.first_product_exposure_at
    AND from_iso8601_timestamp(cart.received_at) <= date_add(
      'hour',
      24,
      exposure.first_product_exposure_at
    )
    AND from_iso8601_timestamp(cart.received_at) < from_iso8601_timestamp('__FOLLOWUP_THROUGH_UTC__')
)
SELECT
  COUNT(*) AS exposed_devices,
  COUNT(converted.device_id) AS converted_devices
FROM first_product_exposure AS exposure
LEFT JOIN converted_devices AS converted
  ON exposure.device_id = converted.device_id
