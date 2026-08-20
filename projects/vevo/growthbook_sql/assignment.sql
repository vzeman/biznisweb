SELECT
  device_id,
  CAST(from_iso8601_timestamp(first_exposure_at) AS timestamp) AS timestamp,
  experiment_id,
  variation_id,
  meta_campaign_id,
  meta_adset_id,
  meta_ad_id,
  meta_placement
FROM experiment_device_facts
WHERE metric_contract_version = 'vevo_cm1_v1_2026-08-20'
  AND eligible = 1
  AND contaminated = 0
  AND from_iso8601_timestamp(first_exposure_at) BETWEEN
    from_iso8601_timestamp('{{startDateISO}}')
    AND from_iso8601_timestamp('{{endDateISO}}')
