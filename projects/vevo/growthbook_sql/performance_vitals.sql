SELECT
  CAST(from_iso8601_timestamp(measured_at) AS timestamp) AS timestamp,
  device_id,
  experiment_id,
  variation_id,
  page_load_id,
  vital_name,
  vital_value
FROM experiment_performance_facts
WHERE metric_contract_version = 'vevo_cm1_v1_2026-08-20'
  AND eligible = 1
  AND experiment_id LIKE '{{ experimentId }}'
  AND from_iso8601_timestamp(measured_at) BETWEEN
    from_iso8601_timestamp('{{startDateISO}}')
    AND from_iso8601_timestamp('{{endDateISO}}')
