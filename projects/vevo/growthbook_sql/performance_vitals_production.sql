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
UNION ALL
SELECT
  CAST(current_timestamp AS timestamp) AS timestamp,
  '00000000-0000-4000-8000-000000000000' AS device_id,
  '__growthbook_schema_only__' AS experiment_id,
  'control' AS variation_id,
  '00000000-0000-4000-8000-000000000000' AS page_load_id,
  'lcp_ms' AS vital_name,
  CAST(NULL AS bigint) AS vital_value
FROM (VALUES (1)) AS schema_seed(x)
WHERE '{{ experimentId }}' = '%'
