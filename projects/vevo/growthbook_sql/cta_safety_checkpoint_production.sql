WITH eligible_rows AS (
  SELECT
    device_id,
    variation_id,
    client_error_observed
  FROM experiment_device_facts
  WHERE experiment_id = 'vevo-sk-product-cta-color-001'
    AND metric_contract_version = 'vevo_cm1_v1_2026-08-20'
    AND eligible = 1
    AND contaminated = 0
    AND variation_id IN ('control', 'brand_contrast')
    AND from_iso8601_timestamp(first_exposure_at) >= from_iso8601_timestamp('__CTA_STARTED_AT_UTC__')
    AND from_iso8601_timestamp(first_exposure_at) < from_iso8601_timestamp('__CHECKPOINT_THROUGH_UTC__')
),
variation_health AS (
  SELECT
    variation_id,
    COUNT(DISTINCT device_id) AS eligible_devices,
    SUM(client_error_observed) AS client_error_devices
  FROM eligible_rows
  GROUP BY variation_id
),
performance AS (
  SELECT
    eligible.variation_id,
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
  FROM eligible_rows AS eligible
  INNER JOIN experiment_performance_facts AS performance
    ON performance.device_id = eligible.device_id
    AND performance.variation_id = eligible.variation_id
  WHERE performance.experiment_id = 'vevo-sk-product-cta-color-001'
    AND performance.metric_contract_version = 'vevo_cm1_v1_2026-08-20'
    AND performance.eligible = 1
    AND from_iso8601_timestamp(performance.measured_at) >= from_iso8601_timestamp('__CTA_STARTED_AT_UTC__')
    AND from_iso8601_timestamp(performance.measured_at) < from_iso8601_timestamp('__CHECKPOINT_THROUGH_UTC__')
  GROUP BY eligible.variation_id
),
assignment_quality AS (
  SELECT
    SUM(assignment_count - 1) AS duplicate_device_fact_rows,
    COUNT(
      DISTINCT CASE
        WHEN assignment_count <> 1 OR variation_count <> 1 THEN device_id
      END
    ) AS conflicting_assignment_devices
  FROM (
    SELECT
      device_id,
      COUNT(*) AS assignment_count,
      COUNT(DISTINCT variation_id) AS variation_count
    FROM eligible_rows
    GROUP BY device_id
  )
)
SELECT
  health.variation_id,
  health.eligible_devices,
  COALESCE(performance.measured_page_loads, 0) AS measured_page_loads,
  health.client_error_devices,
  performance.lcp_p75_ms,
  performance.inp_p75_ms,
  performance.cls_p75_milli,
  quality.duplicate_device_fact_rows,
  quality.conflicting_assignment_devices
FROM variation_health AS health
LEFT JOIN performance
  ON performance.variation_id = health.variation_id
CROSS JOIN assignment_quality AS quality
ORDER BY health.variation_id
