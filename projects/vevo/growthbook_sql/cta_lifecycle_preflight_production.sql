WITH frozen_aa_cohort AS (
  SELECT
    joined_order_count,
    immature_order_count,
    cm1_eur,
    cancelled_order_count,
    refunded_order_count,
    facts_generated_at
  FROM experiment_device_facts
  WHERE experiment_id = 'vevo-sk-aa-001'
    AND metric_contract_version = 'vevo_cm1_v1_2026-08-20'
    AND eligible = 1
    AND contaminated = 0
    AND from_iso8601_timestamp(first_exposure_at) >= from_iso8601_timestamp('__SOURCE_FROM_UTC__')
    AND from_iso8601_timestamp(first_exposure_at) < from_iso8601_timestamp('__SOURCE_THROUGH_UTC__')
)
SELECT
  COUNT(*) AS eligible_device_count,
  COALESCE(SUM(joined_order_count), 0) AS joined_order_count,
  COALESCE(SUM(joined_order_count - immature_order_count), 0) AS mature_joined_order_count,
  COALESCE(SUM(immature_order_count), 0) AS immature_order_count,
  CAST(ROUND(COALESCE(SUM(cm1_eur), 0), 2) AS DECIMAL(18, 2)) AS cm1_sum_eur,
  COALESCE(SUM(cancelled_order_count), 0) AS cancelled_order_count,
  COALESCE(SUM(refunded_order_count), 0) AS refunded_or_creditnoted_order_count,
  COUNT(DISTINCT facts_generated_at) AS facts_generation_count,
  MAX(facts_generated_at) AS facts_generated_at
FROM frozen_aa_cohort
