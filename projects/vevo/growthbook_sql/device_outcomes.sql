SELECT
  CAST(from_iso8601_timestamp(first_exposure_at) AS timestamp) AS timestamp,
  device_id,
  experiment_id,
  variation_id,
  meta_campaign_id,
  meta_adset_id,
  meta_ad_id,
  meta_placement,
  add_to_cart_24h,
  purchase_converted,
  joined_order_count,
  net_revenue_eur,
  cm1_eur,
  cancelled_order_count,
  refunded_order_count,
  immature_order_count,
  CASE WHEN immature_order_count = 0 THEN 1 ELSE 0 END AS lifecycle_mature,
  client_error_observed,
  order_attribution_eligible,
  unmatched_transaction_count,
  ambiguous_transaction_count
FROM experiment_device_facts
WHERE metric_contract_version = 'vevo_cm1_v1_2026-08-20'
  AND eligible = 1
  AND contaminated = 0
  AND experiment_id LIKE '{{ experimentId }}'
  AND from_iso8601_timestamp(first_exposure_at) BETWEEN
    from_iso8601_timestamp('{{startDateISO}}')
    AND from_iso8601_timestamp('{{endDateISO}}')
