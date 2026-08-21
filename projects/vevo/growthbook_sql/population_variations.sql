SELECT
  experiment_id,
  variation_id,
  COUNT(*) AS eligible_devices,
  COUNT_IF(meta_campaign_id IS NOT NULL) AS meta_campaign_devices,
  COUNT_IF(meta_adset_id IS NOT NULL) AS meta_adset_devices,
  COUNT_IF(meta_ad_id IS NOT NULL) AS meta_ad_devices,
  COUNT_IF(meta_placement IS NOT NULL) AS meta_placement_devices
FROM experiment_device_facts
WHERE metric_contract_version = 'vevo_cm1_v1_2026-08-20'
  AND eligible = 1
  AND contaminated = 0
GROUP BY experiment_id, variation_id
ORDER BY experiment_id, variation_id
