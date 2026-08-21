WITH assignments AS (
  SELECT
    experiment_id,
    device_id,
    variation_id,
    meta_campaign_id,
    meta_adset_id,
    meta_ad_id,
    meta_placement
  FROM experiment_device_facts
  WHERE metric_contract_version = 'vevo_cm1_v1_2026-08-20'
    AND eligible = 1
    AND contaminated = 0
),
outcomes AS (
  SELECT
    experiment_id,
    device_id,
    variation_id
  FROM experiment_device_facts
  WHERE metric_contract_version = 'vevo_cm1_v1_2026-08-20'
    AND eligible = 1
    AND contaminated = 0
),
assignment_keys AS (
  SELECT experiment_id, device_id, COUNT(*) AS row_count
  FROM assignments
  GROUP BY experiment_id, device_id
),
outcome_keys AS (
  SELECT experiment_id, device_id, COUNT(*) AS row_count
  FROM outcomes
  GROUP BY experiment_id, device_id
)
SELECT
  (SELECT COUNT(*) FROM assignments) AS assignment_rows,
  (SELECT COUNT(*) FROM outcomes) AS outcome_rows,
  (SELECT COUNT(*) FROM assignment_keys) AS assignment_keys,
  (SELECT COUNT(*) FROM outcome_keys) AS outcome_keys,
  (SELECT COUNT(*) FROM assignment_keys WHERE row_count <> 1) AS duplicate_assignment_keys,
  (SELECT COUNT(*) FROM outcome_keys WHERE row_count <> 1) AS duplicate_outcome_keys,
  (
    SELECT COUNT(*) FROM assignment_keys a
    LEFT JOIN outcome_keys o
      ON a.experiment_id = o.experiment_id AND a.device_id = o.device_id
    WHERE o.device_id IS NULL
  ) AS assignments_missing_outcomes,
  (
    SELECT COUNT(*) FROM outcome_keys o
    LEFT JOIN assignment_keys a
      ON a.experiment_id = o.experiment_id AND a.device_id = o.device_id
    WHERE a.device_id IS NULL
  ) AS outcomes_missing_assignments,
  (SELECT COUNT_IF(meta_campaign_id IS NOT NULL) FROM assignments) AS meta_campaign_rows,
  (SELECT COUNT_IF(meta_adset_id IS NOT NULL) FROM assignments) AS meta_adset_rows,
  (SELECT COUNT_IF(meta_ad_id IS NOT NULL) FROM assignments) AS meta_ad_rows,
  (SELECT COUNT_IF(meta_placement IS NOT NULL) FROM assignments) AS meta_placement_rows,
  (
    SELECT COUNT_IF(
      meta_campaign_id IS NOT NULL
      AND meta_adset_id IS NOT NULL
      AND meta_ad_id IS NOT NULL
      AND meta_placement IS NOT NULL
    ) FROM assignments
  ) AS complete_meta_dimension_rows,
  (
    SELECT COUNT_IF(
      (meta_campaign_id IS NOT NULL AND NOT regexp_like(meta_campaign_id, '^[0-9]{1,30}$'))
      OR (meta_adset_id IS NOT NULL AND NOT regexp_like(meta_adset_id, '^[0-9]{1,30}$'))
      OR (meta_ad_id IS NOT NULL AND NOT regexp_like(meta_ad_id, '^[0-9]{1,30}$'))
      OR (
        meta_placement IS NOT NULL
        AND meta_placement NOT IN (
          'audience_network', 'facebook_feed', 'facebook_marketplace', 'facebook_reels',
          'facebook_stories', 'facebook_video_feeds', 'instagram_explore', 'instagram_feed',
          'instagram_profile_feed', 'instagram_reels', 'instagram_stories', 'messenger_inbox',
          'threads_feed'
        )
      )
    ) FROM assignments
  ) AS invalid_meta_dimension_rows
