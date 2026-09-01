CREATE OR REPLACE VIEW facebook_data.vw_facebook_page_metrics AS
SELECT
  -- Dimensions
  account_id,
  page_id,
  page_name,
  DATE(date) AS date,

  -- Metrics (cast to FLOAT64; convert invalid values to 0.0)
  IFNULL(SAFE_CAST(followers_count AS FLOAT64), 0.0) AS followers_count,
  IFNULL(SAFE_CAST(page_impressions AS FLOAT64), 0.0) AS page_impressions,
  IFNULL(SAFE_CAST(page_impressions_unique AS FLOAT64), 0.0) AS page_impressions_unique,
  IFNULL(SAFE_CAST(page_impressions_paid AS FLOAT64), 0.0) AS page_impressions_paid,
  IFNULL(SAFE_CAST(page_impressions_paid_unique AS FLOAT64), 0.0) AS page_impressions_paid_unique,
  IFNULL(SAFE_CAST(page_impressions_viral AS FLOAT64), 0.0) AS page_impressions_viral,
  IFNULL(SAFE_CAST(page_impressions_viral_unique AS FLOAT64), 0.0) AS page_impressions_viral_unique,
  IFNULL(SAFE_CAST(page_posts_impressions AS FLOAT64), 0.0) AS page_posts_impressions,
  IFNULL(SAFE_CAST(page_posts_impressions_unique AS FLOAT64), 0.0) AS page_posts_impressions_unique,
  IFNULL(SAFE_CAST(page_posts_impressions_paid AS FLOAT64), 0.0) AS page_posts_impressions_paid,
  IFNULL(SAFE_CAST(page_posts_impressions_paid_unique AS FLOAT64), 0.0) AS page_posts_impressions_paid_unique,
  IFNULL(SAFE_CAST(page_posts_impressions_organic AS FLOAT64), 0.0) AS page_posts_impressions_organic,
  IFNULL(SAFE_CAST(page_posts_impressions_organic_unique AS FLOAT64), 0.0) AS IFNULL(SAFE_CAST(page_posts_impressions_viral AS FLOAT64), 0.0) AS page_posts_impressions_viral,
  IFNULL(SAFE_CAST(page_posts_impressions_viral_unique AS FLOAT64), 0.0) AS page_posts_impressions_viral_unique,
  IFNULL(SAFE_CAST(page_post_engagements AS FLOAT64), 0.0) AS page_post_engagements,
  IFNULL(SAFE_CAST(page_fan_adds AS FLOAT64), 0.0) AS page_fan_adds,
  IFNULL(SAFE_CAST(page_fan_adds_unique AS FLOAT64), 0.0) AS page_fan_adds_unique,
  IFNULL(SAFE_CAST(page_fan_removes AS FLOAT64), 0.0) AS page_fan_removes,
  IFNULL(SAFE_CAST(page_fan_removes_unique AS FLOAT64), 0.0) AS page_fan_removes_unique,
  IFNULL(SAFE_CAST(page_daily_follows AS FLOAT64), 0.0) AS page_daily_follows,
  IFNULL(SAFE_CAST(page_daily_follows_unique AS FLOAT64), 0.0) AS page_daily_follows_unique,
  IFNULL(SAFE_CAST(page_daily_unfollows AS FLOAT64), 0.0) AS page_daily_unfollows,
  IFNULL(SAFE_CAST(page_daily_unfollows_unique AS FLOAT64), 0.0) AS page_daily_unfollows_unique,
  IFNULL(SAFE_CAST(page_actions_post_reactions_like_total AS FLOAT64), 0.0) AS page_actions_post_reactions_like_total,
  IFNULL(SAFE_CAST(page_actions_post_reactions_love_total AS FLOAT64), 0.0) AS page_actions_post_reactions_love_total,
  IFNULL(SAFE_CAST(page_actions_post_reactions_wow_total AS FLOAT64), 0.0) AS page_actions_post_reactions_wow_total,
  IFNULL(SAFE_CAST(page_actions_post_reactions_haha_total AS FLOAT64), 0.0) AS page_actions_post_reactions_haha_total,
  IFNULL(SAFE_CAST(page_actions_post_reactions_sorry_total AS FLOAT64), 0.0) AS page_actions_post_reactions_sorry_total,
  IFNULL(SAFE_CAST(page_actions_post_reactions_anger_total AS FLOAT64), 0.0) AS page_actions_post_reactions_anger_total,
  IFNULL(SAFE_CAST(page_actions_post_reactions_total AS FLOAT64), 0.0) AS page_actions_post_reactions_total,
  IFNULL(SAFE_CAST(page_views_total AS FLOAT64), 0.0) AS page_views_total,
  IFNULL(SAFE_CAST(page_video_views AS FLOAT64), 0.0) AS page_video_views,
  IFNULL(SAFE_CAST(page_video_views_paid AS FLOAT64), 0.0) AS page_video_views_paid,
  IFNULL(SAFE_CAST(page_video_views_organic AS FLOAT64), 0.0) AS page_video_views_organic,
  IFNULL(SAFE_CAST(page_video_complete_views_30s AS FLOAT64), 0.0) AS page_video_complete_views_30s,
  IFNULL(SAFE_CAST(page_total_actions AS FLOAT64), 0.0) AS page_total_actions,
  IFNULL(SAFE_CAST(page_fans AS FLOAT64), 0.0) AS page_fans,

  -- Derived validation columns for validation of a couple of columns - may not be necessary 
  (
    IFNULL(SAFE_CAST(page_actions_post_reactions_like_total AS FLOAT64), 0.0) +
    IFNULL(SAFE_CAST(page_actions_post_reactions_love_total AS FLOAT64), 0.0) +
    IFNULL(SAFE_CAST(page_actions_post_reactions_wow_total AS FLOAT64), 0.0) +
    IFNULL(SAFE_CAST(page_actions_post_reactions_haha_total AS FLOAT64), 0.0) +
    IFNULL(SAFE_CAST(page_actions_post_reactions_sorry_total AS FLOAT64), 0.0) +
    IFNULL(SAFE_CAST(page_actions_post_reactions_anger_total AS FLOAT64), 0.0)
  ) AS check_page_action_total,

  (
    IFNULL(SAFE_CAST(page_post_engagements AS FLOAT64), 0.0) +
    IFNULL(SAFE_CAST(page_total_actions AS FLOAT64), 0.0) +
    IFNULL(SAFE_CAST(page_views_total AS FLOAT64), 0.0) +
    IFNULL(SAFE_CAST(page_video_views AS FLOAT64), 0.0) +
    IFNULL(SAFE_CAST(page_fan_adds AS FLOAT64), 0.0) -
    IFNULL(SAFE_CAST(page_fan_removes AS FLOAT64), 0.0)
  ) AS check_all_actions

FROM
  facebook_data.facebook_page_insights;


CREATE OR REPLACE VIEW facebook_data.vw_facebook_post_metrics AS
SELECT
  -- Dimensions
  account_id,
  page_id,
  post_id,
  page_name,
  message,
  TIMESTAMP(created_time) AS created_time,
  DATE(TIMESTAMP(created_time)) AS date,

  -- Derived columns
  DATETIME(TIMESTAMP(created_time), "America/Chicago") AS created_time_cst,
  FORMAT_DATETIME("%H:%M:%S", DATETIME(TIMESTAMP(created_time), "America/Chicago")) AS
created_time_hour_cst,
  DATE(DATETIME(TIMESTAMP(created_time), "America/Chicago")) AS created_time_date_cst,
  REGEXP_EXTRACT(message, r'https?://\S+') AS message_link,
  

  -- Metrics (stored as FLOAT64 for accuracy)
  IFNULL(SAFE_CAST(comments_count AS FLOAT64), 0.0) AS comments_count,
  IFNULL(SAFE_CAST(shares_count AS FLOAT64), 0.0) AS shares_count,
  IFNULL(SAFE_CAST(post_impressions AS FLOAT64), 0.0) AS post_impressions,
  IFNULL(SAFE_CAST(post_impressions_unique AS FLOAT64), 0.0) AS post_impressions_unique,
  IFNULL(SAFE_CAST(post_impressions_paid AS FLOAT64), 0.0) AS post_impressions_paid,
  IFNULL(SAFE_CAST(post_impressions_paid_unique AS FLOAT64), 0.0) AS post_impressions_paid_unique,
  IFNULL(SAFE_CAST(post_impressions_fan AS FLOAT64), 0.0) AS post_impressions_fan,
  IFNULL(SAFE_CAST(post_impressions_fan_unique AS FLOAT64), 0.0) AS post_impressions_fan_unique,
  IFNULL(SAFE_CAST(post_impressions_organic AS FLOAT64), 0.0) AS post_impressions_organic,
  IFNULL(SAFE_CAST(post_impressions_organic_unique AS FLOAT64), 0.0) AS post_impressions_organic_unique,
  IFNULL(SAFE_CAST(post_impressions_viral AS FLOAT64), 0.0) AS post_impressions_viral,
  IFNULL(SAFE_CAST(post_impressions_viral_unique AS FLOAT64), 0.0) AS post_impressions_viral_unique,
  IFNULL(SAFE_CAST(post_clicks AS FLOAT64), 0.0) AS post_clicks,
  IFNULL(SAFE_CAST(post_reactions_like_total AS FLOAT64), 0.0) AS post_reactions_like_total,
  IFNULL(SAFE_CAST(post_reactions_love_total AS FLOAT64), 0.0) AS post_reactions_love_total,
  IFNULL(SAFE_CAST(post_reactions_wow_total AS FLOAT64), 0.0) AS post_reactions_wow_total,
  IFNULL(SAFE_CAST(post_reactions_haha_total AS FLOAT64), 0.0) AS post_reactions_haha_total,
  IFNULL(SAFE_CAST(post_reactions_sorry_total AS FLOAT64), 0.0) AS post_reactions_sorry_total,
  IFNULL(SAFE_CAST(post_reactions_anger_total AS FLOAT64), 0.0) AS post_reactions_anger_total,
  IFNULL(SAFE_CAST(post_video_views AS FLOAT64), 0.0) AS post_video_views,
  IFNULL(SAFE_CAST(post_video_views_organic AS FLOAT64), 0.0) AS post_video_views_organic,
  IFNULL(SAFE_CAST(post_video_views_paid AS FLOAT64), 0.0) AS post_video_views_paid,
  IFNULL(SAFE_CAST(post_video_complete_views_30s AS FLOAT64), 0.0) AS post_video_complete_views_30s,

  -- Derived Totals
  (
    IFNULL(SAFE_CAST(post_reactions_like_total AS FLOAT64), 0.0) +
    IFNULL(SAFE_CAST(post_reactions_love_total AS FLOAT64), 0.0) +
    IFNULL(SAFE_CAST(post_reactions_wow_total AS FLOAT64), 0.0) +
    IFNULL(SAFE_CAST(post_reactions_haha_total AS FLOAT64), 0.0) +
    IFNULL(SAFE_CAST(post_reactions_sorry_total AS FLOAT64), 0.0) +
    IFNULL(SAFE_CAST(post_reactions_anger_total AS FLOAT64), 0.0)
  ) AS post_reactions_total,

  (
    IFNULL(SAFE_CAST(comments_count AS FLOAT64), 0.0) +
    IFNULL(SAFE_CAST(shares_count AS FLOAT64), 0.0) +
    (
      IFNULL(SAFE_CAST(post_reactions_like_total AS FLOAT64), 0.0) +
      IFNULL(SAFE_CAST(post_reactions_love_total AS FLOAT64), 0.0) +
      IFNULL(SAFE_CAST(post_reactions_wow_total AS FLOAT64), 0.0) +
      IFNULL(SAFE_CAST(post_reactions_haha_total AS FLOAT64), 0.0) +
      IFNULL(SAFE_CAST(post_reactions_sorry_total AS FLOAT64), 0.0) +
      IFNULL(SAFE_CAST(post_reactions_anger_total AS FLOAT64), 0.0)
    )
  ) AS post_total_engagements,

  SAFE_DIVIDE(
    (
      IFNULL(SAFE_CAST(comments_count AS FLOAT64), 0.0) +
      IFNULL(SAFE_CAST(shares_count AS FLOAT64), 0.0) +
      (
        IFNULL(SAFE_CAST(post_reactions_like_total AS FLOAT64), 0.0) +
        IFNULL(SAFE_CAST(post_reactions_love_total AS FLOAT64), 0.0) +
        IFNULL(SAFE_CAST(post_reactions_wow_total AS FLOAT64), 0.0) +
        IFNULL(SAFE_CAST(post_reactions_haha_total AS FLOAT64), 0.0) +
        IFNULL(SAFE_CAST(post_reactions_sorry_total AS FLOAT64), 0.0) +
        IFNULL(SAFE_CAST(post_reactions_anger_total AS FLOAT64), 0.0)
      )
    ),
    NULLIF(IFNULL(SAFE_CAST(post_impressions_unique AS FLOAT64), 0.0), 0.0)
  ) AS post_engagement_rate

FROM
  facebook_data.facebook_post_insights;
  
  
  