{{ config (
    alias = target.database + '_tiktok_ad_performance'
)}}

SELECT 
campaign_name,
campaign_id,
campaign_status,
campaign_type_default,
adgroup_name,
adgroup_id,
adgroup_status,
audience,
ad_name,
ad_id,
ad_status,
visual,
date,
date_granularity,
cost as spend,
impressions,
clicks,
complete_payment as purchases,
total_complete_payment_rate as revenue,
web_event_add_to_cart as atc,
skan_conversion as app_install,
0 as trial_started,
0 as trial_converted,
0 as initial_purchase
FROM {{ ref('tiktok_performance_by_ad') }}
