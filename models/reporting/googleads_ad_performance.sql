{{ config (
    alias = target.database + '_googleads_ad_performance'
)}}

SELECT
account_id,
ad_id,
campaign_name,
campaign_id,
campaign_status,
campaign_type_default,
ad_group_name,
ad_group_id,
date,
date_granularity,
spend,
impressions,
clicks,
"stardustprod5a598comstarduststardustappiosfirstopen" as app_install,
"rc_trial_started_via_firebaseios" as trial_started,
"stardustprod5a598comstarduststardustappiosapp_store_subscription_convert" as trial_converted,
"d2p_t2p_via_firebaseios" as initial_purchase,
conversions as purchases,
conversions_value as revenue

FROM {{ ref('googleads_performance_by_ad') }}
