{{ config (
    alias = target.database + '_facebook_campaign_performance'
)}}

SELECT 
campaign_name,
campaign_id,
campaign_effective_status,
campaign_type_default,
date,
date_granularity,
spend,
impressions,
link_clicks,
omni_app_install as app_install,
"app_custom_event.rc_trial_started_event" as trial_started,
"app_custom_event.rc_trial_converted_event" as trial_converted,
"app_custom_event.rc_initial_purchase_event" as initial_purchase,
0 as trial_converted_value,
0 as initial_purchase_value
FROM {{ ref('facebook_performance_by_campaign') }}
