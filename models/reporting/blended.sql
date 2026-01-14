{{ config (
    alias = target.database + '_blended'
)}}

{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}


WITH appsflyer_data AS (
        {% for granularity in date_granularity_list %}
        SELECT 
            '{{granularity}}' as date_granularity,
            case when '{{granularity}}' = 'week' then date_trunc('{{granularity}}',date+1)-1 else date_trunc('{{granularity}}',date) end as date,
			app,
            CASE 
                WHEN source = 'Facebook Ads' THEN 'Meta'
                WHEN source = 'googleadwords_int' THEN 'Google Ads'
                WHEN source = 'tiktokglobal_int' THEN 'Tiktok Ads'
                ELSE source
            END AS channel,
            campaign_name,
            sum(sessions) as sessions,
			sum(installs) as apps_installs,
			sum(rc_trial_started_event) as apps_trial_started,
			sum(rc_trial_converted_event) as apps_trial_converted,
			sum(rc_initial_purchase_event) as apps_initial_purchase,
			sum(revenue) as revenue
        FROM {{ source('gsheet_raw','appsflyer_insights') }}
        GROUP BY 1,2,3,4,5
        {% if not loop.last %}UNION ALL{% endif %}
        {% endfor %}
    ),
    
paid_data as
    (SELECT channel, campaign_id::varchar as campaign_id, campaign_name, date::date, date_granularity, app, COALESCE(SUM(spend),0) as spend, COALESCE(SUM(clicks),0) as clicks, 
        COALESCE(SUM(impressions),0) as impressions, COALESCE(SUM(app_install),0) as installs, COALESCE(SUM(trial_started),0) as trial_started, COALESCE(SUM(trial_converted),0) as trial_converted,
		COALESCE(SUM(initial_purchase),0) as initial_purchase
    FROM
        (SELECT 'Meta' as channel, campaign_id::varchar as campaign_id, campaign_name, date, date_granularity, 
            case 
				when campaign_name ~* '_ios_' then 'iOS'
				when campaign_name ~* '_and_' then 'Android'
				when campaign_name ~* '_web_' then 'Web'
				else 'Other'
			end as app,
			spend, link_clicks as clicks, impressions, app_install, trial_started, trial_converted, initial_purchase 
        FROM {{ source('reporting','facebook_campaign_performance') }}
        UNION ALL
        SELECT 'Google Ads' as channel, campaign_id::varchar as campaign_id, campaign_name, date, date_granularity,
			case 
				when campaign_name ~* '_ios_' then 'iOS'
				when campaign_name ~* '_and_' then 'Android'
				when campaign_name ~* '_web_' then 'Web'
				else 'Other'
			end as app,
            spend, clicks, impressions, app_install, trial_started, trial_converted, initial_purchase 
        FROM {{ source('reporting','googleads_campaign_performance') }}
		UNION ALL
		{% for granularity in date_granularity_list %}
        SELECT 
			'Tiktok Ads' as channel, campaign_id::varchar as campaign_id, campaign_name,
			case when '{{granularity}}' = 'week' then date_trunc('{{granularity}}',stat_time_day+1)-1 else date_trunc('{{granularity}}',stat_time_day) end as date,
			'{{granularity}}' as date_granularity,
			case 
				when campaign_name ~* '_ios_' then 'iOS'
				when campaign_name ~* '_and_' then 'Android'
				when campaign_name ~* '_web_' then 'Web'
				else 'Other'
			end as app,
            COALESCE(SUM(spend),0) as spend, 
			COALESCE(SUM(clicks),0) as clicks, 
        	COALESCE(SUM(impressions),0) as impressions, 
			COALESCE(SUM(skan_conversion),0) as installs, 
			COALESCE(SUM(skan_start_trial),0) as trial_started, 
			COALESCE(SUM(0),0) as trial_converted,
			COALESCE(SUM(0),0) as initial_purchase
        FROM {{ source('tiktok_raw','campaign_performance_report') }}
        GROUP BY 1,2,3,4,5,6
        {% if not loop.last %}UNION ALL{% endif %}
        {% endfor %}
        )
    GROUP BY channel, campaign_id, campaign_name, date, date_granularity, app),

paid_appsflyer_data as (
  SELECT 
    channel, campaign_name, coalesce(campaign_id::varchar,'(not set)') as campaign_id, date::date, date_granularity, app,
    SUM(COALESCE(spend, 0)) AS spend,
    SUM(COALESCE(clicks, 0)) AS clicks,
    SUM(COALESCE(impressions, 0)) AS impressions,
    SUM(COALESCE(installs, 0)) AS installs,
    SUM(COALESCE(sessions, 0)) AS sessions,
	SUM(COALESCE(revenue, 0)) AS revenue,
    SUM(COALESCE(initial_purchase, 0)) AS initial_purchase,
    SUM(COALESCE(trial_converted, 0)) AS trial_converted,
    SUM(COALESCE(trial_started, 0)) AS trial_started,
	SUM(COALESCE(apps_installs, 0)) AS apps_installs,
    SUM(COALESCE(apps_initial_purchase, 0)) AS apps_initial_purchase,
    SUM(COALESCE(apps_trial_converted, 0)) AS apps_trial_converted,
    SUM(COALESCE(apps_trial_started, 0)) AS apps_trial_started
  FROM paid_data FULL OUTER JOIN appsflyer_data USING(channel,date,date_granularity,campaign_name,app)
  GROUP BY 1,2,3,4,5,6)
    
SELECT 
    channel,
	app,
    campaign_name,
	campaign_id,
    date,
    date_granularity,
    spend,
    clicks,
    impressions,
    installs,
    sessions,
	revenue,
    initial_purchase,
    trial_converted,
    trial_started,
	apps_installs,
	apps_initial_purchase,
	apps_trial_converted,
	apps_trial_started
FROM paid_appsflyer_data
