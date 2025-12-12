{{ config (
    alias = target.database + '_blended'
)}}

{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}


WITH appsflyer_data AS (
        {% for granularity in date_granularity_list %}
        SELECT 
            '{{granularity}}' as date_granularity,
            date_trunc('{{granularity}}',date) as date,
			app,
            CASE 
                WHEN source = 'Facebook Ads' THEN 'Meta'
                WHEN source = 'googleadwords_int' THEN 'Google Ads'
                WHEN source = 'tiktokglobal_int' THEN 'Tiktok Ads'
                ELSE source
            END AS channel,
            campaign_name,
            sum(installs) as installs, 
            sum(sessions) as sessions,
            sum(rc_initial_purchase_users) as initial_purchase,
            sum(rc_trial_converted_users) as trial_converted,
            sum(rc_trial_started_users) as trial_started
        FROM {{ source('gsheet_raw','appsflyer_insights') }}
        GROUP BY 1,2,3,4,5
        {% if not loop.last %}UNION ALL{% endif %}
        {% endfor %}
    ),
    
paid_data as
    (SELECT channel, campaign_id::varchar as campaign_id, campaign_name, date::date, date_granularity, app, COALESCE(SUM(spend),0) as spend, COALESCE(SUM(clicks),0) as clicks, 
        COALESCE(SUM(impressions),0) as impressions
    FROM
        (SELECT 'Meta' as channel, campaign_id, campaign_name, date, date_granularity, 
            case 
				when campaign_name ~* '_ios_' then 'iOS'
				when campaign_name ~* '_and_' then 'Android'
				when campaign_name ~* '_web_' then 'Web'
				else 'Other'
			end as app,
			spend, link_clicks as clicks, impressions
        FROM {{ source('reporting','facebook_ad_performance') }}
        UNION ALL
        SELECT 'Google Ads' as channel, campaign_id, campaign_name, date, date_granularity,
			case 
				when campaign_name ~* '_ios_' then 'iOS'
				when campaign_name ~* '_and_' then 'Android'
				when campaign_name ~* '_web_' then 'Web'
				else 'Other'
			end as app,
            spend, clicks, impressions
        FROM {{ source('reporting','googleads_campaign_performance') }}
		UNION ALL
		SELECT 'Tiktok Ads' as channel, campaign_id, campaign_name, date, date_granularity, 
			case 
				when campaign_name ~* '_ios_' then 'iOS'
				when campaign_name ~* '_and_' then 'Android'
				when campaign_name ~* '_web_' then 'Web'
				else 'Other'
			end as app,
			spend, clicks, impressions
        FROM {{ source('reporting','tiktok_ad_performance') }}
        )
    GROUP BY channel, campaign_id, campaign_name, date, date_granularity, app),

paid_appsflyer_data as (
  SELECT 
    channel, campaign_name, date::date, date_granularity, app,
    SUM(COALESCE(spend, 0)) AS spend,
    SUM(COALESCE(clicks, 0)) AS clicks,
    SUM(COALESCE(impressions, 0)) AS impressions,
    SUM(COALESCE(installs, 0)) AS installs,
    SUM(COALESCE(sessions, 0)) AS sessions,
    SUM(COALESCE(initial_purchase, 0)) AS initial_purchase,
    SUM(COALESCE(trial_converted, 0)) AS trial_converted,
    SUM(COALESCE(trial_started, 0)) AS trial_started
  FROM paid_data FULL OUTER JOIN appsflyer_data USING(channel,date,date_granularity,campaign_name,app)
  GROUP BY 1,2,3,4,5)
    
SELECT 
    channel,
	app,
    campaign_name,
    date,
    date_granularity,
    spend,
    clicks,
    impressions,
    installs,
    sessions,
    initial_purchase,
    trial_converted,
    trial_started
FROM paid_appsflyer_data
