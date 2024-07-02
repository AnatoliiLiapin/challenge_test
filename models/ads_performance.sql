with
    ads_performance as (
        select
            cast(ad_id as string) as ad_id,
            cast(null as int64) as add_to_cart,
            cast(adset_id as string) as adset_id,
            campaign_id,
            channel,
            clicks,
            cast(null as int64) as comments,
            null as creative_id,
            cast(__insert_date as date) as date,
            cast(null as int64) as engagements,
            imps as impressions,
            cast(null as int64) as installs,
            cast(null as int64) as likes,
            cast(null as int64) as link_clicks,
            cast(null as string) as placement_id,
            cast(null as int64) as post_click_conversions,
            cast(null as int64) as post_view_conversions,
            cast(null as int64) as posts,
            cast(null as int64) as purchase,
            cast(null as int64) as registrations,
            revenue,
            cast(null as int64) as shares,
            spend,
            conv as total_conversions,
            cast(null as int64) as video_views
        from {{ ref("src_ads_bing_all_data") }}
        union all
        select
            cast(ad_id as string) as ad_id,
            add_to_cart,
            cast(null as string) as adset_id,
            campaign_id,
            channel,
            clicks,
            cast(null as int64) as comments,
            null as creative_id,
            cast(__insert_date as date) as date,
            cast(null as int64) as engagements,
            impressions,
            cast(null as int64) as installs,
            cast(null as int64) as likes,
            cast(null as int64) as link_clicks,
            cast(null as string) as placement_id,
            cast(null as int64) as post_click_conversions,
            cast(null as int64) as post_view_conversions,
            cast(null as int64) as posts,
            purchase,
            registrations,
            cast(null as int64) as revenue,
            cast(null as int64) as shares,
            spend,
            conversions as total_conversions,
            video_views
        from {{ ref("src_ads_tiktok_ads_all_data") }}
        union all
        select
            cast(null as string) as ad_id,
            cast(null as int64) as add_to_cart,
            cast(null as string) as adset_id,
            campaign_id,
            channel,
            clicks,
            comments,
            null as creative_id,
            cast(__insert_date as date) as date,
            engagements,
            impressions,
            cast(null as int64) as installs,
            likes,
            url_clicks as link_clicks,
            cast(null as string) as placement_id,
            cast(null as int64) as post_click_conversions,
            cast(null as int64) as post_view_conversions,
            cast(null as int64) as posts,
            cast(null as int64) as purchase,
            cast(null as int64) as registrations,
            cast(null as int64) as revenue,
            retweets as shares,
            spend,
            cast(null as int64) as total_conversions,
            video_total_views as video_views
        from {{ ref("src_promoted_tweets_twitter_all_data") }}
        union all
        select
            cast(ad_id as string) as ad_id,
            add_to_cart,
            cast(adset_id as string) as adset_id,
            campaign_id,
            channel,
            clicks,
            comments,
            creative_id,
            cast(__insert_date as date) as date,
            (
                likes
                + shares
                + comments
                + views
                + clicks
                + mobile_app_install
                + complete_registration
            ) as engagements,
            impressions,
            mobile_app_install as installs,
            likes,
            inline_link_clicks as link_clicks,
            cast(null as string) as placement_id,
            cast(null as int64) as post_click_conversions,
            cast(null as int64) as post_view_conversions,
            cast(null as int64) as posts,
            purchase,
            complete_registration as registrations,
            purchase_value as revenue,
            shares,
            spend,
            purchase as total_conversions,
            cast(null as int64) as video_views
        from {{ ref("src_ads_creative_facebook_all_data") }}
    )

select *
from ads_performance
