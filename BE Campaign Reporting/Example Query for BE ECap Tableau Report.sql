with campaign as (
    select sc.CAMPAIGN_YEAR_MONTH        CAMPAIGN
         , sc.SALES_CAMPAIGN_ID
         , sc_one.CAMPAIGN_YEAR_MONTH as comp_one
         , scc.COMPARISON_CAMPAIGN_ID
         , sc_two.CAMPAIGN_YEAR_MONTH as comp_two
         , scc.COMPARISON_CAMPAIGN_ID_2
    from PROCESSED.BE_CAMPAIGN.SALES_CAMPAIGN sc
--Main
             join PROCESSED.BE_CAMPAIGN.SALES_CAMPAIGN_COMPARISONS scc
                  on sc.SALES_CAMPAIGN_ID = scc.SALES_CAMPAIGN_ID
--Comp1
             left join PROCESSED.BE_CAMPAIGN.SALES_CAMPAIGN sc_one
                       on sc_one.SALES_CAMPAIGN_ID = scc.COMPARISON_CAMPAIGN_ID
--Comp2
             left join PROCESSED.BE_CAMPAIGN.SALES_CAMPAIGN sc_two
                       on sc_two.SALES_CAMPAIGN_ID = scc.COMPARISON_CAMPAIGN_ID_2
    where sc.CAMPAIGN_YEAR_MONTH = <Parameters.campaign_year_month>)

--Main
select c.CAMPAIGN as main_campaign
     , c.CAMPAIGN as reference_campaign
     , ae.HIGH_PRODUCT_RANK
     , ae.RELATIONSHIP_TYPE
     , ae.SUBSCRIPTION_MARKETING
     , ae.BE_COUNT
     , ae.FE_COUNT
     , ae.TOTAL_SERVICES
     , ae.PERCENTILE_GROUP
     , NL_VISITS
     , RECENCY_TARGET_GROUP
     , es.DAYS_SINCE_LB_START
     , ae.uid     as audience_uid
     , es.uid     as ecap_uid
     , 'Main'     as campaign_type
     , am.offer
     , am.TREATMENT
from campaign c
         join PROCESSED.BE_CAMPAIGN.V_MARKETING_AUDIENCE_ALL_CAMPAIGNS am
              on c.CAMPAIGN = am.CAMPAIGN
         join PROCESSED.BE_CAMPAIGN.AUDIENCE_COMBINED ae
              on am.UID = ae.UID
                  and am.CAMPAIGN = ae.CAMPAIGN
         left join REPORTING.SALES.V_BE_ECAPS es
                   on es.UID = ae.UID
                       and es.CAMPAIGN = ae.CAMPAIGN

union all

--Comp1
select c.CAMPAIGN as main_campaign
     , c.comp_one    reference_campaign
     , ae_1.HIGH_PRODUCT_RANK
     , ae_1.RELATIONSHIP_TYPE
     , ae_1.SUBSCRIPTION_MARKETING
     , ae_1.BE_COUNT
     , ae_1.FE_COUNT
     , ae_1.TOTAL_SERVICES
     , ae_1.PERCENTILE_GROUP
     , NL_VISITS
     , RECENCY_TARGET_GROUP
     , es.DAYS_SINCE_LB_START
     , ae_1.uid   as audience_uid
     , es.uid     as ecap_uid
     , 'Comp1'    as campaign_type
     , am_1.offer
     , am_1.TREATMENT
from campaign c
         join PROCESSED.BE_CAMPAIGN.V_MARKETING_AUDIENCE_ALL_CAMPAIGNS am_1
              on c.comp_one = am_1.CAMPAIGN
         join PROCESSED.BE_CAMPAIGN.AUDIENCE_COMBINED ae_1
              on am_1.UID = ae_1.UID
                  and am_1.CAMPAIGN = ae_1.CAMPAIGN
         left join REPORTING.SALES.V_BE_ECAPS es
                   on es.UID = ae_1.UID
                       and es.CAMPAIGN = ae_1.CAMPAIGN

union all

--Comp2
select c.CAMPAIGN as main_campaign
     , c.comp_two    reference_campaign
     , ae_2.HIGH_PRODUCT_RANK
     , ae_2.RELATIONSHIP_TYPE
     , ae_2.SUBSCRIPTION_MARKETING
     , ae_2.BE_COUNT
     , ae_2.FE_COUNT
     , ae_2.TOTAL_SERVICES
     , ae_2.PERCENTILE_GROUP
     , NL_VISITS
     , RECENCY_TARGET_GROUP
     , es.DAYS_SINCE_LB_START
     , ae_2.uid   as audience_uid
     , es.uid     as ecap_uid
     , 'Comp2'    as campaign_type
     , am_2.offer
     , am_2.TREATMENT
from campaign c
         join PROCESSED.BE_CAMPAIGN.V_MARKETING_AUDIENCE_ALL_CAMPAIGNS am_2
              on c.comp_two = am_2.CAMPAIGN
         join PROCESSED.BE_CAMPAIGN.AUDIENCE_COMBINED ae_2
              on am_2.UID = ae_2.UID
                  and am_2.CAMPAIGN = ae_2.CAMPAIGN
         left join REPORTING.SALES.V_BE_ECAPS es
                   on es.UID = ae_2.UID
                       and es.CAMPAIGN = ae_2.CAMPAIGN
