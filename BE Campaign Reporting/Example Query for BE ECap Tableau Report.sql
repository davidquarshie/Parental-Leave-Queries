SELECT c.CAMPAIGN_YEAR_MONTH AS main_campaign
     , c.CAMPAIGN_YEAR_MONTH AS reference_campaign
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
     , ae.uid     AS audience_uid
     , es.uid     AS ecap_uid
     , 'Main'     AS campaign_type
     , am.offer
     , am.TREATMENT
FROM PROCESSED.BE_CAMPAIGN.SALES_CAMPAIGN  c
         JOIN PROCESSED.BE_CAMPAIGN.V_MARKETING_AUDIENCE_ALL_CAMPAIGNS am
    --DW.DW_SUMMARY.DBO_REPORTDATA_ALL_MADNESS am
              ON c.CAMPAIGN_YEAR_MONTH = am.CAMPAIGN
         JOIN PROCESSED.BE_CAMPAIGN.AUDIENCE_COMBINED ae
--PROCESSED.BE_CAMPAIGN.AUDIENCE_EXTENSION ae
              ON am.UID = ae.UID
                  AND am.CAMPAIGN = ae.CAMPAIGN
         LEFT JOIN REPORTING.SALES.V_BE_ECAPS es
                   ON es.UID = ae.UID
                       AND es.CAMPAIGN = c.CAMPAIGN_YEAR_MONTH
where c.CAMPAIGN_YEAR_MONTH='2024.08.13 AI Software'