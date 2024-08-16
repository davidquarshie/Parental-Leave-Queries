WITH campaign AS (
    SELECT sc.CAMPAIGN_YEAR_MONTH        CAMPAIGN
         , sc.SALES_CAMPAIGN_ID
         , sc_one.CAMPAIGN_YEAR_MONTH AS comp_one
         , scc.COMPARISON_CAMPAIGN_ID
         , sc_two.CAMPAIGN_YEAR_MONTH AS comp_two
         , scc.COMPARISON_CAMPAIGN_ID_2
    FROM PROCESSED.BE_CAMPAIGN.SALES_CAMPAIGN sc
--Main
             left JOIN PROCESSED.BE_CAMPAIGN.SALES_CAMPAIGN_COMPARISONS scc
                  ON sc.SALES_CAMPAIGN_ID = scc.SALES_CAMPAIGN_ID
--Comp1
             LEFT JOIN PROCESSED.BE_CAMPAIGN.SALES_CAMPAIGN sc_one
                       ON sc_one.SALES_CAMPAIGN_ID = scc.COMPARISON_CAMPAIGN_ID
--Comp2
             LEFT JOIN PROCESSED.BE_CAMPAIGN.SALES_CAMPAIGN sc_two
                       ON sc_two.SALES_CAMPAIGN_ID = scc.COMPARISON_CAMPAIGN_ID_2
    WHERE sc.CAMPAIGN_YEAR_MONTH = '2024.08.13 AI Software')

--Main
SELECT c.CAMPAIGN AS main_campaign
     , c.CAMPAIGN AS reference_campaign
     , ae.HIGH_PRODUCT_RANK
     , ae.RELATIONSHIP_TYPE
     , ae.SUBSCRIPTION_MARKETING
     , ae.BE_COUNT
     , ae.FE_COUNT
     , ae.TOTAL_SERVICES
     , ae.PERCENTILE_GROUP
     , NL_VISITS
     , RECENCY_TARGET_GROUP
     , es.SALESDAYNUM
     , es.ORDERS
     , es.CASH
     , ae.uid     AS audience_uid
     , es.uid     AS order_uid
     , 'Main'     AS campaign_type
     , es.PRODUCT_NAME
     , es.DESCRIPTION
     , es.CHANNEL
     , am.TREATMENT
     , am.offer
     , ecap.UID   AS ecap_uid
     , es.VEHICLE
     , pro.TAGSJSON
     , es.SOURCEDESCRIPTION
     , es.SOURCE_CODE
     , es.COMMERCE_PROMO_GUID
     , pro.PROMOTIONOFFERID
     , oi.PROMOTIONOFFERID AS OI_PROMOTIONOFFERID
     , pro.PROMOTIONOFFERNAME
     , oi.UNITPRICE
FROM campaign c
         JOIN PROCESSED.BE_CAMPAIGN.V_MARKETING_AUDIENCE_ALL_CAMPAIGNS am
    --DW.DW_SUMMARY.DBO_REPORTDATA_ALL_MADNESS am
--PROCESSED.BE_CAMPAIGN.V_MARKETING_AUDIENCE_ALL_CAMPAIGNS am switch to this fro VHJan23
              ON c.CAMPAIGN = am.CAMPAIGN
         JOIN PROCESSED.BE_CAMPAIGN.AUDIENCE_COMBINED ae
              ON am.UID = ae.UID
                  AND am.CAMPAIGN = ae.CAMPAIGN
         LEFT JOIN PROCESSED.BE_CAMPAIGN.V_BE_ORDERS_SIMPLE es
                   ON es.UID = ae.UID
                       AND es.CAMPAIGN_YEAR_MONTH = ae.CAMPAIGN
                       AND DESCRIPTION ILIKE '%Window&%'
         LEFT JOIN PROCESSED.BE_CAMPAIGN.V_BE_ECAPS_SIMPLE ecap
                   ON ecap.UID = am.UID
                       AND ecap.CAMPAIGN = am.CAMPAIGN
         LEFT JOIN raw.FOOL_COMMERCE.DBO_ORDERITEM oi
                   ON es.ORDERID = oi.ORDERITEMID
         LEFT JOIN raw.FOOL_COMMERCE.DBO_PROMOTIONOFFER pro
                   ON oi.PROMOTIONOFFERID = pro.PROMOTIONOFFERID

UNION ALL

--Comp1
SELECT c.CAMPAIGN AS main_campaign
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
     , es.SALESDAYNUM
     , es.ORDERS
     , es.CASH
     , ae_1.uid   AS audience_uid
     , es.uid     AS order_uid
     , 'Comp1'    AS campaign_type
     , es.PRODUCT_NAME
     , es.DESCRIPTION
     , es.CHANNEL
     , am_1.TREATMENT
     , am_1.offer
     , ecap.UID   AS ecap_uid
     , es.VEHICLE
     , pro.TAGSJSON
     , es.SOURCEDESCRIPTION
     , es.SOURCE_CODE
     , es.COMMERCE_PROMO_GUID
     , pro.PROMOTIONOFFERID
     , oi.PROMOTIONOFFERID AS OI_PROMOTIONOFFERID
     , pro.PROMOTIONOFFERNAME
     , oi.UNITPRICE
FROM campaign c
         LEFT JOIN PROCESSED.BE_CAMPAIGN.V_MARKETING_AUDIENCE_ALL_CAMPAIGNS am_1
    --DW.DW_SUMMARY.DBO_REPORTDATA_ALL_MADNESS am_1
--PROCESSED.BE_CAMPAIGN.V_MARKETING_AUDIENCE_ALL_CAMPAIGNS am switch to this fro VHJan23
                   ON c.comp_one = am_1.CAMPAIGN
         JOIN PROCESSED.BE_CAMPAIGN.AUDIENCE_COMBINED ae_1
              ON am_1.UID = ae_1.UID
                  AND am_1.CAMPAIGN = ae_1.CAMPAIGN
         LEFT JOIN PROCESSED.BE_CAMPAIGN.V_BE_ORDERS_SIMPLE es
                   ON es.UID = ae_1.UID
                       AND es.CAMPAIGN_YEAR_MONTH = ae_1.CAMPAIGN
                       AND DESCRIPTION ILIKE '%Window&%'
         LEFT JOIN PROCESSED.BE_CAMPAIGN.V_BE_ECAPS_SIMPLE ecap
                   ON ecap.UID = ae_1.UID
                       AND ecap.CAMPAIGN = ae_1.CAMPAIGN
         LEFT JOIN raw.FOOL_COMMERCE.DBO_ORDERITEM oi
                   ON es.ORDERID = oi.ORDERITEMID
         LEFT JOIN raw.FOOL_COMMERCE.DBO_PROMOTIONOFFER pro
                   ON oi.PROMOTIONOFFERID = pro.PROMOTIONOFFERID

UNION ALL

--Comp2
SELECT c.CAMPAIGN AS main_campaign
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
     , es.SALESDAYNUM
     , es.ORDERS
     , es.CASH
     , ae_2.uid   AS audience_uid
     , es.uid     AS order_uid
     , 'Comp2'    AS campaign_type
     , es.PRODUCT_NAME
     , es.DESCRIPTION
     , es.CHANNEL
     , am_2.TREATMENT
     , am_2.offer
     , ecap.UID   AS ecap_uid
     , es.VEHICLE
     , pro.TAGSJSON
     , es.SOURCEDESCRIPTION
     , es.SOURCE_CODE
     , es.COMMERCE_PROMO_GUID
     , pro.PROMOTIONOFFERID
     , oi.PROMOTIONOFFERID AS OI_PROMOTIONOFFERID
     , pro.PROMOTIONOFFERNAME
     , oi.UNITPRICE
FROM campaign c
         LEFT JOIN PROCESSED.BE_CAMPAIGN.V_MARKETING_AUDIENCE_ALL_CAMPAIGNS am_2
                   ON c.comp_two = am_2.CAMPAIGN
         JOIN PROCESSED.BE_CAMPAIGN.AUDIENCE_COMBINED ae_2
              ON am_2.UID = ae_2.UID
                  AND am_2.CAMPAIGN = ae_2.CAMPAIGN
         LEFT JOIN PROCESSED.BE_CAMPAIGN.V_BE_ORDERS_SIMPLE es
                   ON es.UID = ae_2.UID
                       AND es.CAMPAIGN_YEAR_MONTH = ae_2.CAMPAIGN
                       AND DESCRIPTION ILIKE '%Window&%'
         LEFT JOIN PROCESSED.BE_CAMPAIGN.V_BE_ECAPS_SIMPLE ecap
                   ON ecap.UID = ae_2.UID
                       AND ecap.CAMPAIGN = ae_2.CAMPAIGN
         LEFT JOIN raw.FOOL_COMMERCE.DBO_ORDERITEM oi
                   ON es.ORDERID = oi.ORDERITEMID
         LEFT JOIN raw.FOOL_COMMERCE.DBO_PROMOTIONOFFER pro
                   ON oi.PROMOTIONOFFERID = pro.PROMOTIONOFFERID