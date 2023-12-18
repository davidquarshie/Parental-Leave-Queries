SELECT oh.delivertouid            AS uid
,oh.PLACEDBYUID as placed_by_uid
     , maac.TREATMENT
     , po.productid
     , p.ProductName
     , oh.orderdate
     , ii.invoiceitemid
     , po.promotionofferid
     , promo.PromotionName
     , oi.unitprice
     , att.TransactionName
     , x.SubscriptionId
     , sour.DESCRIPTION
     , SUM(ati.AmountBeforeTaxes) AS cash
FROM RAW.FOOL_COMMERCE.DBO_INVOICEITEM AS ii
         JOIN RAW.FOOL_COMMERCE.dbo_orderitem AS oi ON ii.orderitemid = oi.orderitemid
         JOIN RAW.FOOL_COMMERCE.dbo_promotionoffer AS po ON oi.promotionofferid = po.promotionofferid
         JOIN RAW.FOOL_COMMERCE.dbo_Promotion promo
              ON po.PromotionId = promo.PromotionId
         JOIN RAW.FOOL_COMMERCE.dbo_orderheader AS oh ON oi.orderid = oh.orderid
         JOIN RAW.FOOL_COMMERCE.dbo_accountingtransactionitem AS ati ON ii.invoiceitemid = ati.parenttransactionitemid
         JOIN RAW.FOOL_COMMERCE.dbo_product AS p ON po.productid = p.productid
         JOIN RAW.FOOL_COMMERCE.dbo_accountingtransactiontype AS att
              ON ati.accountingtransactiontype = att.accountingtransactiontype
         JOIN RAW.FOOL_COMMERCE.dbo_accountingtransaction AS at ON ati.transactionid = at.transactionid
         LEFT JOIN RAW.FOOL_COMMERCE.dbo_Subscription x
                   ON x.ProductId = po.ProductId
                       AND x.uid = oh.DeliverToUid
                       AND x.StartDate >= '2023-12-05'
         left join PROCESSED.SHARED.SOURCE sour
                   on sour.SOURCE_CODE = oh.SOURCECODE

         left join PROCESSED.BE_CAMPAIGN.V_MARKETING_AUDIENCE_ALL_CAMPAIGNS maac
                   on maac.uid = oh.delivertouid
                       AND CAMPAIGN = 'MPDec23'


WHERE CAST(oh.orderdate AS DATE) >= '2023-12-05' --and '2021-04-08 23:50:00'

  AND PromotionName NOT LIKE '%AOX%'
  --AND PromotionName NOT LIKE '%Ret%'
  AND TransactionName = 'Payment'
--and PRODUCTNAME like '%Hidden%'
  and PROMOTIONNAME ilike '%window%'
  --and PROMOTIONNAME ilike '%IU%'

 -- and oh.DELIVERTOUID=2057165092
--and PRODUCTNAME ='Everlasting Portfolio'--ilike any ('%Epic%','%Virtu%')
GROUP BY oh.delivertouid
,oh.PLACEDBYUID
       , maac.TREATMENT
       , po.productid
       , p.ProductName
       , po.productid
       , oh.orderdate
       , ii.invoiceitemid
       , po.promotionofferid
       , promo.PromotionName
       , oi.unitprice
       , att.TransactionName
       , x.SubscriptionId
       , sour.DESCRIPTION

/*
with orders as
(select uid
,sum(ORDERS) orders
from PROCESSED.BE_CAMPAIGN.V_BE_ORDERS_SIMPLE es
where es.CAMPAIGN_YEAR_MONTH='EPAugl23'
group by 1)

select
       o.UID,
       AccountId
from orders o
left join RAW.FOOL_PARTY.DBO_PARTYEMAIL e
on e.uid = o.UID
where o.orders>0

 */

