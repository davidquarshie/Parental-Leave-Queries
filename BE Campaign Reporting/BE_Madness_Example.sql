USE SCHEMA OPERATIONAL.EMAIL_OPS;
use role EMAIL_OPS;

set campaignid = 2282;
set campaigniid = 333455776;
set maincampaigntreatment = 'Boss_Mode_Campaign';


-- For the smiley suppressions the two most recent campaigns we want to use for holdouts
select top 10 *
from processed.be_campaign.sales_campaign sc
order by sales_campaign_id desc;

set mostrecentcampaignacronym = 'SmallCapsAIJul24';
set secondmostrecentcampaignacronym ='BossFlashJul24';


-- The previous script is having some issues with setting this flag, so doing this manually until we can get that fixed
update OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE as p
set can_mail = 1
from (
         Select pool.uid
         from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE pool
         where pool.MF1_SUB = 0
           --and pool.EMAIL_VALID = 1
           and pool.IS_CANADIAN = 0
           and pool.FOOL_EMAIL = 0
           and pool.FE_FORMER = 0
           and pool.NEVERMAIL = 0
           and pool.BOUNCERS = 0
           and pool.ADMIN_BLOCKED = 0
           and pool.COMPLAINERS = 0
           and pool.PRIVACY_OPT_OUT = 0
           and pool.LAST_MKTG_ENGAGE_TIMESTAMP >= dateadd(year, -1, current_date)
           AND pool.ON_SPECIAL_OFFERS = 0
           and pool.SNOOZED_MARKETING = 0
           and pool.AD_HOC_SUPPRESSIONS = '{}'
           and pool.SALES_CAMPAIGN_ID = $campaignid
     ) subq
where subq.uid = p.uid
  and p.SALES_CAMPAIGN_ID = $campaignid;

--Check Counts
select can_mail, count(UID)
from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE
where SALES_CAMPAIGN_ID = $campaignid
group by all;


-- clear the decks
update OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE
set CAMPAIGN_SPECIFIC_DATA = null
where CAMPAIGN_SPECIFIC_DATA is not null
  and SALES_CAMPAIGN_ID = $campaignid;


-- GT, 2023-01-11: So it turns out that OBJECT_INSERT doesn't work unless the variant field already has JSON
-- in it. E.g., OBJECT_INSERT into a NULL variant field does nothing at all. We need to initialize the field with
-- empty JSON and then can do inserts afterward
update OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
SET CAMPAIGN_SPECIFIC_DATA = to_variant(parse_json('{}'))
where p.SALES_CAMPAIGN_ID = $campaignid;


-- GT- this has been a long time temp thing. Probably doesn't need to go into the default script, but
-- should (for the time being) still run every time
create or replace temporary table products as
select p.*
from RAW.FOOL_COMMERCE.DBO_PRODUCT as p
         join RAW.FOOL_COMMERCE.DBO_PRODUCTTYPE as pt on p.ProductType = pt.ProductType
         join RAW.FOOL_COMMERCE.DBO_SUBSCRIPTIONPRODUCT sp on sp.ProductId = p.ProductId
where lower(pt.ProductTypeDescription) = 'subscription'
  and upper(p.Brand) = 'USMF'
  and ifnull(p.DiscontinuationDate, current_date()) >= current_date()
-- these are discontinued or not real products but dont have discontinuation dates
  and p.ProductId not in
      (
       4189 -- Motley Fool Test Site for New PMs
          , 4171 -- Top Stocks
          , 4095 -- Discovery: The Long-Short Portfolio
          , 4030 -- Financial Planning Retainer Fee
          , 1463 -- QA Base Product
          , 1464 -- QA Container Product
          , 1772 -- Motley Fool Options Basic
          , 1830 -- Motley Fool Autobots
          , 2857 -- Motley Fool One Classic
          , 2858 -- Motley Fool Wealth Management
          , 2885 -- Motley Fool Wealth Management SMAs
          , 3882 -- Financial Planning from Motley Fool Wealth Management (Retainer)
          , 3902 -- 360 Planning
          , 3991 -- Motley Fool Wealth Defender Bundle
          , 3994 -- Motley Fool Explorer
          , 4441 -- Market Pass / Marijuana Masters Bundle
          , 4355 -- Blast Off Renewal Reports
          , 4262 -- The Motley Fool
          );

--Tag Single FE members
create or replace temporary table single_FE_members as
select sub.UID, 1 as SingleFE
from RAW.FOOL_COMMERCE.DBO_SUBSCRIPTION sub
         join
     (
         select UID, count(distinct PRODUCTID) as subs
         from RAW.FOOL_COMMERCE.DBO_SUBSCRIPTION
         where SUBSCRIPTIONSTATUSTYPE = 1
         group by UID
         having count(distinct PRODUCTID) = 1
     ) z on sub.UID = z.uid
         and sub.SUBSCRIPTIONSTATUSTYPE = 1
         join products p on sub.PRODUCTID = p.PRODUCTID
    and p.ISBACKEND = 0
    and p.PRODUCTID <> 4299; -- Epic Bundle, which doesn't count for these purposes


UPDATE OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
set CAMPAIGN_SPECIFIC_DATA = OBJECT_INSERT(p.CAMPAIGN_SPECIFIC_DATA, 'Single_FE_Member', z.singlefestatus, false)
from (
         select pool.uid, ifnull(s.SingleFE, 0) as singlefestatus
         from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE pool
                  left join single_FE_members s
                            on pool.uid = s.UID
         where pool.SALES_CAMPAIGN_ID = $campaignid
     ) z
where p.uid = z.uid
  and p.SALES_CAMPAIGN_ID = $campaignid;


--Tag Multi FE members
-- GT, 2024-07-12: these guys are a thing of the past now :pour one out:
create or replace temporary table multi_FE_members as
select UID
     , count(distinct sub.PRODUCTID) as subs
     , 1                             as MultiFE
from RAW.FOOL_COMMERCE.DBO_SUBSCRIPTION sub
         join products p
              on sub.PRODUCTID = p.PRODUCTID
                  and p.ISBACKEND = 0
where SUBSCRIPTIONSTATUSTYPE = 1
group by UID
having count(distinct sub.PRODUCTID) > 1;


UPDATE OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
set CAMPAIGN_SPECIFIC_DATA = OBJECT_INSERT(p.CAMPAIGN_SPECIFIC_DATA, 'Multi_FE_Member', z.singlefestatus, false)
from (
         select pool.uid, ifnull(s.MultiFE, 0) as singlefestatus
         from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE pool
                  left join multi_FE_members s
                            on pool.uid = s.UID
         where pool.SALES_CAMPAIGN_ID = $campaignid
     ) z
where p.uid = z.uid
  and p.SALES_CAMPAIGN_ID = $campaignid;


-- =================================================================
-- Campaign-specific logic/holdouts begins here
-- =================================================================

/* update for AI Software Aug24
   --Since we're selling Fool Port but also selling the report to Fool Port owners we need to have them in the campaign

-- GT: if the campaign is a simple one -- no funny business with multiple products or bundles or any
-- of that -- suppress the folks that have access to the product you're selling
-- Holdout anyone that owns these products
UPDATE OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
set AD_HOC_SUPPRESSIONS = OBJECT_INSERT(p.AD_HOC_SUPPRESSIONS, 'Owns_Holdout_Product', 1, false)
  , CAN_MAIL            = 0
where p.PRODUCT_ACCESS:"4155" = 1
  and p.SALES_CAMPAIGN_ID = $campaignid;
 */

-- Suppress any members that have clicked on the negative smiley during the previous 2 campaigns
create or replace temporary table negativesmileyclickers as
select distinct c.UID
from PROCESSED.EMAIL.EMAIL_LINK_CLICK elc
         join PROCESSED.EMAIL.EMAIL_LINK el
              ON el.link_id = elc.link_id
         join PROCESSED.EMAIL.ISSUE AS i
              on i.ISSUE_ID = elc.ISSUE_ID
         join PROCESSED.SHARED.CUSTOMER c
              on elc.CUSTOMER_ID = c.CUSTOMER_ID
         join PROCESSED.BE_CAMPAIGN.SALES_CAMPAIGN sc
              on SPLIT_PART(i.ISSUE_NAME, '-', 4) = SPLIT_PART(sc.CAMPAIGN_NAME_FOR_EMAIL, '_', 1)
where el.LINK_MINUS_TOKEN LIKE '%op=11183%'
  and i.ISSUE_NAME like 'BE-%'
  and sc.CAMPAIGN_YEAR_MONTH in ($mostrecentcampaignacronym, $secondmostrecentcampaignacronym)
  and MESSAGE_SUBJECT is not null;

UPDATE OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
set AD_HOC_SUPPRESSIONS = OBJECT_INSERT(p.AD_HOC_SUPPRESSIONS, 'Negative_Smiley_Clicker', 1, false)
  , CAN_MAIL            = 0
  , treatment= 'Holdoutout_Negative_Smiley_Clickers'
from (
         select aj.uid
         from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE aj
                  join negativesmileyclickers cli
                       on aj.uid = cli.UID
         where aj.SALES_CAMPAIGN_ID = $campaignid
           and aj.can_mail = 1
     ) subq
where p.uid = subq.uid
  and p.SALES_CAMPAIGN_ID = $campaignid;


-- Flag 90 day email clickers
create or replace temporary table clickslast90days as
Select pool.uid, count(ea.ISSUE_ID) as clicks
from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE pool
         JOIN PROCESSED.EMAIL.EMAIL_ACTIVITY ea
              on pool.uid = ea.UID
         join PROCESSED.EMAIL.V_ISSUE iss
              on ea.ISSUE_ID = iss.ISSUE_ID
where ea.UTC_SEND_DATETIME >= dateadd(day, -90, current_timestamp)
  and ea.CLICKED >= 1
  and pool.SALES_CAMPAIGN_ID = $campaignid
group by pool.uid;

update OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
set CAMPAIGN_SPECIFIC_DATA = OBJECT_INSERT(p.CAMPAIGN_SPECIFIC_DATA, 'Clicks_90', ifnull(subq.clicks, 0), false)
FROM (Select pool.uid, ifnull(cli.clicks, 0) as clicks
      from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE pool
               left join clickslast90days cli
                         on pool.uid = cli.uid
      where pool.SALES_CAMPAIGN_ID = $campaignid
     ) subq
where subq.uid = p.uid
  and p.SALES_CAMPAIGN_ID = $campaignid;


-- Flag sixty day visits
create or replace temporary table sixty_day_visits as
--GA4
select sit.uid, count(ur.URL_ID) as visits
from PROCESSED.ACTIVITY.EVENT_90 sit -- was PROCESSED.WEB.SITEUSAGE in UA
         join PROCESSED.ACTIVITY.URL_ID_LOOKUP ur --was PROCESSED.WEB.URL in UA
              on ur.URL_ID = sit.URL_ID
where ur.URL ilike '/premium%'
  and sit.DATETIME_UTC >= dateadd(day, -60, current_date)
group by sit.uid;


UPDATE OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
set CAMPAIGN_SPECIFIC_DATA = OBJECT_INSERT(p.CAMPAIGN_SPECIFIC_DATA, 'Visits_60', z.visits, false)
from (
         select pool.uid, ifnull(sdv.visits, 0) as visits
         from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE pool
                  left join sixty_day_visits sdv on pool.uid = sdv.UID
         where pool.SALES_CAMPAIGN_ID = $campaignid
     ) z
where p.uid = z.uid
  and p.SALES_CAMPAIGN_ID = $campaignid;


-- Flag bought a sub in the last 6 months
create or replace temporary table recently_bought_sub as
select pool.uid, 1 as boughtrecently
from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE pool
         join raw.FOOL_COMMERCE.DBO_SUBSCRIPTION sub
              on pool.uid = sub.UID
where pool.SALES_CAMPAIGN_ID = $campaignid
  and sub.SUBSCRIPTIONSTATUSTYPE = 1
  and sub.STARTDATE >= dateadd(month, -6, current_date)
  and pool.SALES_CAMPAIGN_ID = $campaignid;

UPDATE OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
set CAMPAIGN_SPECIFIC_DATA = OBJECT_INSERT(p.CAMPAIGN_SPECIFIC_DATA, 'Started_Sub_Within_Six_months',
                                           subq.boughtrecently, false)
from (
         select pool.uid, ifnull(rec.boughtrecently, 0) as boughtrecently
         from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE pool
                  left join recently_bought_sub rec
                            on pool.uid = rec.uid
         where pool.SALES_CAMPAIGN_ID = $campaignid
     ) subq
where p.uid = subq.uid
  and p.SALES_CAMPAIGN_ID = $campaignid;


--New member suppression: Hold out any single FE member that joined in the last 14 days
UPDATE OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
set AD_HOC_SUPPRESSIONS = OBJECT_INSERT(p.AD_HOC_SUPPRESSIONS, 'JoinedLast14Days', 1, false)
  , CAN_MAIL            = 0
from (
         select aj.uid
         from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE aj
                  join raw.FOOL_COMMERCE.DBO_SUBSCRIPTION sub
                       on aj.uid = sub.uid
         where aj.SALES_CAMPAIGN_ID = $campaignid
           and aj.can_mail = 1
           and sub.SUBSCRIPTIONSTATUSTYPE = 1
           and sub.STARTDATE >= dateadd(day, -14, current_date)
           and aj.CAMPAIGN_SPECIFIC_DATA:Single_FE_Member = 1
     ) subq
where p.uid = subq.uid
  and p.SALES_CAMPAIGN_ID = $campaignid;



-- Hold out 90% of single FE members (excluding Epic) who haven’t clicked any emails in the last 90 days and have had no 
-- premium site visits last 60 days. Ignore this suppression for any member that started their subscription in the last 6 months
UPDATE OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
set treatment= iff(subq.r = 0, '(1) Level 1: Unengaged_10PctInCampaign', 'Unengaged_90PctHeldOut')
from (
         select uid, row_number() over (order by uuid_string()) % 10 as r
         from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE
         where SALES_CAMPAIGN_ID = $campaignid
           and CAMPAIGN_SPECIFIC_DATA:Single_FE_Member = 1
           and CAMPAIGN_SPECIFIC_DATA:Clicks_90 = 0
           and CAMPAIGN_SPECIFIC_DATA:Visits_60 = 0
           and CAMPAIGN_SPECIFIC_DATA:Started_Sub_Within_Six_months = 0
           and can_mail = 1
     ) subq
where p.uid = subq.uid
  and p.SALES_CAMPAIGN_ID = $campaignid;


UPDATE OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
set AD_HOC_SUPPRESSIONS = OBJECT_INSERT(p.AD_HOC_SUPPRESSIONS, 'Unengaged_90PctHeldOut', 1, false)
  , CAN_MAIL            = 0 -- this is a suppression; make sure we mark as unmailable
from (
         select uid
         from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE
         where SALES_CAMPAIGN_ID = $campaignid
           and treatment = 'Unengaged_90PctHeldOut'
           and can_mail = 1
     ) subq
where p.uid = subq.uid
  and p.SALES_CAMPAIGN_ID = $campaignid;

/*
-- gut check
select can_mail
     , treatment
     , offer
     ,sp.productlevel
     , count(*)
from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE mma
join raw.fool_commerce.dbo_subscription s 
ON s.uid = mma.uid
JOIN raw.fool_commerce.dbo_subscriptionproduct sp
on sp.productid = s.productid
--where  s.subscriptionstatustype = 1
where sales_campaign_id = $campaignid
group by all
order by 1;


 */
-- gut check2
select can_mail
     , treatment
     , offer
     , count(*)
from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE mma
where sales_campaign_id = $campaignid
group by all
order by 1;



-- =================================================================
-- Flagging the treatment field as needed.
-- IMPORTANT: if you have a suppression test split, that NEEDS to be flagged in TREATMENT to make it into the final Historicals table for anaylsis!
--Treatments:

--Lvl 4: Boss Mode (Fool Port) -> Report Only
--Lvl 3: MoneyMakers (Epic+) -> Boss
--Lvl 2: Epic -> MoneyMakers (Epic+)
--Lvl 1: SA -> Epic
-- =================================================================

--===========================================
--Tag all Boss Mode members
--===========================================
create or replace temp table bm_owners as
select distinct sub.uid
from raw.fool_commerce.dbo_product p
         join RAW.FOOL_COMMERCE.DBO_SUBSCRIPTION sub
              on p.PRODUCTID = sub.PRODUCTID
         join OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE mma
              on mma.uid = sub.uid
where SUBSCRIPTIONSTATUSTYPE = 1
  and p.PRODUCTID = 4155
  and mma.SALES_CAMPAIGN_ID = $campaignid;

UPDATE OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
set TREATMENT='(4) Level_4: Fool Portfolios Members'
  , offer='Report_Only_Campaign'
from bm_owners b
where p.SALES_CAMPAIGN_ID = $campaignid
  and p.uid = b.uid
  and TREATMENT is null
  and CAN_MAIL = 1;

--===========================================
--Tag Lvl 3 members
--Put in Fool Portfolio
--===========================================
create or replace temp table mm_owners as
select distinct sub.uid
from raw.fool_commerce.dbo_product p
         join RAW.FOOL_COMMERCE.DBO_SUBSCRIPTION sub
              on p.PRODUCTID = sub.PRODUCTID
         join OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE mma
              on mma.uid = sub.uid
where SUBSCRIPTIONSTATUSTYPE = 1
  and p.PRODUCTID = 4094
  and mma.SALES_CAMPAIGN_ID = $campaignid;

UPDATE OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
set TREATMENT='(3) Level_3: Epic Plus Members'
  , offer='Fool_Portfolios_Campaign'
from mm_owners b
where p.SALES_CAMPAIGN_ID = $campaignid
  and p.uid = b.uid
  and TREATMENT is null
  and CAN_MAIL = 1;

--===========================================
--Tag Epic members
--Put in MM
--===========================================
create or replace temp table eb_owners as
select distinct sub.uid
from raw.fool_commerce.dbo_product p
         join RAW.FOOL_COMMERCE.DBO_SUBSCRIPTION sub
              on p.PRODUCTID = sub.PRODUCTID
         join OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE mma
              on mma.uid = sub.uid
where SUBSCRIPTIONSTATUSTYPE = 1
  and p.PRODUCTID = 4299
  and mma.SALES_CAMPAIGN_ID = $campaignid;

/*
--check that they only have 1 prod
select b.uid
     , count(distinct sub.PRODUCTID) prods
from eb_owners b
         join RAW.FOOL_COMMERCE.DBO_SUBSCRIPTION sub
              on b.uid = sub.uid
where SUBSCRIPTIONSTATUSTYPE = 1
group by 1
having count(distinct sub.PRODUCTID) > 1
 */


UPDATE OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
set TREATMENT='(2) Level_2: Epic Members'
  , offer='Epic_Plus_Campaign'
from eb_owners b
where p.SALES_CAMPAIGN_ID = $campaignid
  and p.uid = b.uid
  and TREATMENT is null
  and CAN_MAIL = 1;

--===========================================
--Tag SA members
--Put in Epic
--===========================================
create or replace temp table sa_owners as
select distinct sub.uid
from raw.fool_commerce.dbo_product p
         join RAW.FOOL_COMMERCE.DBO_SUBSCRIPTION sub
              on p.PRODUCTID = sub.PRODUCTID
         join OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE mma
              on mma.uid = sub.uid
where SUBSCRIPTIONSTATUSTYPE = 1
  and p.PRODUCTID = 1081
  and mma.SALES_CAMPAIGN_ID = $campaignid;


UPDATE OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
set TREATMENT='(1) Level_1: SA Members'
  , offer='Epic_Campaign'
from sa_owners b
where p.SALES_CAMPAIGN_ID = $campaignid
  and p.uid = b.uid
  and TREATMENT is null
  and CAN_MAIL = 1;

--=================================
--Set 10% unengaged to Epic offer
--=================================
UPDATE OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
set offer='Epic_Campaign'
where p.TREATMENT = '(1) Level 1: Unengaged_10PctInCampaign'
  and p.SALES_CAMPAIGN_ID = $campaignid
  and CAN_MAIL = 1;

--=================================
--Set no sub members to epic
--=================================
UPDATE OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
set offer='Epic_Campaign'
  , TREATMENT='(0) Level_0: Formers'
  , CAMPAIGN_SPECIFIC_DATA = OBJECT_INSERT(p.CAMPAIGN_SPECIFIC_DATA, 'No_Subs', 1, false)
where p.TREATMENT is null
  and p.SALES_CAMPAIGN_ID = $campaignid
  and CAN_MAIL = 1;

--===================================
--HOP Header Test
--split 50/50
--50% get header
--===================================
create or replace temp table split as
select uid, row_number() over (order by uuid_string()) % 2 as r
from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE
where SALES_CAMPAIGN_ID = $campaignid
  and CAN_MAIL = 1;


UPDATE OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
set CAMPAIGN_SPECIFIC_DATA = OBJECT_INSERT(p.CAMPAIGN_SPECIFIC_DATA, 'HOP', 'Test_HOP', false)
--iff(subq.r = 0, 'Epic_Campaign', 'SA_Campaign')
from split s
where p.uid = s.uid
  and p.SALES_CAMPAIGN_ID = $campaignid
  and p.CAN_MAIL = 1
  and s.r = 0;

UPDATE OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
set CAMPAIGN_SPECIFIC_DATA = OBJECT_INSERT(p.CAMPAIGN_SPECIFIC_DATA, 'HOP', 'Control_HOP', false)
--iff(subq.r = 0, 'Epic_Campaign', 'SA_Campaign')
from split s
where p.uid = s.uid
  and p.SALES_CAMPAIGN_ID = $campaignid
  and p.CAN_MAIL = 1
  and s.r = 1;

-- gut check3
select can_mail
     , treatment
     , offer
     , CAMPAIGN_SPECIFIC_DATA:HOP
     , count(*)
from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE mma
where sales_campaign_id = $campaignid
  and CAN_MAIL = 1
group by all
order by 1;


--======================================================
--Supercreditors
--======================================================
create or replace temp table credits as
with all_credits_sp as
         (SELECT s.uid,
                 s.productid,
                 productname,
                 s.subscriptionid,
                 sp.subscriptionperiodid,
                 sp.startdate,
                 sp.enddate,
                 SUM(CASE
                         WHEN ati.AccountingTransactionType IN (2, 3) THEN amount
                         WHEN ati.AccountingTransactionType IN (4, 8) THEN -1 * ati.Amount end) AS total_amount,
                 DATEDIFF(dd, sp.StartDate, sp.EndDate)                                         AS total_days,

                 total_amount / total_days                                                      as amount_per_day,
                 datediff(dd, greatest(sp.startdate, current_date), sp.EndDate)                 as days_left,
                 amount_per_day * days_left                                                     as amount_left
          FROM raw.fool_commerce.dbo_subscription s
                   JOIN raw.fool_commerce.dbo_subscriptionperiod sp
                        ON s.subscriptionid = sp.subscriptionid
                   JOIN raw.fool_commerce.dbo_product p
                        ON s.productid = p.productid
                   JOIN raw.fool_commerce.dbo_productitem pri
                        ON sp.subscriptionperiodid = pri.productitemid
                   JOIN raw.fool_commerce.dbo_InvoiceItem ii
                        ON pri.productitemid = ii.productitemid
                   JOIN raw.fool_commerce.dbo_accountingtransactionitem ati
                        ON ati.ParentTransactionItemId = ii.InvoiceItemId
               -- join OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE aud
               --      on aud.uid = s.UID
               --          and aud.SALES_CAMPAIGN_ID = 2244
          WHERE s.SubscriptionStatusType = 1
            AND pri.IsActive = 1
            AND s.EndDate > '2024-08-13'
            AND sp.StartDate <> sp.EndDate
            AND sp.EndDate > '2024-08-13'
            --and aud.CAN_MAIL = 1
          GROUP BY DATEDIFF(dd, sp.StartDate, sp.EndDate),
                   DATEDIFF(dd, '2024-08-13', sp.EndDate),
                   s.Uid,
                   s.ProductId,
                   p.ProductName,
                   s.SubscriptionId,
                   sp.SubscriptionPeriodId,
                   sp.StartDate,
                   sp.EndDate)
SELECT ac.uid,
       pc.productid,
       p.productname,
       SUM(ac.amount_left) AS credits
FROM all_credits_sp ac
         JOIN raw.fool_commerce.dbo_ProductComponent pc
              ON ac.ProductId = pc.ComponentProductId
         JOIN raw.fool_commerce.dbo_Product p
              ON p.ProductId = pc.ProductId
         join OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE mma
              on mma.uid = ac.uid
                  and mma.CAN_MAIL = 1
                  and mma.SALES_CAMPAIGN_ID = $campaignid
--and mma.TREATMENT='Level_3'
WHERE ac.ProductId <> p.ProductId
  AND p.IsActive = 1
  and p.PRODUCTID = 1255--MFOne
  --and p.PRODUCTID=4299 --EB
  --AND p.ProductId =4094--MM
  -- AND p.ProductId in (4155, 4094, 4299,1255)--BM
--and mma.uid=2047499488
GROUP BY ac.Uid,
         pc.ProductId,
         p.ProductName;


--Insert  Credits
update OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
set CAMPAIGN_SPECIFIC_DATA = OBJECT_INSERT(p.CAMPAIGN_SPECIFIC_DATA, 'Credits', c.credits, false)
from credits c
where p.UID = c.uid
--  and c.productid = 4155
  and p.CAN_MAIL = 1
  and p.SALES_CAMPAIGN_ID = $campaignid;


--Hold out members with >=2993 credits
update OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
set AD_HOC_SUPPRESSIONS = OBJECT_INSERT(p.AD_HOC_SUPPRESSIONS, 'Supercreditors', 1, false)
  , CAN_MAIL            = 0
where p.SALES_CAMPAIGN_ID = $campaignid
  and CAN_MAIL = 1
  and p.campaign_specific_data:Credits >= 2993;


--Move L2 members with > 1298 creds to L4 offer
update OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
set offer='Fool_Portfolios_Campaign'
  , CAMPAIGN_SPECIFIC_DATA = OBJECT_INSERT(p.CAMPAIGN_SPECIFIC_DATA, 'L2_Supercreditors', 1, false)
where p.SALES_CAMPAIGN_ID = $campaignid
  and CAN_MAIL = 1
  and p.TREATMENT = '(2) Level_2: Epic Members'
  and p.campaign_specific_data:Credits >= 1298;

--Move L1 members with >= 355 creds to L3 offer
update OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
set offer='Epic_Plus_Campaign'
  , CAMPAIGN_SPECIFIC_DATA = OBJECT_INSERT(p.CAMPAIGN_SPECIFIC_DATA, 'L1_Supercreditors', 1, false)
where p.SALES_CAMPAIGN_ID = $campaignid
  and CAN_MAIL = 1
  and p.TREATMENT like any ('%Level_1%', '%Level_0%')
  and p.campaign_specific_data:Credits >= 355;


--=======================================
--Take Small Caps AI buyers out of L4
--=======================================
create or replace temporary table small as
    select uid
from PROCESSED.BE_CAMPAIGN.V_BE_ORDERS_SIMPLE bos
where CAMPAIGN_YEAR_MONTH='SmallCapsAIJul24'
and ORDERS>0;

update OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
set CAN_MAIL=0
,AD_HOC_SUPPRESSIONS = OBJECT_INSERT(p.AD_HOC_SUPPRESSIONS, 'SmallCapsJul24_Buyer', 1, false)
from small s
where p.SALES_CAMPAIGN_ID = $campaignid
  and p.UID = s.uid
  and p.TREATMENT='(4) Level_4: Fool Portfolios Members'
  and CAN_MAIL = 1;

-- gut check4
select can_mail
     , treatment
     -- , offer
     , count(*)
from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE mma
where sales_campaign_id = $campaignid
  AND CAN_MAIL = 1
group by all
order by 1;

-- =================================================================
-- Update the IID field for the campaign
-- =================================================================
-- now get the IIDs in there

update OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE
set IID = $campaigniid -- this (probably) got set way up top
where SALES_CAMPAIGN_ID = $campaignid
  and treatment is not null
  and can_mail = true;

-- =================================================================
-- Q&A
-- =================================================================
-- gut check
select can_mail
     , treatment
     , offer
     , count(*)
from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE
where sales_campaign_id = $campaignid
  and CAN_MAIL = 1
group by all
order by 1;

select
     --CAN_MAIL
     --, p.TREATMENT
    offer
     , iid
     , count(distinct uid)
from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
where SALES_CAMPAIGN_ID = $campaignid
      --and CAN_MAIL = 1
group by all
order by 1;

Select *
from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE
where SALES_CAMPAIGN_ID = $campaignid
limit 199
    and can_mail = 1 and treatment is not null;

Select count(uid), treatment, offer
from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE
where SALES_CAMPAIGN_ID = $campaignid
      --and can_mail = 1
      --and treatment is not null
group by treatment, offer;



select ad_hoc_suppressions:"JoinedLast8-14Days", can_mail, treatment, count(*)
from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE
where SALES_CAMPAIGN_ID = $campaignid
group by all
order by 1;


-----------------------------------------------------------------
-- Pulling the Braze AID CSVs
-- Send CSV to #msg-tech-help
-----------------------------------------------------------------
Select lower(ACCOUNT_GUID) "external_id", 10979 as aid
from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE aj
         join PROCESSED.SHARED.CUSTOMER cus
              on aj.uid = cus.UID
where aj.SALES_CAMPAIGN_ID = $campaignid
  and aj.can_mail = 1
  and OFFER = 'Fool_Portfolios_Campaign'
union
Select lower(ACCOUNT_GUID) "external_id", 10980 as aid
from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE aj
         join PROCESSED.SHARED.CUSTOMER cus
              on aj.uid = cus.UID
where aj.SALES_CAMPAIGN_ID = $campaignid
  and aj.can_mail = 1
  and aj.OFFER = 'Epic_Plus_Campaign'
union
Select lower(ACCOUNT_GUID) "external_id", 10981 as aid
from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE aj
         join PROCESSED.SHARED.CUSTOMER cus
              on aj.uid = cus.UID
where aj.SALES_CAMPAIGN_ID = $campaignid
  and aj.can_mail = 1
  and aj.OFFER = 'Epic_Campaign'
;
--L4
Select lower(ACCOUNT_GUID) "external_id", 10985 as aid
from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE aj
         join PROCESSED.SHARED.CUSTOMER cus
              on aj.uid = cus.UID
where aj.SALES_CAMPAIGN_ID = $campaignid
  and aj.can_mail = 1
  and aj.OFFER = 'Report_Only_Campaign'
union
Select lower(ACCOUNT_GUID) "external_id", 10986 as aid
from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE aj
         join PROCESSED.SHARED.CUSTOMER cus
              on aj.uid = cus.UID
where aj.SALES_CAMPAIGN_ID = $campaignid
  and aj.can_mail = 1
  and aj.PRODUCT_ACCESS:"4155" = 1
   and offer= 'Fool_Portfolios_Campaign'
union
Select lower(ACCOUNT_GUID) "external_id", 10986 as aid
from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE aj
         join PROCESSED.SHARED.CUSTOMER cus
              on aj.uid = cus.UID
where aj.SALES_CAMPAIGN_ID = $campaignid
  and aj.can_mail = 1
   and aj.PRODUCT_ACCESS:"4094" = 1
   and offer= 'Epic_Plus_Campaign'
union
Select lower(ACCOUNT_GUID) "external_id", 10986 as aid
from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE aj
         join PROCESSED.SHARED.CUSTOMER cus
              on aj.uid = cus.UID
where aj.SALES_CAMPAIGN_ID = $campaignid
  and aj.can_mail = 1
   and aj.PRODUCT_ACCESS:"4299" = 1
   and offer= 'Epic_Campaign'

--=========================================================
-- Hotkeys
--=========================================================
--L2 Total
with hotkey as
         (select uid
               , 'SW-Aug24-L2'                        as key
               , 1                                    as value
               , row_number() over (order by uid) % 2 as test_group
          from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE
          where SALES_CAMPAIGN_ID = $campaignid
            and OFFER = 'Epic_Campaign'
            and CAN_MAIL = 1
         )
select uid
     , key
     , value
--,test_group
--,count(uid)
from hotkey
--group by 1
--where uid=2044014621
where test_group = 1;

--L2 Test
select aj.uid
     , 'SW-Aug24-L2Test' as key
     , 1                 as value
     , lower(ACCOUNT_GUID)  "external_id"

from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE aj
         join PROCESSED.SHARED.CUSTOMER cus
              on aj.uid = cus.UID
where SALES_CAMPAIGN_ID = $campaignid
  and CAN_MAIL = 1
  and offer = 'Epic_Campaign'
  and CAMPAIGN_SPECIFIC_DATA:HOP = 'Test_HOP'

--L2 Control
select aj.uid
     , 'SW-Aug24-L2Ctrl' as key
     , 1                 as value
     , lower(ACCOUNT_GUID)  "external_id"

from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE aj
         join PROCESSED.SHARED.CUSTOMER cus
              on aj.uid = cus.UID
where SALES_CAMPAIGN_ID = $campaignid
  and CAN_MAIL = 1
  and offer = 'Epic_Campaign'
  and CAMPAIGN_SPECIFIC_DATA:HOP = 'Control_HOP'


--L3 Test
select aj.uid
     , 'SW-Aug24-L3Test' as key
     , 1                 as value
     , lower(ACCOUNT_GUID)  "external_id"

from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE aj
         join PROCESSED.SHARED.CUSTOMER cus
              on aj.uid = cus.UID

where SALES_CAMPAIGN_ID = $campaignid
  and CAN_MAIL = 1
  and offer = 'Epic_Plus_Campaign'
  and CAMPAIGN_SPECIFIC_DATA:HOP = 'Test_HOP'
union
--L3 Control
select aj.uid
     , 'SW-Aug24-L3Ctrl' as key
     , 1                 as value
     , lower(ACCOUNT_GUID)  "external_id"
from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE aj
         join PROCESSED.SHARED.CUSTOMER cus
              on aj.uid = cus.UID
where SALES_CAMPAIGN_ID = $campaignid
  and CAN_MAIL = 1
  and offer = 'Epic_Plus_Campaign'
  and CAMPAIGN_SPECIFIC_DATA:HOP = 'Control_HOP'

--L3 Total
select uid
     , 'SW-Aug24-L3' as key
     , 1             as value
from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE
where SALES_CAMPAIGN_ID = $campaignid
  and CAN_MAIL = 1
  and offer = 'Epic_Plus_Campaign'


--L4
select aj.uid
     , 'SW-Aug24-L4' as key
     , 1                 as value
from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE aj
where SALES_CAMPAIGN_ID = $campaignid
  and CAN_MAIL = 1
  and offer = 'Report_Only_Campaign';


--==================================
--MS Comments
--===================================
Select aj.uid
from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE aj
where aj.SALES_CAMPAIGN_ID = $campaignid
  and aj.can_mail = 1
  and OFFER = 'MP_Campaign'
  and TREATMENT = 'Epic_Member'

select bos.PRODUCT_NAME
     , bos.DESCRIPTION
     , sum(bos.ORDERS) orders
     , sum(bos.CASH)   cash
from REPORTING.ACQUISITION.FEB_2024_VDAY_COMP_TEST_ISSUED_COMPS s
         join OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE mma
              on mma.UID = s.uid
                  and mma.SALES_CAMPAIGN_ID = 2268
                  and mma.CAN_MAIL = 1
         join PROCESSED.SHARED.CUSTOMER cus
              on mma.uid = cus.UID
                  and offer = 'Epic_Campaign'
         join PROCESSED.BE_CAMPAIGN.V_BE_ORDERS_SIMPLE bos
              on bos.uid = s.UID
                  and bos.CAMPAIGN_YEAR_MONTH = 'EnergyMay24'
group by 1, 2


with mems as
    (select uid
     from --raw.fool_commerce.dbo_subscription
          OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE mma
     where mma.SALES_CAMPAIGN_ID = $campaignid
       and CAN_MAIL = 1
       and TREATMENT = 'Level_0')

   , max_sub as
    (select m.uid
          , max(s.DATEMODIFIED) enddate
     from raw.fool_commerce.dbo_subscription s
              join mems m
                   on m.uid = s.UID
          --       and m.uid = 2036832022
          --2054520330
     group by 1)
select count(distinct ms.UID) uids
     , p.PRODUCTNAME
from max_sub ms
         join raw.fool_commerce.dbo_subscription s
              on ms.enddate = s.DATEMODIFIED
                  and ms.uid = s.uid
         join raw.FOOL_COMMERCE.DBO_PRODUCT p
              on p.PRODUCTID = s.PRODUCTID
group by 2
