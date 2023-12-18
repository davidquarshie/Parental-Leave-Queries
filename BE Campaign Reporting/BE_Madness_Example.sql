USE SCHEMA OPERATIONAL.EMAIL_OPS;
use role EMAIL_OPS;

set campaignid = 2245;-- {{ params.campaign_id }};
set campaigniid = 333455726;
set maincampaigntreatment = 'VH_Campaign';

-- For the smiley suppressions the two most recent campaigns we want to use for holdouts
set mostrecentcampaignacronym = 'MPDec23';
set secondmostrecentcampaignacronym = 'MPNov23';

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

--  Given that the campaign being served and the IID can change depending on each campaign
--  this will be updated regularly
-- IMPORTANT! Don't forget to update the IIDs at the bottom of the script!
-- clear the decks
update OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE
set CAMPAIGN_SPECIFIC_DATA = null
where CAMPAIGN_SPECIFIC_DATA is not null
  and SALES_CAMPAIGN_ID = $campaignid;

update OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE
set AD_HOC_SUPPRESSIONS = null
where AD_HOC_SUPPRESSIONS is not null
  and SALES_CAMPAIGN_ID = $campaignid;

-- GT, 2023-01-11: So it turns out that OBJECT_INSERT doesn't work unless the variant field already has JSON
-- in it. E.g., OBJECT_INSERT into a NULL variant field does nothing at all. We need to initialize the field with
-- empty JSON and then can do inserts afterward
update OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
SET CAMPAIGN_SPECIFIC_DATA = to_variant(parse_json('{}'))
where p.SALES_CAMPAIGN_ID = $campaignid;

update OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
SET AD_HOC_SUPPRESSIONS = to_variant(parse_json('{}'))
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

-- =================================================================
-- Campaign-specific logic/holdouts begins here
-- =================================================================
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
select sit.uid, count(ur.URL_ID) as visits
from PROCESSED.WEB.SITEUSAGE sit
         join PROCESSED.WEB.URL ur on ur.URL_ID = sit.URL_ID
where ur.CLEAN_URL ilike 'https://www.fool.com/premium%'
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

-- Hold out 90% of single FE members (excluding Epic) who haven’t clicked any emails in the last 90 days and have had no premium site visits last 60 days
--Ignore this suppression for any member that started their subscription in the last 6 months
UPDATE OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
set treatment= iff(subq.r = 0, 'Unengaged_10PctInCampaign', 'Unengaged_90PctHeldOut')
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

-- Suppress any members that have clicked on the negative smiley during the previous 2 campaigns (NGS and VH)
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

-- =================================================================
-- Flagging the treatment field as needed.
-- IMPORTANT: if you have a suppression test split, that NEEDS to be flagged in TREATMENT to make it into the final Historicals table for anaylsis!
-- =================================================================
-- Holdout anyone that owns these products
UPDATE OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
set AD_HOC_SUPPRESSIONS = OBJECT_INSERT(p.AD_HOC_SUPPRESSIONS, 'Owns_Holdout_Product', 1, false)
  , CAN_MAIL            = 0
where p.SALES_CAMPAIGN_ID = $campaignid
  and p.PRODUCT_ACCESS:"4468" = 1;  /*Value Hunters*/


-- Set Single FE treatment
UPDATE OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
set treatment= 'Single_FE_Engaged'
from (
         select uid
         from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE
         where SALES_CAMPAIGN_ID = 2245
           and PRODUCT_ACCESS:"4468" = 0
           and can_mail = 1
           and CAMPAIGN_SPECIFIC_DATA:Single_FE_Member = 1
           and treatment is null
     ) subq
where p.uid = subq.uid
  and p.SALES_CAMPAIGN_ID = 2245;

--Set Non Single FE Treatmemt
UPDATE OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
set treatment= 'Non_Single_FE'
where p.SALES_CAMPAIGN_ID = 2245
and TREATMENT is null
and CAN_MAIL=1;

-- =================================================================
-- Make Offers
--50% IU on HOP
--50% IU on next page
-- =================================================================
set campaignid = 2245;
UPDATE OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
set offer = iff(subq.r = 0, '50Pct_IU_On_HOP',
                    '50Pct_IU_Next_Page')
from (
         select uid, row_number() over (order by uuid_string()) % 2 as r
         from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE
         where SALES_CAMPAIGN_ID = $campaignid
           and can_mail = 1
     ) subq
where p.uid = subq.uid
  and p.SALES_CAMPAIGN_ID = $campaignid
and CAN_MAIL=1;


-- =================================================================
-- Update the IID field for the campaign
-- =================================================================
-- now get the IIDs in there
update OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE
set IID = 333455726
where SALES_CAMPAIGN_ID = 2245
  and treatment is not null
  and can_mail = 1;


-- =================================================================
-- Q&A
-- =================================================================
Select count(uid)
from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE
where SALES_CAMPAIGN_ID = 2245
  and can_mail = 1
  and treatment is not null;

Select count(uid), treatment, offer, can_mail
from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE
where SALES_CAMPAIGN_ID = 2245
 -- and can_mail = 1
 -- and treatment is not null
group by treatment, offer, can_mail;


--=========================================================
-- Pulling the Braze AID CSVs
-- Send CSV to #msg-tech-help
--=========================================================
Select lower(ACCOUNT_GUID) as "external_id", 10863 as aid
from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE aj
         join PROCESSED.SHARED.CUSTOMER cus
              on aj.uid = cus.UID
where aj.SALES_CAMPAIGN_ID = 2245
  and aj.can_mail = 1
  and aj.offer in ('50Pct_IU_On_HOP')
union
Select lower(ACCOUNT_GUID) as "external_id", 10864 as aid
from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE aj
         join PROCESSED.SHARED.CUSTOMER cus
              on aj.uid = cus.UID
where aj.SALES_CAMPAIGN_ID = 2245
  and aj.can_mail = 1
  and aj.offer in ('50Pct_IU_Next_Page')
--===========================================================================
--Hotkeys
--===========================================================================
select uid
     , 'VH-Dec23-IU' as key
     , 1                 as value
from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE
where SALES_CAMPAIGN_ID = 2245
  and offer ='50Pct_IU_Next_Page'


select uid
     , 'Bio-Sept23-BioTech' as key
     , 1                    as value
from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE
where SALES_CAMPAIGN_ID = 2245
  and treatment in ('BioTech_Campaign', 'SingleFE_10Pct_BioTech_Campaign')


select TREATMENT
,       ae.RELATIONSHIP_TYPE
,       count(p.uid)
from PROCESSED.BE_CAMPAIGN.AUDIENCE_COMBINED ae
         join OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
              on ae.uid = p.uid
where ae.CAMPAIGN = 'MPOct23'
  and p.SALES_CAMPAIGN_ID = 2245
  and p.CAN_MAIL = 1
 group by 1,2