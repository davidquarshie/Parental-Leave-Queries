USE SCHEMA OPERATIONAL.EMAIL_OPS;
use role EMAIL_OPS;

-- TODO: edit this for every new Ascent mailing
set campaignid = 2280;

-- Flag the mailables
--use role EMAIL_OPS;
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
     ) subq
where subq.uid = p.uid
  and p.SALES_CAMPAIGN_ID = $campaignid;


-- In the event that you need to reset CAMPAIGN_SPECIFIC_DATA...
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
create or replace temporary table SANDBOX.DQUARSHIE.products as
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

create or replace temporary table SANDBOX.DQUARSHIE.single_FE_members as
select sub.UID
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
         join SANDBOX.DQUARSHIE.products p on sub.PRODUCTID = p.PRODUCTID
    and p.ISBACKEND = 0
    and p.PRODUCTID <> 4299; -- Epic Bundle, which doesn't count for these purposes

UPDATE OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
set CAMPAIGN_SPECIFIC_DATA = OBJECT_INSERT(p.CAMPAIGN_SPECIFIC_DATA, 'Single_FE_Member', 1, false)
from (
         select pool.uid
         from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE pool
                  join SANDBOX.DQUARSHIE.single_FE_members s on pool.uid = s.UID
             and pool.SALES_CAMPAIGN_ID = $campaignid
     ) z
where p.uid = z.uid
  and p.SALES_CAMPAIGN_ID = $campaignid;

-- =================================================================
-- Campaign-specific logic/holdouts begins here
-- =================================================================
-- Hold out anyone that IS NOT a single FE member
UPDATE OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
set AD_HOC_SUPPRESSIONS = OBJECT_INSERT(p.AD_HOC_SUPPRESSIONS, 'NotSingleFE', 1, false)
  , CAN_MAIL            = 0 -- this is a suppression; make sure we mark as unmailable
from (
         select pool.uid
         from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE pool
         where pool.CAMPAIGN_SPECIFIC_DATA:Single_FE_Member = 0
     ) subq
where p.uid = subq.uid
  and p.SALES_CAMPAIGN_ID = $campaignid;

/*--Note (5/10/2024):Hubspot filtering is done in braze now
-- Holdout anyone that opted out via Hubspot
UPDATE OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
set AD_HOC_SUPPRESSIONS = OBJECT_INSERT(p.AD_HOC_SUPPRESSIONS, 'HubspotOptOut', 1, false)
  , CAN_MAIL            = 0
from (
         SELECT pe.UID
,         PROPERTY_NEVERMAIL
         ,PROPERTY_HS_EMAIL_OPTOUT
         FROM RAW.HUBSPOT_ASCENT.OBJECTS_CONTACTS hub
              --FIVETRAN.HUBSPOT_ASCENT.CONTACT hub
                  join raw.FOOL_PARTY.DBO_EMAIL ema
                       on hub.PROPERTY_EMAIL = ema.EMAIL
                  join raw.FOOL_PARTY.DBO_PARTYEMAIL pe
                       on ema.EMAILID = pe.EMAILID
         WHERE hub.PROPERTY_NEVERMAIL IS NOT NULL
            OR hub.PROPERTY_HS_EMAIL_OPTOUT IS NOT NULL
         GROUP BY pe.uid
     ) subq
where p.uid = subq.uid
  and p.SALES_CAMPAIGN_ID = $campaignid;

select min(UPDATEDAT)
from RAW.HUBSPOT_ASCENT.OBJECTS_CONTACTS hub
limit 100
 */
-- Holdout anyone that opted out via IID from these campaigns
UPDATE OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
set AD_HOC_SUPPRESSIONS = OBJECT_INSERT(p.AD_HOC_SUPPRESSIONS, 'IIDOptOut', 1, false)
  , CAN_MAIL            = 0
from (
         SELECT aj.uid
         from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE aj
                  join raw.APRIMODIALOGUE.DBO_OPTOUT oo
                       on aj.uid = oo.UID
         where oo.IID = 205496260
         group by aj.uid
     ) subq
where p.uid = subq.uid
  and p.SALES_CAMPAIGN_ID = $campaignid;

with send as
         (select p.uid
               , sum(case when ea.UID is not null then 1 else 0 end) sends
          from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
                   left join processed.email.email_activity ea
                             on p.UID = ea.UID
                   left join processed.email.issue i
                             on ea.issue_id = i.issue_id
                                 and i.ISSUE_NAME ilike '%ascent%'
                                 and i.ISSUE_NAME like 'BE-%'
          where SALES_CAMPAIGN_ID = $campaignid

            and CAN_MAIL = 1
          group by 1)
select *
from send
where sends = 0
--===============================================================
--Previous Ascent Email Sends
--===============================================================
UPDATE OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
set CAMPAIGN_SPECIFIC_DATA = OBJECT_INSERT(p.CAMPAIGN_SPECIFIC_DATA, 'Ascent_Sends', '2+', false)
from (select p.uid
      from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
               join processed.email.email_activity ea
                    on p.UID = ea.UID
               join processed.email.issue i
                    on ea.issue_id = i.issue_id
      where SALES_CAMPAIGN_ID = $campaignid
        and i.ISSUE_NAME ilike '%ascent%'
        and i.ISSUE_NAME like 'BE-%'
        and CAN_MAIL = 1
      group by 1) subq
where subq.UID = p.uid
  and p.SALES_CAMPAIGN_ID = $campaignid;


UPDATE OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
set CAMPAIGN_SPECIFIC_DATA = OBJECT_INSERT(p.CAMPAIGN_SPECIFIC_DATA, 'Ascent_Sends', '1st', true)
from (select uid
      from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE
      where CAN_MAIL = 1
        and SALES_CAMPAIGN_ID = $campaignid
        and CAMPAIGN_SPECIFIC_DATA:Ascent_Sends is null) subq
where subq.uid = p.uid
  and p.SALES_CAMPAIGN_ID = $campaignid;

--==============================================================
--New rule (4/19)
--Mail anyone that has had 0 or 1 Ascent Send
--==============================================================
--Get Count of Ascent Sends
UPDATE OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
set CAMPAIGN_SPECIFIC_DATA = OBJECT_INSERT(p.CAMPAIGN_SPECIFIC_DATA, 'Num_of_Ascent_Sends', sends, false)
from (select p.uid
           , count(distinct i.ISSUE_NAME) sends
      from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
               join processed.email.email_activity ea
                    on p.UID = ea.UID
               join processed.email.issue i
                    on ea.issue_id = i.issue_id
      where SALES_CAMPAIGN_ID = $campaignid
        and i.ISSUE_NAME ilike '%ascent%'
        and i.ISSUE_NAME like 'BE-%'
        and CAN_MAIL = 1
      group by 1) subq
where subq.UID = p.uid
  and p.SALES_CAMPAIGN_ID = $campaignid;

UPDATE OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
set CAMPAIGN_SPECIFIC_DATA = OBJECT_INSERT(p.CAMPAIGN_SPECIFIC_DATA, 'Num_of_Ascent_Sends', 0, false)
where SALES_CAMPAIGN_ID = $campaignid
  and CAMPAIGN_SPECIFIC_DATA:Num_of_Ascent_Sends is null
  and CAN_MAIL = 1

--Check
select uid
     , CAMPAIGN_SPECIFIC_DATA:Num_of_Ascent_Sends
from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
where CAN_MAIL = 1
  and SALES_CAMPAIGN_ID = $campaignid
  and CAMPAIGN_SPECIFIC_DATA:Num_of_Ascent_Sends in (0, 1);


--===============================================================
--New Model Rules
--All 1st and 2nd Ascent Mail Receivers Get mail
--===============================================================
update OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
set TREATMENT ='1st Ascent Mail'
from (select uid
      from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE
      where CAN_MAIL = 1
        and SALES_CAMPAIGN_ID = $campaignid
        and CAMPAIGN_SPECIFIC_DATA:Num_of_Ascent_Sends = 0) subq
where subq.uid = p.uid
  and p.SALES_CAMPAIGN_ID = $campaignid;

update OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
set TREATMENT ='2nd Ascent Mail'
from (select uid
      from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE
      where CAN_MAIL = 1
        and SALES_CAMPAIGN_ID = $campaignid
        and CAMPAIGN_SPECIFIC_DATA:Num_of_Ascent_Sends = 1) subq
where subq.uid = p.uid
  and p.SALES_CAMPAIGN_ID = $campaignid;

--Check
select TREATMENT
     , count(uid)
from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
where CAN_MAIL = 1
  and SALES_CAMPAIGN_ID = $campaignid
group by 1;


--===============================================================
--New Model Rules
--Only For 2+ Ascent Mail Receivers
--Top 40% get mail
--10% of the bottom 60% get mail
--===============================================================
set max_time_stamp_utc = (select max(TIMESTAMP_UTC)
                          from models.output.ascent_click_prediction);


--get >= 40 percentile of new model
update OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
set TREATMENT ='Top_40Prct_GettingMail'
from (select uid
      from (select aj.uid
                 , ss.VALUE
                 , ntile(10) over (order by ss.VALUE desc) as tile
            FROM OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE aj
                     join models.output.ascent_click_prediction ss
                          on aj.uid = ss.UID
            where ss.TIMESTAMP_UTC = $max_time_stamp_utc
              and aj.SALES_CAMPAIGN_ID = $campaignid
              and aj.can_mail = 1
              and aj.TREATMENT is null)
      where tile <= 4) subq
where p.uid = subq.uid
  and p.SALES_CAMPAIGN_ID = $campaignid;


--Get 10% of the bottom 60%
UPDATE OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
set treatment= iff(subq.r = 0, 'Bottom_60Pct_GettingMail', 'Bottom_60Pct_HeldOut')
from (select uid
           , row_number() over (order by uuid_string()) % 10 as r
      from (select aj.uid
                 , ss.VALUE
                 , ntile(10) over (order by ss.VALUE desc) as tile
            FROM OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE aj
                     join models.output.ascent_click_prediction ss
                          on aj.uid = ss.UID
            where ss.TIMESTAMP_UTC = $max_time_stamp_utc
              and aj.SALES_CAMPAIGN_ID = $campaignid
              and aj.can_mail = 1
              and aj.TREATMENT is null)
      where tile > 4) subq
where p.uid = subq.uid
  and p.SALES_CAMPAIGN_ID = $campaignid;


--Set holdout group to can mail =0
UPDATE OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
set CAN_MAIL=0
where TREATMENT = 'Bottom_60Pct_HeldOut'
  and p.SALES_CAMPAIGN_ID = $campaignid;


--Add Scores to the table
UPDATE OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
set CAMPAIGN_SPECIFIC_DATA = OBJECT_INSERT(p.CAMPAIGN_SPECIFIC_DATA, 'Model_Score', z.score, false)
from (
         select aj.uid, ss.value as score
         FROM OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE aj
                  join models.output.ascent_click_prediction ss
                       on aj.uid = ss.UID
         where ss.TIMESTAMP_UTC = $max_time_stamp_utc-- change date
           and aj.SALES_CAMPAIGN_ID = $campaignid
     ) z
where p.uid = z.uid
  and p.SALES_CAMPAIGN_ID = $campaignid;

--Check
select CAN_MAIL
     , TREATMENT
     , count(uid)
from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
where
    --CAN_MAIL = 1
    SALES_CAMPAIGN_ID = $campaignid
group by 1,2;

--=================================================================
--Set null treatments to can mail =0
--=================================================================
update OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE
set can_mail = 0
where treatment is null
  and can_mail = 1
  and SALES_CAMPAIGN_ID = $campaignid;

-- =================================================================
-- Flagging the treatment field as needed.
-- =================================================================
-- GT: on the fence about making this live code or leaving in this comment. It's good for re-setting, but
-- that can be disastrous if things are in flight. Leaving as a comment for now, and def side-eyeing
/*
    update  OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE
    set     treatment = null
    where   SALES_CAMPAIGN_ID = $campaignid;

-- Unscored folks are held out of campaign emails
update OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE m
set treatment           = 'Unscored- No Mail'
  , AD_HOC_SUPPRESSIONS = OBJECT_INSERT(m.AD_HOC_SUPPRESSIONS, 'UnscoredFolks', 1, false)
  , CAN_MAIL            = 0
from (
         select uid
         from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE
         where SALES_CAMPAIGN_ID = $campaignid
           and can_mail = 1
           and AD_HOC_SUPPRESSIONS = '{}' -- This is the default value for the Ad hoc suppressions field. If it's this value there's no ad hoc supps! Woo!
           and offer is null
     ) z
where m.uid = z.uid
  and m.SALES_CAMPAIGN_ID = $campaignid
  and m.can_mail = 1;

USE SCHEMA OPERATIONAL.EMAIL_OPS;
use role EMAIL_OPS;
 UPDATE OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
set TREATMENT='1st Ascent Mail'
    select * from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p

where SALES_CAMPAIGN_ID=$campaignid
and CAMPAIGN_SPECIFIC_DATA:Ascent_Sends='1st'
 */
-- =================================================================
-- Put the model ID in campaign specific data
-- =================================================================
USE SCHEMA OPERATIONAL.EMAIL_OPS;
use role EMAIL_OPS;


UPDATE OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
set CAMPAIGN_SPECIFIC_DATA = OBJECT_INSERT(p.CAMPAIGN_SPECIFIC_DATA, 'Model_ID', z.ID, true)
from (
         select aj.uid, ss.ID
         FROM OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE aj
                  join models.output.ascent_click_prediction ss
                       on aj.uid = ss.UID
         where ss.TIMESTAMP_UTC = $max_time_stamp_utc --change date
           and aj.SALES_CAMPAIGN_ID = $campaignid
     ) z
where p.uid = z.uid
  and p.SALES_CAMPAIGN_ID = $campaignid;


-- =================================================================
-- Update the IID field for the campaign
-- =================================================================
-- now get the IIDs in there
USE SCHEMA OPERATIONAL.EMAIL_OPS;
use role EMAIL_OPS;


update OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE
set IID = 205496260 -- Ascent campaigns re-use the IID, so it's OK that this is hard-coded
where treatment is not null
  and can_mail = 1
  and SALES_CAMPAIGN_ID = $campaignid;


-- =================================================================
-- Check counts
-- =================================================================

-- by segment
select can_mail
    -- , CAMPAIGN_SPECIFIC_DATA:Ascent_Sends
     , TREATMENT
     , IID
     , count(uid)
from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE
where SALES_CAMPAIGN_ID = $campaignid
group by all
order by 1;


-- compare this send to previous sends as a gut check
with campaigns as
         (
             select top 5 sales_campaign_id, campaign_name
                  , campaign_start_date
                  , iff(sales_campaign_id = $campaignid, true, false) as is_this_campaign
             from processed.be_campaign.sales_campaign
             where campaign_name ilike '%ascent%'
             order by campaign_start_date desc
         )

select c.*, count(distinct uid)
from campaigns c
         join OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE o
              on c.sales_campaign_id = o.sales_campaign_id
                  and o.iid is not null
group by all
order by 1;


-- =================================================================
-- Remove the records from the last Ascent mailing campaign
-- =================================================================

set latestascentinsertdate = (
    Select cast(max(date_created) as date)
    from OPERATIONAL.EMAIL_OPS.Braze_Member_Marketing
    where CAMPAIGN_INTERACTION_ID = 205496260
      and ADD_OR_REMOVE = 1
);
set campaignid = $campaignid;
set add_or_delete = 0;
set maxBrazeID = (Select max(ID)
                  from OPERATIONAL.EMAIL_OPS.Braze_Member_Marketing);


insert into OPERATIONAL.EMAIL_OPS.Braze_Member_Marketing
select $maxBrazeID + 1
     , mad.ACCOUNT_ID
     , mad.CAMPAIGN_INTERACTION_ID
     , $add_or_delete
     , current_timestamp
     , NULL -- date_processed; updated by msg tech scripts when updating Braze
from OPERATIONAL.EMAIL_OPS.Braze_Member_Marketing mad
where CAMPAIGN_INTERACTION_ID = 205496260
  and date_created >= $latestascentinsertdate
  and ADD_OR_REMOVE = 1;



--===============================================
-- General QA
--===============================================
select p.uid
     , ss.value
from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE p
         left join models.output.ascent_click_prediction ss
                   on p.uid = ss.UID
                       and ss.TIMESTAMP_UTC = $max_time_stamp_utc
where SALES_CAMPAIGN_ID = $campaignid
  and CAN_MAIL = 1
  and p.treatment is null
group by all;


Select count(uid), treatment, offer
from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE
where sales_campaign_id = $campaignid
  and can_mail = 1
group by treatment, offer;

-- Why are there null treatments in here?
Select *
from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE
where sales_campaign_id = $campaignid
  and can_mail = 1
  and treatment is null
group by treatment;


update OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE
set treatment = null,
    can_mail  = 0
--Select count(uid)
--from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE
where treatment is null
  and can_mail = 1
  --and AD_HOC_SUPPRESSIONS <> '{}'
  and SALES_CAMPAIGN_ID = $campaignid;


-- ==============================================
-- QA for Stuart- diminishing size over time of this audience
-- ==============================================
create or replace temporary table Ascentcampaignids as
Select *
from PROCESSED.BE_CAMPAIGN.SALES_CAMPAIGN
where CAMPAIGN_NAME ilike '%Ascent%'
order by SALES_CAMPAIGN_ID desc;

Select count(aud.uid) as unscoredfolks, aud.SALES_CAMPAIGN_ID, asce.CAMPAIGN_NAME, asce.CAMPAIGN_START_DATE
from Ascentcampaignids asce
         join OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE aud
              on asce.SALES_CAMPAIGN_ID = aud.SALES_CAMPAIGN_ID
where aud.treatment ilike '%unscored%'
  and CAMPAIGN_START_DATE >= '2023-02-18'
group by aud.SALES_CAMPAIGN_ID, asce.CAMPAIGN_NAME, asce.CAMPAIGN_START_DATE
order by asce.CAMPAIGN_START_DATE;

Select count(aud.uid), aud.SALES_CAMPAIGN_ID, asce.CAMPAIGN_NAME, asce.CAMPAIGN_START_DATE
from Ascentcampaignids asce
         join OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE aud
              on asce.SALES_CAMPAIGN_ID = aud.SALES_CAMPAIGN_ID
where aud.can_mail = 1
  and aud.treatment is not null
  and CAMPAIGN_START_DATE >= '2023-02-18'
group by aud.SALES_CAMPAIGN_ID, asce.CAMPAIGN_NAME, asce.CAMPAIGN_START_DATE
order by asce.CAMPAIGN_START_DATE;


Select count(aud.uid) as unscoredfolks, aud.SALES_CAMPAIGN_ID, asce.CAMPAIGN_NAME, asce.CAMPAIGN_START_DATE
from Ascentcampaignids asce
         join OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE aud
              on asce.SALES_CAMPAIGN_ID = aud.SALES_CAMPAIGN_ID
where aud.treatment ilike '%unscored%'
  and CAMPAIGN_START_DATE >= '2023-02-18'
group by aud.SALES_CAMPAIGN_ID, asce.CAMPAIGN_NAME, asce.CAMPAIGN_START_DATE
order by asce.CAMPAIGN_START_DATE;

Select count(uid)
from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE
where SALES_CAMPAIGN_ID = $campaignid
  and can_mail = 1
  and treatment is not null

Select count(uid), treatment
from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE
where SALES_CAMPAIGN_ID = $campaignid
group by treatment

-- Review the Numbers and suppressions
SELECT z.mailable,
       z.Member_Status,
       COUNT(DISTINCT z.uid) AS UIDs
FROM (
         SELECT pool.uid,
                pool.Member_Status,
                CASE
                    WHEN
                        (pool.MF1_SUB = 0
                            and pool.EMAIL_VALID = 1
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
                            AND pool.AD_HOC_SUPPRESSIONS = '{}'
                            ) THEN
                        'Y'
                    ELSE
                        'N'
                    END AS mailable
         FROM OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE pool
         where pool.SALES_CAMPAIGN_ID = $campaignid
     ) z
GROUP BY z.mailable,
         z.Member_Status
ORDER BY z.mailable DESC,
         z.Member_Status,
         COUNT(DISTINCT z.uid) DESC;


-- and/or see where folks fall off
select case
           when (MF1_SUB = 1) then 'MF ONE sub'
           when (EMAIL_VALID = 0) then 'invalid email'
           when (IS_CANADIAN = 1) then 'canadian'
           when (FOOL_EMAIL = 1) then 'fool email'
           when (FE_FORMER = 1) then 'fe former'
           when (NEVERMAIL = 1) then 'nevermail'
           when (BOUNCERS = 1) then 'bouncer'
           when (ADMIN_BLOCKED = 1) then 'admin blocked'
           when (COMPLAINERS = 1) then 'Complainer'
           when (PRIVACY_OPT_OUT = 1) then 'Privacy OptOut'
           when (ifnull(LAST_MKTG_ENGAGE_TIMESTAMP, '2000-01-01') < dateadd(year, -1, current_date))
               then 'no activity in a year'
           WHEN (ON_SPECIAL_OFFERS = 1) THEN 'On Special Offers'
           when (SNOOZED_MARKETING = 1) then 'Snoozed Marketing 3months'
           when (AD_HOC_SUPPRESSIONS:"AOL_Inactives" = 1) then 'aol_inactive'
           when (AD_HOC_SUPPRESSIONS:"Freq_BoardPoster" = 1) then 'Frequent Board Poster'
           when (AD_HOC_SUPPRESSIONS:"MFWMValuedClients" = 1) then 'MFWM High Value Client'
           when (AD_HOC_SUPPRESSIONS:"EpicBundle_TestGroup" = 1) then 'Epic Bundle Test Group'
           when (AD_HOC_SUPPRESSIONS:"OwnsBackstage" = 1) then 'Owns Backstage Subscription'
           else AD_HOC_SUPPRESSIONS end as suppression_reason
     , count(distinct uid)              AS UIDs
from OPERATIONAL.EMAIL_OPS.MEMBER_MKTG_AUDIENCE
where SALES_CAMPAIGN_ID = $campaignid
  and can_mail = 0
group by case
             when (MF1_SUB = 1) then 'MF ONE sub'
             when (EMAIL_VALID = 0) then 'invalid email'
             when (IS_CANADIAN = 1) then 'canadian'
             when (FOOL_EMAIL = 1) then 'fool email'
             when (FE_FORMER = 1) then 'fe former'
             when (NEVERMAIL = 1) then 'nevermail'
             when (BOUNCERS = 1) then 'bouncer'
             when (ADMIN_BLOCKED = 1) then 'admin blocked'
             when (COMPLAINERS = 1) then 'Complainer'
             when (PRIVACY_OPT_OUT = 1) then 'Privacy OptOut'
             when (ifnull(LAST_MKTG_ENGAGE_TIMESTAMP, '2000-01-01') < dateadd(year, -1, current_date))
                 then 'no activity in a year'
             WHEN (ON_SPECIAL_OFFERS = 1) THEN 'On Special Offers'
             when (SNOOZED_MARKETING = 1) then 'Snoozed Marketing 3months'
             when (AD_HOC_SUPPRESSIONS:"AOL_Inactives" = 1) then 'aol_inactive'
             when (AD_HOC_SUPPRESSIONS:"Freq_BoardPoster" = 1) then 'Frequent Board Poster'
             when (AD_HOC_SUPPRESSIONS:"MFWMValuedClients" = 1) then 'MFWM High Value Client'
             when (AD_HOC_SUPPRESSIONS:"EpicBundle_TestGroup" = 1) then 'Epic Bundle Test Group'
             when (AD_HOC_SUPPRESSIONS:"OwnsBackstage" = 1) then 'Owns Backstage Subscription'
             else AD_HOC_SUPPRESSIONS end
order by 2 DESC;
