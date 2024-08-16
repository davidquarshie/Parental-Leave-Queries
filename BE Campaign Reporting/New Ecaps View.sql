create or replace view REPORTING.SALES.V_BE_ECAPS as
select sc.CAMPAIGN_YEAR_MONTH CAMPAIGN
     , sc.CAMPAIGN_NAME
     , i.ISSUE_ID
     , i.ISSUE_NAME
     , es.uid
     , datediff('day', LIST_BUILD_START_DATE, click.DATE) + 1  as days_since_lb_start
     , click.DATE
from PROCESSED.BE_CAMPAIGN.SALES_CAMPAIGN sc
         join PROCESSED.EMAIL.ISSUE i
              on LOWER(SPLIT_PART(ISSUE_NAME, '-', 4)) = lower(sc.CAMPAIGN_NAME_FOR_EMAIL)
         JOIN PROCESSED.EMAIL.EMAIL_ACTIVITY es
              ON es.issue_id = i.issue_id
         JOIN processed.shared.calendardate cd
              ON es.send_date_id = cd.date_id
                  AND cd.date BETWEEN list_build_start_date AND CAMPAIGN_END_DATE
         join PROCESSED.EMAIL.EMAIL_LINK_CLICK elc
              on elc.ISSUE_ID = i.ISSUE_ID
                  and elc.USER_ID = es.UID
                  and elc.CLICK_DATE_ID = es.CLICK_DATE_ID
         join PROCESSED.EMAIL.EMAIL_LINK el
              on el.LINK_ID = elc.LINK_ID
         join raw.BRAZE.V_USERS_MESSAGES_EMAIL_CLICK eumc
              on eumc.LINK_ID = el.QUERY_STRINGS:lid
                  and eumc.LINK_ALIAS ilike 'link%'
         JOIN processed.shared.calendardate click
              ON es.CLICK_DATE_ID = click.date_id
where
      --sc.CAMPAIGN_YEAR_MONTH = '2024.08.13 AI Software'
   i.MAILING_FAMILY IN ('TMF-premiuminfo.fool.com', 'Acquisition')
  AND i.MESSAGE_SUBJECT IS NOT NULL
  AND i.ISSUE_NAME LIKE 'BE-%'
  and i.ISSUE_NAME ilike '%lb%'
  and CLICKED = 1
    qualify ROW_NUMBER() OVER (PARTITION BY sc.CAMPAIGN_YEAR_MONTH, uid ORDER BY click.DATE) = 1;


select *
from REPORTING.SALES.V_BE_ECAPS
limit 10