with sends as (
    select cd.date send_date
         , i.ISSUE_ID
         , i.ISSUE_NAME
,i.MESSAGE_SUBJECT
         , es.uid
         , es.OPENED
         , es.CLICKED
         --, COUNT(DISTINCT es.uid)                                   AS sends
         --, COUNT(DISTINCT CASE WHEN es.opened = 1 THEN es.uid END)  AS opens
         --, COUNT(DISTINCT CASE WHEN es.clicked = 1 THEN es.uid END) AS clicks

    from PROCESSED.BE_CAMPAIGN.SALES_CAMPAIGN sc
             join PROCESSED.EMAIL.ISSUE AS i
                  on LOWER(SPLIT_PART(ISSUE_NAME, '-', 4)) = lower(sc.CAMPAIGN_NAME_FOR_EMAIL)
             JOIN PROCESSED.EMAIL.EMAIL_ACTIVITY es
                  ON es.issue_id = i.issue_id
             JOIN processed.shared.calendardate AS cd
                  ON es.send_date_id = cd.date_id
                      AND cd.date BETWEEN list_build_start_date AND CAMPAIGN_END_DATE


    where sc.CAMPAIGN_YEAR_MONTH = <Parameters.campaign_year_month>
      and i.MAILING_FAMILY IN ('TMF-premiuminfo.fool.com', 'Acquisition')
      AND i.MESSAGE_SUBJECT IS NOT NULL
      AND i.ISSUE_NAME LIKE '%BE-%')

   , sources as
    (select distinct elc.ISSUE_ID
                   , el.SOURCECODE
     from PROCESSED.EMAIL.EMAIL_LINK_CLICK elc
              JOIN PROCESSED.EMAIL.EMAIL_LINK AS el
                   ON el.link_id = elc.link_id
                       and el.SOURCECODE IS NOT NULL
                       and UTM_CAMPAIGN = <Parameters.utm_campaign>)

   , sends_w_source as
    (select s.*
          , src.SOURCECODE
     from sends s
              left join sources src
                   on s.ISSUE_ID = src.ISSUE_ID)

select ss.*
     , sum(bos.ORDERS) orders
     , sum(cash)       cash
from sends_w_source ss
         left join PROCESSED.BE_CAMPAIGN.V_BE_ORDERS_SIMPLE bos
                   on bos.UID = ss.uid
                       and bos.SOURCE_CODE = ss.SOURCECODE
                       and bos.CAMPAIGN_YEAR_MONTH = <Parameters.campaign_year_month>
group by all
