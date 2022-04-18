
/***************************************************************************************************************************
        pulling down total dmm data to append to targeted list data later on
***************************************************************************************************************************/

drop table if exists tmp_tl_dmm
create table tmp_tl_dmm as

select
            dmm.sfdc_account_id                             accountid
        ,   dmm.taker_division_uuid
        ,   row_number() over(partition by dmm.sfdc_account_id order by cast(dmm.first_participation_date as date)) first_play_count
        ,   row_number() over(partition by dmm.sfdc_account_id order by cast(dmm.participation_Dt as date))         play_count
        ,   cast(dmm.participation_Dt as date)              participation_Dt
        ,   case when dmm.participation_dt::date = cast(dmm.first_participation_date as date) then dmm.first_participation_date::date else null end      first_participation_date
        ,   dmm.usd_played_ap                               played_ap
        ,   dmm.usd_awarded_income                          gmr
        ,   case when dmm.participation_dt::date = cast(dmm.first_participation_date as date) then 1 else 0 end                                          new_p_count
        ,   case when dmm.participation_dt::date = cast(dmm.first_participation_date as date) then dmm.usd_awarded_income else 0 end                     new_p_gmr
from analytics_common.daily_market_metrics dmm (nolock)
where 1=1
    and dmm.usd_awarded_ap > 0

/***************************************************************************************************************************
        creating a flag where there is a soft touch in EL but not in the tasks table on a given day
            -doing this to see how much the Task data is cooking the books (one way or another)
***************************************************************************************************************************/

drop table if exists tmp_tasks
create table tmp_tasks as

SELECT a.accountid                                            accountid
                  , convert_timezone('UTC', 'CST', a.createddate)::date as event_date
                  , 'soft_touch'                                           touch_type
             from cir_salesforce.Task A (nolock)
                      inner join cir_salesforce.[user] c
             on a.lastmodifiedbyid = c.id
             WHERE STATUS in ('COMPLETED', 'Completed')
               and a.lastmodifiedbyid not in ( '0051H000009RYqEQAW', '0051H000007TllVQAS', '00512000007Ntm0AAC')
               and a.RecordTypeId in ('012120000019E7hAAE', '0121H000001IewQQAS', '0121H000001Ifh4QAC', '0121H000001Ifh5QAC', '012A0000000oDeZIAU')
               and a.accountid is not null
               and event_date >= '2022-01-01'

/***************************************************************************************************************************
--------throwing touch data that doesn't have a task associated with it into a table so I can flag those later on (think this might be inflating numbers)
***************************************************************************************************************************/

drop table if exists tmp_st_no_task
create table tmp_st_no_task as

select
        el.account_id       accountid
    ,   el.event_timestamp::date event_date
    ,   case when el.account_id not in (select accountid from tmp_tasks tt where tt.event_date = el.event_timestamp::date) then 1 else 0 end    no_task_flag
from cir_engagement.engagements_linked el (nolock)
where 1=1
    and el.engagement_system not in ('marketo','sendgrid','intercom')
    and el.hard_touch is null

/***************************************************************************************************************************
        touch data attribution (using Reid Hanson's logic from the TL Touch reporting)
***************************************************************************************************************************/

drop table if exists cte_touches
create table cte_touches as

select x.*
from (
         select a.id                                                                        accountid
              , el.event_date
              , lead(el.event_date) over (partition by a.id order by el.event_date)         lead_event_date
              , row_number() over (partition by a.id order by el.event_date)                RN
              , row_number() over (partition by a.id, el.event_date order by el.event_date) RN_event
         from cir_salesforce.account a (nolock)
                  join cir_Salesforce.user u (nolock) on u.id = a.ownerid
             and u.srm_team__c = 'US-KS-HQ'
                  left join (
             select account_id               accountid
                  --,   max(el.event_timestamp::date)   event_date
                  , el.event_timestamp::date event_date
                  , 'hard_touch'             touch_type
             from cir_engagement.engagements_linked el (nolock)
             where 1 = 1
               --and el.event_timestamp::date >= '2022-01-01'
               and el.engagement_system not in ('marketo', 'sendgrid', 'intercom')
               and el.hard_touch = 'True'
               --group by el.account_id

             UNION

             SELECT a.accountid                                            accountid
                  --, max(convert_timezone('UTC', 'CST', a.createddate)::date) as event_date
                  , convert_timezone('UTC', 'CST', a.createddate)::date as event_date
                  , 'soft_touch'                                           touch_type
             from cir_salesforce.Task A (nolock)
                      inner join cir_salesforce.[user] c
             on a.lastmodifiedbyid = c.id
             WHERE STATUS in ('COMPLETED'
                 , 'Completed')
               and a.lastmodifiedbyid not in ( '0051H000009RYqEQAW'
                 , '0051H000007TllVQAS'
                 , '00512000007Ntm0AAC')
               and a.RecordTypeId in ('012120000019E7hAAE'
                 , '0121H000001IewQQAS'
                 , '0121H000001Ifh4QAC'
                 , '0121H000001Ifh5QAC'
                 , '012A0000000oDeZIAU')
               and a.accountid is not null
         ) el on el.accountid = a.id
     ) x
where x.RN_event = 1

/***************************************************************************************************************************
        appending dmm data to touched data and attributing plays back to specific touches on accounts
            -plays are attributed within the last touch and next touch so long as the next play is <= 30 days from the last play (we can tweak this, 30 days is arbitrary)
***************************************************************************************************************************/

drop table if exists tmp_play_attribution
create table tmp_play_attribution as

select  a.accountid
    ,   a.event_date
    ,   a.lead_event_date
    ,   a.rn
    ,   case when  (row_number() over(partition by a.accountid,a.participation_Dt order by a.participation_dt)) =1 then a.participation_dt  else null end                     participation_dt
    ,   case when  (row_number() over(partition by a.accountid,a.participation_Dt order by a.participation_dt)) =1 then a.new_p_dt          else null end                     new_p_dt
    ,   case when  (row_number() over(partition by a.accountid,a.participation_Dt order by a.participation_dt)) =1 then a.gmr               else 0    end                     gmr
    ,   case when  (row_number() over(partition by a.accountid,a.participation_Dt order by a.participation_dt)) =1 then a.new_p_gmr         else 0    end                     new_p_gmr
    ,   row_number() over(partition by a.accountid,a.participation_Dt order by a.participation_dt)                                                                            rn_dmm
    ,   case when  (row_number() over(partition by a.accountid,a.participation_Dt order by a.participation_dt)) =1 and a.participation_dt is not null then 1 else 0 end       play_count
    ,   case when  (row_number() over(partition by a.accountid,a.participation_Dt order by a.participation_dt)) =1 and a.new_p_dt         is not null then 1 else 0 end       new_p_count
    ,   a.tl_flag
    ,   case when a.accountid not in (select s.sf_id from srm_home_reporting_production.accounts_es_index s where s.targeted_list_name is not null) then 1 else 0 end         never_on_tl_flag
from (
         select c.accountid
              , c.event_date
              , c.lead_event_date
              , c.rn
              , min(dmm.participation_Dt)     participation_Dt
              , min(dmm.new_p_dt)             new_p_dt
              , isnull(sum(dmm.gmr), 0)       gmr
              , isnull(sum(dmm.new_p_gmr), 0) new_p_gmr
              , case
                    when c.accountid in
                         (
                             select s.sf_id accountid
                             from srm_home_reporting_production.accounts_es_index s
                             where s.targeted_list_date::date = c.event_date
                                and s.targeted_list_name is not null
                         ) then 1
                    else 0 end                tl_flag --checking to see if the TL was touched on that day
         from cte_touches c
                  left join
                      (
                          select dmm.accountid
                               , dmm.participation_Dt::date              participation_dt
                               , dmm.first_participation_date::date      new_p_dt
                               , count(distinct dmm.taker_division_uuid) plays
                               , sum(dmm.gmr)                            gmr
                               , sum(dmm.new_p_gmr)                      new_p_gmr
                          from tmp_tl_dmm dmm
                               --where dmm.accountid = '0011200001DyJriAAF'
                          group by dmm.accountid
                                 , dmm.participation_Dt::date
                                 , dmm.first_participation_date::date
                      ) dmm on c.accountid = dmm.accountid
                          and dmm.participation_Dt::date between c.event_date and c.lead_event_date
                          and dmm.participation_Dt::date between c.event_date and dateadd(day, 30, c.event_date)

         group by c.accountid
                , c.event_date
                , c.lead_event_date
                , c.rn
     ) a

/***************************************************************************************************************************
        final output
            -appending user data and various flags that I tee up before this part
            -I slice and dice all of this in excel
            -I limit data to 2022 YTD with the knowledge that there is touch and play data that occurred before
                I can always bring this in / filter off of accounts that had activity prior to 2022 if I need to later, but wanted to limit data set to the same time frame that we have targeted list data
***************************************************************************************************************************/

select

            pa.*
        ,   row_number() over(partition by pa.accountid order by pa.event_date)     rn_2022
        ,   mas.account_segment
        ,   u.name                      srm_name
        ,   u.role_type__c
        ,   u.functioning_role__c
        ,   u.management_team__c
        ,   case when u.createddate::date < '2021-06-01'                                then 'Tenured'
                 when datediff(month,u.createddate::date,getdate()::date) <= 6          then 'New'
                 when u.createddate::date >= '2022-01-01'                               then '2022_Hiring_Class'
                 when u.createddate::date > '2021-06-01'                                then '2021_Hiring_Class'
                 else null end tenure
        ,   case when pa.accountid in
                        (select accountid from tmp_play_attribution where event_date < '2022-01-01') then 1 else 0 end touched_before2022
        ,   case when pa.accountid in
                        (select accountid from tmp_play_attribution where participation_dt < '2022-01-01') then 1 else 0 end played_before2022
        ,   case when pa.accountid in
                        (
                            select account_id               accountid
                             from cir_engagement.engagements_linked el (nolock)
                             where 1 = 1
                               and el.event_timestamp::date = pa.event_date::date
                               and el.engagement_system not in ('marketo', 'sendgrid', 'intercom')
                               and el.hard_touch = 'True'
                        )   then 1 else 0 end                                                                               ht_flag
        ,   case when pa.accountid in (select accountid from tmp_st_no_task nt where nt.no_task_flag = 1) then 1 else 0 end st_no_task_flag
from tmp_play_attribution  pa
    join cir_salesforce.account a (nolock)                                      on a.id = pa.accountid
    join cir_Salesforce.user u (nolock)                                         on u.id = a.ownerid
    left join cir_core.marketing_account_segmentation mas                       on mas.account_id = a.id
    left join srm_home_reporting_production.accounts_es_index s (nolock)        on s.sf_id = a.id
        and s.targeted_list_date::date = pa.event_date
        and s.targeted_list_name is not null
where 1=1
    and pa.event_date >= '2022-01-01'
    --and u.role_type__c = 'Hunter'

