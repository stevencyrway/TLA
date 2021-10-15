with recursive
    cteyandyavgcost as (Select prod_option_id,
                                last_day(to_date(to_timestamp(STAMPED)))                                              as MonthEnding,
                                avg(cost_price)                                                                       as MonthlyCostAvg,
                                Lead(last_day(to_date(to_timestamp(STAMPED))))
                                     OVER (ORDER BY FL.prod_option_id, last_day(to_date(to_timestamp(STAMPED))) desc) as PreviousMonthEnding
                         from FIVETRAN_DB.POSTGRES_PUBLIC.FIFO_LEDGER FL
                         group by last_day(to_date(to_timestamp(STAMPED))), to_timestamp(stamped), prod_option_id)
   , cteyandyinvcostLeadDayAdd as (Select *,
                                       case
                                           when PreviousMonthEnding >= MonthEnding then MonthEnding
                                           when PreviousMonthEnding < MonthEnding
                                               then dateadd(month, 1, PreviousMonthEnding) end as StartMonth
       from cteyandyavgcost)


select * from cteyandyavgcost
where PROD_OPTION_ID = 176
-- fifo_ledger.stamped (int) column is the epoch timestamp of the date/time of the inventory bucket.
-- to_timestamp(stamped) would get you the datetime translation.


--- to count monthly active rows.
Select 'lightspeed' as source, last_day(DONE), cast(avg(ROWS_UPDATED_OR_INSERTED) as bigint) from LIGHT_SPEED_RETAIL.FIVETRAN_AUDIT
group by last_day(DONE)
union all

Select 'yandy' as source, last_day(DONE), cast(avg(ROWS_UPDATED_OR_INSERTED) as bigint) from POSTGRES_PUBLIC.FIVETRAN_AUDIT
group by last_day(DONE)
union all

select 'shopify' as source, last_day(DONE), cast(avg(ROWS_UPDATED_OR_INSERTED) as bigint) from LOVERS_SHOPIFY.FIVETRAN_AUDIT
group by last_day(DONE)