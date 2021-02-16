---В ходе расчета метрик оперировала различными видами событий.

---1. Weekly Active Users
select count(distinct(user_id)) from event 
    where event_name = 'вход' and 
    event_timestamp > current_timestamp - interval '1 week' ;
	
---2. Average Revenue Per Paying User	
select sum(amount)/count(distinct(user_id)) from event where event_name = 'платеж'

---3. Daily New Users
--a. Если в таблице event есть пометка о том, что произошел первый вход(т.е. тип события 'регистрация')
select count(user_id), event_timestamp::date from event where event_name = 'регистрация' group by event_name, event_timestamp::date
--б. Если первый вход по типу события не отличается от последующих входов
with new_user as
(select distinct user_id, min(event_timestamp) over(partition by user_id) event_time from event 
																						where event_name='вход')
select count(user_id), event_time::date from new_user group by event_time::date

---4. 7th Day Retention Rate
with all_users as (
select count(distinct user_id) as all_count from event where event_name='вход' 
), new_user as(
select distinct user_id, 
       event_timestamp, row_number() over(partition by user_id order by event_timestamp asc) as rows_number 
       from event where event_name='вход'
), days_retention as(
select distinct user_id, 
       extract(days from max(event_timestamp) - min(event_timestamp)) as days  
       from new_user where rows_number in (1,2)  group by user_id
), seven_days_retention as (
select count(*) as seven_count from days_retention where days = 7
) select ((select seven_count::real from seven_days_retention) / (select all_count::real from all_users))*100 as seventh_day_retention_rate


---5. Daily Returning Users
with new_user as (
select distinct user_id, 
       event_timestamp, row_number() over(partition by user_id order by event_timestamp asc) as rows_number 
       from event where event_name='вход'
), count_daily as(
select event_timestamp, current_timestamp, extract(days from current_timestamp - event_timestamp+ interval '1 days') as days, user_id from new_user where rows_number = 1
), count_actually as (
    select distinct user_id, count(event_timestamp::date) over (partition by user_id)  from event where event_name='вход' group by event_name, user_id, event_timestamp::date, amount
) select count(*) from count_daily as c_d inner join count_actually as c_a on c_d.user_id=c_a.user_id where c_a.days = c_d.count
