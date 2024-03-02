--Вывести распределение (количество) клиентов по сферам деятельности, отсортировав результат по убыванию количества. — (1 балл)

select job_industry_category, count(*) as count
from customer_2
group by job_industry_category
order by count desc

--Найти сумму транзакций за каждый месяц по сферам деятельности, отсортировав по месяцам и по сфере деятельности. — (1 балл)

select job_industry_category, date_trunc('month', transaction_date::date)::date as months, sum(standard_cost) as month_sum
from customer_2
join transactions
on customer_2.customer_id = transactions.customer_id 

group by job_industry_category, months
order by job_industry_category, months

--Вывести количество онлайн-заказов для всех брендов в рамках подтвержденных заказов клиентов из сферы IT. — (1 балл)

select count(*) as order_count, brand, order_status, online_order
from customer_2
join transactions
on customer_2.customer_id = transactions.customer_id 
where job_industry_category = 'IT' and order_status = 'Approved' and online_order = 'True'
group by brand, order_status, online_order
order by order_count desc, brand

--Найти по всем клиентам сумму всех транзакций (list_price), максимум, минимум и количество транзакций, отсортировав результат по убыванию суммы транзакций и количеству транзакций.
-- Выполните двумя способами: используя только group by и используя только оконные функции. Сравните результат. — (2 балла)

--with group by

select customer_2.customer_id, count(transaction_id) as trans_count, min(list_price) as trans_min, max(list_price) as trans_max, sum(list_price) as trans_sum
from customer_2
join transactions
on customer_2.customer_id = transactions.customer_id 
group by customer_2.customer_id 
order by trans_sum desc, trans_count desc

--with window func

select customer_2.customer_id,
count(transaction_id) over (partition by customer_2.customer_id) as trans_count,
min(list_price) over (partition by customer_2.customer_id) as trans_min,
max(list_price) over (partition by customer_2.customer_id) as trans_max,
sum(list_price) over (partition by customer_2.customer_id) as trans_sum

from customer_2
join transactions
on customer_2.customer_id = transactions.customer_id 
order by trans_sum desc, trans_count desc

--Найти имена и фамилии клиентов с минимальной/максимальной суммой транзакций за весь период (сумма транзакций не может быть null). 
--Напишите отдельные запросы для минимальной и максимальной суммы. — (2 балла)

-- customer name with max value
with trans_sum_rank_customer
as
	(
	with trans_sum_customer
	as
		(select first_name, last_name, transactions.customer_id,
		
		sum(case when list_price is null then 0 else list_price end) over (partition by transactions.customer_id) as trans_sum
		
		from transactions
		left join customer_2
		on transactions.customer_id = customer_2.customer_id 
		order by trans_sum desc
		)
	
	select first_name, last_name, customer_id, trans_sum,
	rank() over(order by trans_sum desc) as rank_sum
	from trans_sum_customer
	)
	
select distinct first_name, last_name, customer_id, trans_sum, rank_sum
from trans_sum_rank_customer
where rank_sum = 1

-- window func
-- customer name with min value
with trans_sum_rank_customer
as
	(
	with trans_sum_customer
	as
		(select first_name, last_name, transactions.customer_id,
		
		sum(case when list_price is null then 0 else list_price end) over (partition by transactions.customer_id) as trans_sum
		
		from transactions
		left join customer_2
		on transactions.customer_id = customer_2.customer_id 
		order by trans_sum
		)
	
	select first_name, last_name, customer_id, trans_sum,
	rank() over(order by trans_sum) as rank_sum
	from trans_sum_customer
	)
	
select distinct first_name, last_name, customer_id, trans_sum, rank_sum
from trans_sum_rank_customer
where rank_sum = 1

--Вывести только самые первые транзакции клиентов. Решить с помощью оконных функций. — (1 балл)

with tansaction_date_customer_id
as 
	(select first_name, last_name, customer_2.customer_id, transaction_date::date as trans_date, transaction_id
	from customer_2
	join transactions
	on customer_2.customer_id = transactions.customer_id
	order by customer_2.customer_id, trans_date)
	
select first_name, last_name, customer_id, trans_date, transaction_id,
first_value(transaction_id) over (partition by customer_id) as first_trunsaction
from tansaction_date_customer_id
	

--Вывести имена, фамилии и профессии клиентов, между соседними транзакциями которых был максимальный интервал (интервал вычисляется в днях) — (2 балла).
with final_rate
as
	(
		with difference_tansaction_date_customer_id
		as
			(
				with tansaction_date_customer_id
				as 
					(select first_name, last_name, customer_2.customer_id, job_title, transaction_date::date as trans_date, transaction_id
					from customer_2
					join transactions
					on customer_2.customer_id = transactions.customer_id
					order by customer_2.customer_id, trans_date)
					
				select first_name, last_name, customer_id, trans_date, transaction_id,
				
				lag(trans_date) over (partition by customer_id) as prev_transacion_date
	
				from tansaction_date_customer_id
			)
			
		select first_name, last_name, customer_id, trans_date, prev_transacion_date,
		coalesce(trans_date - prev_transacion_date, 0) as diff_day,
		
		rank() over (partition by customer_id order by coalesce(trans_date - prev_transacion_date, 0) desc) as rank_diff_day_desc,
		rank() over (order by coalesce(trans_date - prev_transacion_date, 0) desc) as all_rank_diff_day_desc
		
		from difference_tansaction_date_customer_id
		--group by customer_id, first_trunsacion_date, last_trunsaction_date, diff_day
		--order by diff_day desc
	)
	
select first_name, last_name, customer_id, diff_day, rank_diff_day_desc, all_rank_diff_day_desc
from final_rate
where rank_diff_day_desc = 1 and all_rank_diff_day_desc = 1
group by first_name, last_name, customer_id, diff_day, rank_diff_day_desc, all_rank_diff_day_desc
order by diff_day desc, rank_diff_day_desc, all_rank_diff_day_desc


		










