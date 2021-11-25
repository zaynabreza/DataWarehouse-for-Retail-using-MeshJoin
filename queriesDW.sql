-- Query 1
select s.supplier_name, p.product_name, d.quarter, d.month, sum(total_sale)
from transaction_fact as t, date as d, product as p, supplier as s
where t.product_id=p.product_id and t.supplier_id=s.supplier_id and t.date_id=d.date
group by supplier_name, p.product_name, d.quarter, d.month with rollup;

-- Query 2
select s.store_name, p.product_name, sum(total_sale)
from transaction_fact as t, product as p, store as s
where t.product_id=p.product_id and t.store_id=s.store_id
group by store_name, product_name with rollup;

-- Query 3
select a.id, a.product_name, a.Total
from
	(select sum(quantity) as Total, product_name, p.product_id as id
	from transaction_fact t, product p, date d
	where t.product_id=p.product_id and t.date_id=d.date and (d.day='Saturday' or d.day='Sunday')
	group by p.product_id
	order by sum(quantity) desc) as a
LIMIT 5;

-- Query 4
select a.product_name, a.Total as Quarter1, b.Total as Quarter2, c.Total as Quarter3, d.Total as Quarter4 from
(
select p.product_id as pr, sum(total_sale) as Total, product_name
from transaction_fact t, product p, date d
where t.product_id=p.product_id and t.date_id=d.date and d.year=2016 and d.quarter=1
group by pr
order by sum(quantity) desc) as a,
(
select p.product_id as pr, sum(total_sale) as Total, product_name
from transaction_fact t, product p, date d
where t.product_id=p.product_id and t.date_id=d.date and d.year=2016 and d.quarter=2
group by pr
order by sum(quantity) desc) as b,
(
select p.product_id as pr, sum(total_sale) as Total, product_name
from transaction_fact t, product p, date d
where t.product_id=p.product_id and t.date_id=d.date and d.year=2016 and d.quarter=3
group by pr
order by sum(quantity) desc) as c,
(
select p.product_id as pr, sum(total_sale) as Total, product_name
from transaction_fact t, product p, date d
where t.product_id=p.product_id and t.date_id=d.date and d.year=2016 and d.quarter=4
group by pr
order by sum(quantity) desc) as d
where a.pr=b.pr and b.pr=c.pr and c.pr=d.pr;

-- Query 5
select a.pr, a.product_name, a.Total as Half1, b.Total as Half2, c.Total as Total2016 from
(
select p.product_id as pr, sum(total_sale) as Total, product_name
from transaction_fact t, product p, date d
where t.product_id=p.product_id and t.date_id=d.date and d.year=2016 and d.month <=6
group by pr
order by sum(total_sale) desc) as a,
(
select p.product_id as pr, sum(total_sale) as Total, product_name
from transaction_fact t, product p, date d
where t.product_id=p.product_id and t.date_id=d.date and d.year=2016 and d.month > 6
group by pr
order by sum(quantity) desc) as b,
(
select p.product_id as pr, sum(total_sale) as Total, product_name
from transaction_fact t, product p, date d
where t.product_id=p.product_id and t.date_id=d.date and d.year=2016
group by pr
order by sum(quantity) desc) as c
where  a.pr=b.pr and a.pr=c.pr;

-- Query 6
select transaction_id, date_id, store_id
from transaction_fact
order by transaction_id;
/* The anomaly is that the Transaction IDs are inconsistent with the dates of when the transactions took place.
Since they do not hold any other meaning within them and thus are acting like a surrogate key, it stands to reason
that every new ID should be generated when a new transaction occurs, in which case a higher ID should mean a later
date, but this is not the case. Even in the same store higher transaction IDs are occuring on dates that occur previously
than some transactions in that same store. This can be seen in the results of the query given above.*/

-- Query 7
DROP TABLE IF EXISTS `STOREANALYSIS_MV`;
create table STOREANALYSIS_MV as
select s.store_id as STORE_ID, p.product_id as PROD_ID, sum(total_sale) as STORE_TOTAL
from transaction_fact as t, product as p, store as s
where t.product_id=p.product_id and t.store_id=s.store_id
group by s.store_id, p.product_id
order by s.store_id, p.product_id;

select * from STOREANALYSIS_MV;


